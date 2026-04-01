#!/usr/bin/env python3
"""
Enrich processed_podia_books documents with two new fields:

  matn_embedding : list[float]  — 1024-dim Cohere embed-v4 vector (float32)
  topics         : list[str]    — 3-6 Arabic semantic topic tags

EMBEDDING ASYMMETRY NOTE
------------------------
This script uses input_type="search_document" for matn text (offline indexing).
At query time (search endpoint / chatbot), embed the user query with:

    from langchain_cohere import CohereEmbeddings
    embedder = CohereEmbeddings(
        model="embed-v4.0",
        cohere_api_key=COHERE_API_KEY,
    )
    # embed_query() uses input_type="search_query" internally — correct for queries.
    # embed_documents() uses input_type="search_document" — correct for indexing.
    query_vector = embedder.embed_query(user_question)

The 1024-dim output_dimension must match what is stored in matn_embedding.

TOPIC TAXONOMY
--------------
Topics are cross-cutting semantic themes independent of the Bukhari book structure.
The LLM selects 3-6 tags from a predefined list (or adds new ones if clearly needed):

    العقيدة والتوحيد، الإيمان، الطهارة، الصلاة، الزكاة، الصوم، الحج، الجهاد،
    البيوع والمعاملات، النكاح والطلاق، الأخلاق والآداب، العلم والتعليم،
    الدعاء والذكر، القرآن، الأنبياء والرسل، الصحابة، الفتن والأشراط،
    الجنة والنار، الموت والجنائز، الطب والرقية، الأطعمة والأشربة،
    القضاء والأحكام، الحدود والعقوبات، الصدق والأمانة، البر والرحمة

USAGE
-----
    # Count pending documents without touching anything
    python scripts/embed_matn.py --dry-run

    # Smoke test on 5 documents
    python scripts/embed_matn.py --limit 5

    # Full run (7,076 documents)
    python scripts/embed_matn.py

    # Topic tagging only (skip Cohere embedding)
    python scripts/embed_matn.py --skip-embed

    # Embedding only (skip LLM topic tagging)
    python scripts/embed_matn.py --skip-topics

ENV VARS REQUIRED (in .env)
----------------------------
    MONGODB_URI_READ_WRITE  — Atlas URI with read+write privileges
    COHERE_API_KEY          — Cohere API key
    OPENROUTER_API_KEY      — OpenRouter API key (for Gemini Flash)
    DB_NAME                 — optional, default: HadithData

ATLAS VECTOR SEARCH INDEX
--------------------------
After this script completes, create the vector search index manually in Atlas UI:

    Database: HadithData
    Collection: processed_podia_books
    Index name: vector_index_matn
    JSON definition:
    {
      "fields": [{
        "type": "vector",
        "path": "matn_embedding",
        "numDimensions": 1536,
        "similarity": "cosine"
      }]
    }
"""

import argparse
import os
import random
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_cohere import CohereEmbeddings
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COLLECTION_NAME = "processed_podia_books"
DB_NAME_DEFAULT = "HadithData"

COHERE_MODEL    = "embed-v4.0"
EMBEDDING_DIM   = 1536  # embed-v4.0 returns 1536-dim (langchain-cohere does not expose output_dimension)
EMBED_BATCH_MAX = 96   # Cohere API hard limit per request

LLM_MODEL       = "google/gemini-3-flash-preview"
LLM_BATCH_MAX   = 20  # hadiths per Gemini call

MAX_RETRIES_EMBED = 5
MAX_RETRIES_LLM   = 3
RETRY_BASE_DELAY  = 2.0  # seconds; doubles each attempt

TOPIC_LIST = [
    "العقيدة والتوحيد",
    "الإيمان",
    "الطهارة",
    "الصلاة",
    "الزكاة",
    "الصوم",
    "الحج",
    "الجهاد",
    "البيوع والمعاملات",
    "النكاح والطلاق",
    "الأخلاق والآداب",
    "العلم والتعليم",
    "الدعاء والذكر",
    "القرآن",
    "الأنبياء والرسل",
    "الصحابة",
    "الفتن والأشراط",
    "الجنة والنار",
    "الموت والجنائز",
    "الطب والرقية",
    "الأطعمة والأشربة",
    "القضاء والأحكام",
    "الحدود والعقوبات",
    "الصدق والأمانة",
    "البر والرحمة",
]


_REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Pydantic output models (LangChain structured output)
# ---------------------------------------------------------------------------


class HadithTopics(BaseModel):
    topics: list[str] = Field(
        ...,
        min_length=1,
        max_length=3,
        description="قائمة من 1 إلى 3 موضوعات عربية مختصرة من القائمة المرجعية",
    )


class BatchTopics(BaseModel):
    results: list[HadithTopics] = Field(
        ...,
        description="موضوعات لكل حديث في الدفعة بنفس الترتيب",
    )


# ---------------------------------------------------------------------------
# Environment loading
# ---------------------------------------------------------------------------


def load_env() -> tuple[str, str, str, str]:
    """Load .env from repo root. Return (mongo_uri, db_name, cohere_key, openrouter_key)."""
    load_dotenv(_REPO_ROOT / ".env")

    mongo_uri = (
        os.environ.get("MONGODB_URI_READ_WRITE")
        or os.environ.get("MONGODB_URI")
    )
    if not mongo_uri:
        sys.exit(
            "ERROR: MONGODB_URI_READ_WRITE not set in .env\n"
            "  MONGODB_URI_READ is read-only and cannot write embeddings.\n"
            "  Add MONGODB_URI_READ_WRITE=<atlas-uri> to .env"
        )

    cohere_key = os.environ.get("COHERE_API_KEY", "")
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")
    db_name = os.environ.get("DB_NAME", DB_NAME_DEFAULT)

    return mongo_uri, db_name, cohere_key, openrouter_key


# ---------------------------------------------------------------------------
# MongoDB connection
# ---------------------------------------------------------------------------


def get_collection(mongo_uri: str, db_name: str):
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    print(f"Connected to MongoDB. DB={db_name}, Collection={COLLECTION_NAME}")
    return client[db_name][COLLECTION_NAME]


# ---------------------------------------------------------------------------
# LLM topic chain (LangChain)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "أنت نظام تصنيف متخصص في أحاديث صحيح البخاري. "
    "مهمتك تصنيف كل حديث بموضوعات إسلامية دقيقة.\n\n"
    "القائمة المرجعية للموضوعات:\n"
    f"{TOPIC_LIST}\n\n"
    "يمكنك إضافة موضوع جديد إن لم يغطِ أي موضوع من القائمة المعنى بوضوح، "
    "لكن التزم بالقائمة قدر الإمكان."
)

_HUMAN_TEMPLATE = (
    "صنّف الأحاديث التالية. لكل حديث اختر 1-3 موضوعات عربية مختصرة.\n"
    "أعِد النتائج بنفس الترتيب المُدخَل تمامًا.\n\n"
    "{hadiths_block}"
)


def build_llm_chain(openrouter_key: str):
    llm = ChatOpenAI(
        model=LLM_MODEL,
        base_url="https://openrouter.ai/api/v1",
        api_key=openrouter_key,
        temperature=0.0,
    )
    prompt = ChatPromptTemplate.from_messages([
        ("system", _SYSTEM_PROMPT),
        ("human", _HUMAN_TEMPLATE),
    ])
    return prompt | llm.with_structured_output(BatchTopics)


def tag_topics_batch(
    chain,
    texts: list[str],
    retries: int = MAX_RETRIES_LLM,
) -> list[list[str]] | None:
    """
    Call the LLM chain for a batch of matn texts.
    Returns a list of topic lists (one per text), or None on failure.
    """
    hadiths_block = "\n\n".join(
        f"[{i + 1}] {text}" for i, text in enumerate(texts)
    )
    for attempt in range(retries):
        try:
            result: BatchTopics = chain.invoke({"hadiths_block": hadiths_block})
            # Pad or trim to match input length in case model returns wrong count
            topics_list = [h.topics for h in result.results]
            if len(topics_list) < len(texts):
                topics_list += [["غير محدد"]] * (len(texts) - len(topics_list))
            return topics_list[: len(texts)]
        except Exception as exc:
            if attempt == retries - 1:
                tqdm.write(f"  [LLM ERROR] giving up after {retries} attempts: {exc}")
                return None
            delay = RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            tqdm.write(f"  [LLM] retry {attempt + 1}/{retries} in {delay:.1f}s — {exc}")
            time.sleep(delay)
    return None


# ---------------------------------------------------------------------------
# Cohere embedding (LangChain)
# ---------------------------------------------------------------------------


def build_embedder(cohere_key: str) -> CohereEmbeddings:
    # input_type and output_dimension are NOT constructor params in langchain-cohere 0.5+.
    # embed_documents() hardcodes input_type="search_document" internally — correct for us.
    # Dimension is model-determined: embed-v4 returns 1024-dim by default.
    return CohereEmbeddings(
        model=COHERE_MODEL,
        cohere_api_key=cohere_key,
    )


def embed_batch(
    embedder: CohereEmbeddings,
    texts: list[str],
    retries: int = MAX_RETRIES_EMBED,
) -> list[list[float]] | None:
    """
    Embed a batch of texts. Returns list of vectors or None on failure.
    Retries on rate-limit / transient errors with exponential backoff.
    """
    for attempt in range(retries):
        try:
            return embedder.embed_documents(texts)
        except Exception as exc:
            msg = str(exc).lower()
            is_rate_limit = "429" in msg or "rate limit" in msg or "too many" in msg
            if attempt == retries - 1 or not is_rate_limit:
                tqdm.write(f"  [EMBED ERROR] giving up: {exc}")
                return None
            delay = RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            tqdm.write(f"  [EMBED] rate limit, retry {attempt + 1}/{retries} in {delay:.1f}s")
            time.sleep(delay)
    return None


# ---------------------------------------------------------------------------
# Atlas Vector Search index instructions
# ---------------------------------------------------------------------------


def print_index_instructions():
    print("""
══════════════════════════════════════════════════════════════
  Atlas Vector Search Index — create manually in Atlas UI
══════════════════════════════════════════════════════════════
  1. Open your cluster → Search → Create Search Index
  2. Select "Atlas Vector Search" (NOT full-text Atlas Search)
  3. DB: HadithData  |  Collection: processed_podia_books
  4. Select "JSON Editor" and paste:

  {
    "fields": [{
      "type": "vector",
      "path": "matn_embedding",
      "numDimensions": 1536,
      "similarity": "cosine"
    }]
  }

  5. Name the index: vector_index_matn
  6. Click "Create Search Index" (builds in ~2-5 min)
══════════════════════════════════════════════════════════════
""")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args():
    p = argparse.ArgumentParser(
        description="Embed matn_text_plain → matn_embedding and tag topics.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Count pending docs and estimate API calls, then exit.")
    p.add_argument("--batch-size", type=int, default=EMBED_BATCH_MAX, metavar="N",
                   help=f"Cohere embed batch size (max {EMBED_BATCH_MAX}).")
    p.add_argument("--llm-batch", type=int, default=LLM_BATCH_MAX, metavar="N",
                   help=f"Hadiths per Gemini call (max {LLM_BATCH_MAX}).")
    p.add_argument("--limit", type=int, default=0,
                   help="Process at most N documents (0 = all). Useful for smoke tests.")
    p.add_argument("--skip-topics", action="store_true",
                   help="Skip LLM topic tagging; only generate embeddings.")
    p.add_argument("--skip-embed", action="store_true",
                   help="Skip Cohere embedding; only generate topic tags.")
    return p.parse_args()


def main():
    args = parse_args()

    embed_batch_size = min(args.batch_size, EMBED_BATCH_MAX)
    llm_batch_size   = min(args.llm_batch, LLM_BATCH_MAX)

    mongo_uri, db_name, cohere_key, openrouter_key = load_env()

    # Validate API keys based on what we'll actually use
    if not args.skip_embed and not cohere_key:
        sys.exit("ERROR: COHERE_API_KEY not set in .env")
    if not args.skip_topics and not openrouter_key:
        sys.exit("ERROR: OPENROUTER_API_KEY not set in .env")

    col = get_collection(mongo_uri, db_name)

    # Resume filter: process docs missing either field
    missing_conditions = []
    if not args.skip_embed:
        missing_conditions.append({"matn_embedding": {"$exists": False}})
    if not args.skip_topics:
        missing_conditions.append({"topics": {"$exists": False}})

    if not missing_conditions:
        print("Nothing to do — both --skip-embed and --skip-topics are set.")
        sys.exit(0)

    pending_filter = {"$or": missing_conditions} if len(missing_conditions) > 1 else missing_conditions[0]

    total_all     = col.count_documents({})
    total_pending = col.count_documents(pending_filter)
    already_done  = total_all - total_pending

    print(f"  Total documents  : {total_all:,}")
    print(f"  Already complete : {already_done:,}")
    print(f"  Pending          : {total_pending:,}")

    if args.dry_run:
        if not args.skip_embed:
            embed_calls = (total_pending + embed_batch_size - 1) // embed_batch_size
            print(f"\n[dry-run] Cohere calls : ~{embed_calls} (batch={embed_batch_size})")
        if not args.skip_topics:
            llm_calls = (total_pending + llm_batch_size - 1) // llm_batch_size
            print(f"[dry-run] Gemini calls : ~{llm_calls} (batch={llm_batch_size})")
        print("[dry-run] No changes made.")
        sys.exit(0)

    if total_pending == 0:
        print("All documents already processed.")
        print_index_instructions()
        sys.exit(0)

    # Build clients
    embedder = build_embedder(cohere_key) if not args.skip_embed else None
    llm_chain = build_llm_chain(openrouter_key) if not args.skip_topics else None

    cursor = col.find(pending_filter, {"_id": 1, "matn_text_plain": 1, "hadith_url": 1})
    if args.limit > 0:
        cursor = cursor.limit(args.limit)

    effective_total = args.limit if args.limit > 0 else total_pending

    # Accumulation buffers (aligned: same docs in both)
    embed_buffer: list[dict] = []   # docs waiting for embedding
    topic_buffer: list[dict] = []   # docs waiting for topic tagging

    n_embedded   = 0
    n_tagged     = 0
    n_skipped    = 0
    n_write_errs = 0

    def flush_embed(docs: list[dict]):
        nonlocal n_embedded, n_write_errs
        valid = [(d, (d.get("matn_text_plain") or "").strip()) for d in docs]
        valid = [(d, t) for d, t in valid if t]
        if not valid:
            return
        valid_docs, texts = zip(*valid)
        vectors = embed_batch(embedder, list(texts))
        if vectors is None:
            return
        ops = [
            UpdateOne({"_id": d["_id"]}, {"$set": {"matn_embedding": v}})
            for d, v in zip(valid_docs, vectors)
        ]
        try:
            col.bulk_write(ops, ordered=False)
            n_embedded += len(ops)
        except BulkWriteError as bwe:
            n_embedded   += bwe.details.get("nModified", 0)
            n_write_errs += len(bwe.details.get("writeErrors", []))

    def flush_topics(docs: list[dict]):
        nonlocal n_tagged, n_skipped, n_write_errs
        valid = [(d, (d.get("matn_text_plain") or "").strip()) for d in docs]
        skipped = [d for d, t in valid if not t]
        n_skipped += len(skipped)
        for d in skipped:
            tqdm.write(f"  [WARN] skip _id={d['_id']} ({d.get('hadith_url','?')}): empty matn")
        valid = [(d, t) for d, t in valid if t]
        if not valid:
            return
        valid_docs, texts = zip(*valid)
        topics_list = tag_topics_batch(llm_chain, list(texts))
        if topics_list is None:
            return
        ops = [
            UpdateOne({"_id": d["_id"]}, {"$set": {"topics": tl}})
            for d, tl in zip(valid_docs, topics_list)
        ]
        try:
            col.bulk_write(ops, ordered=False)
            n_tagged += len(ops)
        except BulkWriteError as bwe:
            n_tagged     += bwe.details.get("nModified", 0)
            n_write_errs += len(bwe.details.get("writeErrors", []))

    with tqdm(total=effective_total, unit="doc", desc="Processing") as pbar:
        for doc in cursor:
            if embedder is not None:
                embed_buffer.append(doc)
                if len(embed_buffer) >= embed_batch_size:
                    flush_embed(embed_buffer)
                    pbar.update(len(embed_buffer))
                    embed_buffer = []

            if llm_chain is not None:
                topic_buffer.append(doc)
                if len(topic_buffer) >= llm_batch_size:
                    flush_topics(topic_buffer)
                    if embedder is None:  # only update bar here if not already updated
                        pbar.update(len(topic_buffer))
                    topic_buffer = []

        # Flush remainders
        if embed_buffer:
            flush_embed(embed_buffer)
            pbar.update(len(embed_buffer))
            embed_buffer = []
        if topic_buffer:
            flush_topics(topic_buffer)
            if embedder is None:
                pbar.update(len(topic_buffer))
            topic_buffer = []

    print(f"\n{'='*60}")
    if not args.skip_embed:
        print(f"  Embedded (matn_embedding) : {n_embedded:,}")
    if not args.skip_topics:
        print(f"  Tagged   (topics)         : {n_tagged:,}")
        print(f"  Skipped  (empty matn)     : {n_skipped:,}")
    if n_write_errs:
        print(f"  Write errors              : {n_write_errs:,}")
    print(f"{'='*60}")

    print_index_instructions()


if __name__ == "__main__":
    main()
