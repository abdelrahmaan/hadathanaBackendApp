#!/usr/bin/env python3
"""
Tag Podia hadiths with Arabic topic labels — JSONL-first, no MongoDB required.

Reads `mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl`
directly and writes a slim output file `hadith_topics.jsonl` at the repo root:

    {"hadith_url": "...", "topics": ["الصلاة", "الإيمان"]}

The source JSONL is NEVER modified.

RESUME
------
On startup, any `hadith_url` already present in the output file is skipped.
Re-running the script after an interruption continues from where it left off.

IMPORT INTO HadithDataDev
-------------------------
After the script completes, upsert topics into local MongoDB:

    docker exec -i mongodb-hadathana mongoimport --db HadithDataDev \\
      --collection processed_podia_books --mode=upsert --upsertFields=hadith_url \\
      < hadith_topics.jsonl

USAGE
-----
    python scripts/tag_topics_jsonl.py               # full run
    python scripts/tag_topics_jsonl.py --dry-run     # count pending, estimate calls, exit
    python scripts/tag_topics_jsonl.py --limit 5     # smoke test on 5 hadiths
    python scripts/tag_topics_jsonl.py --llm-batch 10

ENV VARS REQUIRED (in .env)
----------------------------
    OPENROUTER_API_KEY  — OpenRouter API key (for Gemini Flash)
"""

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH  = _REPO_ROOT / "mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl"
OUTPUT_PATH = _REPO_ROOT / "hadith_topics.jsonl"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LLM_MODEL     = "google/gemini-3-flash-preview"
LLM_BATCH_MAX = 20

MAX_RETRIES_LLM  = 3
RETRY_BASE_DELAY = 2.0

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
# LLM chain
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
    hadiths_block = "\n\n".join(
        f"[{i + 1}] {text}" for i, text in enumerate(texts)
    )
    for attempt in range(retries):
        try:
            result: BatchTopics = chain.invoke({"hadiths_block": hadiths_block})
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
# I/O helpers
# ---------------------------------------------------------------------------


def load_already_done(output_path: Path) -> set[str]:
    """Return set of hadith_url values already written to the output file."""
    done: set[str] = set()
    if not output_path.exists():
        return done
    with open(output_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                url = row.get("hadith_url")
                if url:
                    done.add(url)
            except json.JSONDecodeError:
                pass
    return done


def load_input(input_path: Path) -> list[dict]:
    if not input_path.exists():
        sys.exit(f"ERROR: input file not found: {input_path}\n"
                 "  Pull from R2 first:\n"
                 "  python scripts/r2_sync/pull_snapshot.py --dataset bukhari_podia --latest")
    with open(input_path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    p = argparse.ArgumentParser(
        description="Tag Podia hadiths with Arabic topics — writes slim hadith_topics.jsonl.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Count pending docs and estimate API calls, then exit.")
    p.add_argument("--llm-batch", type=int, default=LLM_BATCH_MAX, metavar="N",
                   help=f"Hadiths per Gemini call (max {LLM_BATCH_MAX}, default {LLM_BATCH_MAX}).")
    p.add_argument("--limit", type=int, default=0,
                   help="Process at most N hadiths (0 = all). Useful for smoke tests.")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    args = parse_args()
    llm_batch_size = min(args.llm_batch, LLM_BATCH_MAX)

    load_dotenv(_REPO_ROOT / ".env")
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not openrouter_key and not args.dry_run:
        sys.exit("ERROR: OPENROUTER_API_KEY not set in .env")

    print(f"Input  : {INPUT_PATH}")
    print(f"Output : {OUTPUT_PATH}")

    docs = load_input(INPUT_PATH)
    print(f"  Loaded {len(docs):,} documents from input")

    done_urls = load_already_done(OUTPUT_PATH)
    print(f"  Already done : {len(done_urls):,}")

    pending = [d for d in docs if d.get("hadith_url") not in done_urls]
    if args.limit > 0:
        pending = pending[: args.limit]

    print(f"  Pending      : {len(pending):,}")

    if args.dry_run:
        llm_calls = (len(pending) + llm_batch_size - 1) // llm_batch_size
        print(f"\n[dry-run] LLM calls (Gemini) : ~{llm_calls} (batch={llm_batch_size})")
        print("[dry-run] No changes made.")
        return

    if not pending:
        print("All hadiths already tagged.")
        return

    llm_chain = build_llm_chain(openrouter_key)

    n_tagged   = 0
    n_skipped  = 0
    n_failed   = 0
    out_file   = open(OUTPUT_PATH, "a", encoding="utf-8")

    try:
        with tqdm(total=len(pending), unit="hadith", desc="Tagging topics") as pbar:
            buffer: list[dict] = []

            def flush(batch: list[dict]):
                nonlocal n_tagged, n_skipped, n_failed
                valid = [(d, (d.get("matn_text_plain") or "").strip()) for d in batch]
                skipped = [d for d, t in valid if not t]
                n_skipped += len(skipped)
                for d in skipped:
                    tqdm.write(f"  [WARN] skip {d.get('hadith_url','?')}: empty matn")
                valid = [(d, t) for d, t in valid if t]
                if not valid:
                    return
                valid_docs, texts = zip(*valid)
                topics_list = tag_topics_batch(llm_chain, list(texts))
                if topics_list is None:
                    n_failed += len(valid_docs)
                    tqdm.write(f"  [FAIL] {len(valid_docs)} hadiths lost (LLM error)")
                    return
                for doc, topics in zip(valid_docs, topics_list):
                    row = {"hadith_url": doc["hadith_url"], "topics": topics}
                    out_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                    n_tagged += 1
                out_file.flush()

            for doc in pending:
                buffer.append(doc)
                if len(buffer) >= llm_batch_size:
                    flush(buffer)
                    pbar.update(len(buffer))
                    buffer = []

            if buffer:
                flush(buffer)
                pbar.update(len(buffer))

    finally:
        out_file.close()

    print(f"\n{'='*60}")
    print(f"  Tagged   : {n_tagged:,}")
    print(f"  Skipped  : {n_skipped:,}  (empty matn)")
    print(f"  Failed   : {n_failed:,}  (LLM error — re-run to retry)")
    print(f"  Output   : {OUTPUT_PATH}")
    print(f"{'='*60}")
    print("\nNext step — import into HadithDataDev:")
    print("  docker exec -i mongodb-hadathana mongoimport --db HadithDataDev \\")
    print("    --collection processed_podia_books --mode=upsert --upsertFields=hadith_url \\")
    print(f"    < {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
