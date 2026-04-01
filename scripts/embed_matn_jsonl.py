#!/usr/bin/env python3
"""
Embed Podia hadith matn text with Cohere embed-v4.0 — JSONL-first, no MongoDB required.

Reads `mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl`
directly and writes a slim output file `hadith_embeddings.jsonl` at the repo root:

    {"hadith_url": "...", "matn_embedding": [0.123, ...]}  # 1536-dim float32

The source JSONL is NEVER modified.

EMBEDDING ASYMMETRY NOTE
------------------------
This script uses input_type="search_document" (offline indexing).
At query time (search endpoint / chatbot), embed the user query with:

    from langchain_cohere import CohereEmbeddings
    embedder = CohereEmbeddings(model="embed-v4.0", cohere_api_key=COHERE_API_KEY)
    # embed_query() uses input_type="search_query" internally — correct for queries.
    query_vector = embedder.embed_query(user_question)

RESUME
------
On startup, any `hadith_url` already present in the output file is skipped.
Re-running the script after an interruption continues from where it left off.

USAGE
-----
    python scripts/embed_matn_jsonl.py               # full run
    python scripts/embed_matn_jsonl.py --dry-run     # count pending, estimate calls, exit
    python scripts/embed_matn_jsonl.py --limit 5     # smoke test on 5 hadiths
    python scripts/embed_matn_jsonl.py --batch-size 48

ENV VARS REQUIRED (in .env)
----------------------------
    COHERE_API_KEY  — Cohere API key
"""

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from langchain_cohere import CohereEmbeddings
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT  = Path(__file__).resolve().parent.parent
INPUT_PATH  = _REPO_ROOT / "mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl"
OUTPUT_PATH = _REPO_ROOT / "hadith_embeddings.jsonl"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COHERE_MODEL    = "embed-v4.0"
EMBED_BATCH_MAX = 96   # Cohere API hard limit per request

MAX_RETRIES_EMBED = 5
RETRY_BASE_DELAY  = 2.0

# ---------------------------------------------------------------------------
# Cohere embedding
# ---------------------------------------------------------------------------


def build_embedder(cohere_key: str) -> CohereEmbeddings:
    # embed_documents() hardcodes input_type="search_document" internally — correct for indexing.
    return CohereEmbeddings(
        model=COHERE_MODEL,
        cohere_api_key=cohere_key,
    )


def embed_batch(
    embedder: CohereEmbeddings,
    texts: list[str],
    retries: int = MAX_RETRIES_EMBED,
) -> list[list[float]] | None:
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
        description="Embed Podia hadith matn — writes slim hadith_embeddings.jsonl.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Count pending docs and estimate API calls, then exit.")
    p.add_argument("--batch-size", type=int, default=EMBED_BATCH_MAX, metavar="N",
                   help=f"Cohere embed batch size (max {EMBED_BATCH_MAX}, default {EMBED_BATCH_MAX}).")
    p.add_argument("--limit", type=int, default=0,
                   help="Process at most N hadiths (0 = all). Useful for smoke tests.")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    args = parse_args()
    embed_batch_size = min(args.batch_size, EMBED_BATCH_MAX)

    load_dotenv(_REPO_ROOT / ".env")
    cohere_key = os.environ.get("COHERE_API_KEY", "")
    if not cohere_key and not args.dry_run:
        sys.exit("ERROR: COHERE_API_KEY not set in .env")

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
        embed_calls = (len(pending) + embed_batch_size - 1) // embed_batch_size
        print(f"\n[dry-run] Cohere calls : ~{embed_calls} (batch={embed_batch_size})")
        print("[dry-run] No changes made.")
        return

    if not pending:
        print("All hadiths already embedded.")
        return

    embedder = build_embedder(cohere_key)

    n_embedded = 0
    n_skipped  = 0
    n_failed   = 0
    out_file   = open(OUTPUT_PATH, "a", encoding="utf-8")

    try:
        with tqdm(total=len(pending), unit="hadith", desc="Embedding matn") as pbar:
            buffer: list[dict] = []

            def flush(batch: list[dict]):
                nonlocal n_embedded, n_skipped, n_failed
                valid = [(d, (d.get("matn_text_plain") or "").strip()) for d in batch]
                skipped = [d for d, t in valid if not t]
                n_skipped += len(skipped)
                for d in skipped:
                    tqdm.write(f"  [WARN] skip {d.get('hadith_url','?')}: empty matn")
                valid = [(d, t) for d, t in valid if t]
                if not valid:
                    return
                valid_docs, texts = zip(*valid)
                vectors = embed_batch(embedder, list(texts))
                if vectors is None:
                    n_failed += len(valid_docs)
                    tqdm.write(f"  [FAIL] {len(valid_docs)} hadiths lost (Cohere error)")
                    return
                for doc, vector in zip(valid_docs, vectors):
                    row = {"hadith_url": doc["hadith_url"], "matn_embedding": vector}
                    out_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                    n_embedded += 1
                out_file.flush()

            for doc in pending:
                buffer.append(doc)
                if len(buffer) >= embed_batch_size:
                    flush(buffer)
                    pbar.update(len(buffer))
                    buffer = []

            if buffer:
                flush(buffer)
                pbar.update(len(buffer))

    finally:
        out_file.close()

    print(f"\n{'='*60}")
    print(f"  Embedded : {n_embedded:,}")
    print(f"  Skipped  : {n_skipped:,}  (empty matn)")
    print(f"  Failed   : {n_failed:,}  (Cohere error — re-run to retry)")
    print(f"  Output   : {OUTPUT_PATH}")
    print(f"{'='*60}")
    print("\nNote: embeddings are NOT imported into MongoDB.")
    print("They stay in hadith_embeddings.jsonl for Qdrant / vector DB ingestion.")


if __name__ == "__main__":
    main()
