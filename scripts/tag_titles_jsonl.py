#!/usr/bin/env python3
"""
Generate short Arabic titles for Podia hadiths — JSONL-first, no MongoDB required.

Reads `mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl`
directly and writes a slim output file `hadith_titles.jsonl` at the repo root:

    {"hadith_url": "...", "title": "الأعمال بالنيات وثواب كل امرئ على قصده"}

Each title is a single Arabic headline ≤ 150 characters summarising the hadith.
The source JSONL is NEVER modified.

RESUME
------
On startup, any `hadith_url` already present in the output file is skipped.
Re-running the script after an interruption continues from where it left off.

IMPORT INTO HadithDataDev
-------------------------
After the script completes, upsert titles into local MongoDB:

    docker exec -i mongodb-hadathana mongoimport --db HadithDataDev \\
      --collection processed_podia_books --mode=upsert --upsertFields=hadith_url \\
      < hadith_titles.jsonl

USAGE
-----
    python scripts/tag_titles_jsonl.py               # full run
    python scripts/tag_titles_jsonl.py --dry-run     # count pending, estimate calls, exit
    python scripts/tag_titles_jsonl.py --limit 5     # smoke test on 5 hadiths
    python scripts/tag_titles_jsonl.py --llm-batch 10

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
OUTPUT_PATH = _REPO_ROOT / "hadith_titles.jsonl"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LLM_MODEL     = "google/gemini-3-flash-preview"
LLM_BATCH_MAX = 20

MAX_RETRIES_LLM  = 3
RETRY_BASE_DELAY = 2.0

# ---------------------------------------------------------------------------
# Pydantic output models (LangChain structured output)
# ---------------------------------------------------------------------------


class HadithTitle(BaseModel):
    title: str = Field(
        ...,
        max_length=150,
        description="عنوان عربي مختصر لا يتجاوز 150 حرفاً يلخّص معنى الحديث",
    )


class BatchTitles(BaseModel):
    results: list[HadithTitle] = Field(
        ...,
        description="عناوين لكل حديث في الدفعة بنفس الترتيب",
    )


# ---------------------------------------------------------------------------
# LLM chain
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "أنت مساعد متخصص في صياغة عناوين موجزة لأحاديث صحيح البخاري. "
    "لكل حديث اكتب عنواناً واحداً لا يتجاوز 150 حرفاً يلخّص معنى الحديث. "
    "يجب أن يكون العنوان:\n"
    "- موجزاً ودالاً على مضمون الحديث\n"
    "- بالعربية الفصحى\n"
    "- خالياً من علامات الترقيم غير الضرورية\n"
    "- مناسباً للاستخدام كعنوان في واجهة مستخدم"
)

_HUMAN_TEMPLATE = (
    "اكتب عنواناً مختصراً لكل حديث من الأحاديث التالية. "
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
    return prompt | llm.with_structured_output(BatchTitles)


def tag_titles_batch(
    chain,
    items: list[dict],
    retries: int = MAX_RETRIES_LLM,
) -> list[str] | None:
    """
    items: list of dicts with keys 'chapter' and 'matn_text_plain'.
    Returns list of title strings in same order, or None on total failure.
    """
    hadiths_block = "\n\n".join(
        f"[{i + 1}] {item['chapter']} | {item['matn_text_plain']}"
        for i, item in enumerate(items)
    )
    for attempt in range(retries):
        try:
            result: BatchTitles = chain.invoke({"hadiths_block": hadiths_block})
            titles = [h.title for h in result.results]
            if len(titles) < len(items):
                titles += [""] * (len(items) - len(titles))
            return titles[: len(items)]
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
        description="Generate short Arabic titles for Podia hadiths — writes slim hadith_titles.jsonl.",
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
        print("All hadiths already have titles.")
        return

    llm_chain = build_llm_chain(openrouter_key)

    n_titled  = 0
    n_skipped = 0
    n_failed  = 0
    out_file  = open(OUTPUT_PATH, "a", encoding="utf-8")

    try:
        with tqdm(total=len(pending), unit="hadith", desc="Generating titles") as pbar:
            buffer: list[dict] = []

            def flush(batch: list[dict]):
                nonlocal n_titled, n_skipped, n_failed
                valid = [
                    (d, {
                        "chapter": (d.get("chapter") or "").strip(),
                        "matn_text_plain": (d.get("matn_text_plain") or "").strip(),
                    })
                    for d in batch
                ]
                skipped = [d for d, item in valid if not item["matn_text_plain"]]
                n_skipped += len(skipped)
                for d in skipped:
                    tqdm.write(f"  [WARN] skip {d.get('hadith_url','?')}: empty matn")
                valid = [(d, item) for d, item in valid if item["matn_text_plain"]]
                if not valid:
                    return
                valid_docs, items = zip(*valid)
                titles = tag_titles_batch(llm_chain, list(items))
                if titles is None:
                    n_failed += len(valid_docs)
                    tqdm.write(f"  [FAIL] {len(valid_docs)} hadiths lost (LLM error)")
                    return
                for doc, title in zip(valid_docs, titles):
                    row = {"hadith_url": doc["hadith_url"], "title": title}
                    out_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                    n_titled += 1
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
    print(f"  Titled   : {n_titled:,}")
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
