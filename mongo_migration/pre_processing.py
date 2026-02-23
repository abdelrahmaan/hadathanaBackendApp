"""
Pre-processing transformations for shamela JSONL data.

Run this FIRST to produce cleaned JSONL files you can inspect,
then run upload.py to push them to MongoDB Atlas.

Output directory: mongo_migration/processed/
  hadith_pages.jsonl   ← cleaned from shamela_book_1681.jsonl
  narrators.jsonl      ← cleaned from shamela_narrators.jsonl

Usage:
    python mongo_migration/pre_processing.py

Rules applied:
- Only status == "success" records are kept
- `url` / `breadcrumb_links` / narrator `url` fields are dropped
- Leading Arabic numeral prefixes stripped from text  (e.g. "٧٥٣٦ - حَدَّثَنِي" → "حَدَّثَنِي")
- Both tashkeel and plain-text (tashkeel-stripped) variants stored for text fields
"""

import json
import pathlib
import re
import sys
import time
from typing import Optional

# ------------------------------------------------------------------
# Regex helpers
# ------------------------------------------------------------------

_TASHKEEL_RE = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)

# Leading Arabic-Indic digits (٠-٩) or ASCII digits + optional spaces
# followed by a hyphen/en-dash/em-dash, then optional spaces.
_HADITH_NUM_RE = re.compile(r"^[\u0660-\u0669\u0030-\u0039\s]+[-\u2013\u2014]+\s*")


def strip_tashkeel(text: str) -> str:
    """Remove Arabic diacritical marks from text."""
    if not text:
        return text
    return _TASHKEEL_RE.sub("", text)


def strip_hadith_number(text: str) -> str:
    """Remove leading hadith number prefix (e.g. '٧٥٣٦ - ')."""
    if not text:
        return text
    return _HADITH_NUM_RE.sub("", text).strip()


_HADITH_IDX_CAPTURE_RE = re.compile(r"^([\u0660-\u0669\u0030-\u0039]+)")
_ARABIC_TO_INT = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def extract_hadith_number(text: str) -> Optional[int]:
    """Return the leading hadith number as a Western integer, or None."""
    if not text:
        return None
    m = _HADITH_IDX_CAPTURE_RE.match(text.strip())
    if not m:
        return None
    try:
        return int(m.group(1).translate(_ARABIC_TO_INT))
    except ValueError:
        return None


# ------------------------------------------------------------------
# Record transformations
# ------------------------------------------------------------------

def _process_block(block: dict) -> dict:
    """Clean a single hadith_block entry."""
    full_text_raw = block.get("full_text") or ""
    hadith_number = extract_hadith_number(full_text_raw)
    full_text = strip_hadith_number(full_text_raw)
    matn = strip_hadith_number(block.get("matn") or "")

    narrators = [
        {"id": n.get("id"), "name": n.get("name"), "name_plain": strip_tashkeel(n.get("name") or "")}
        for n in (block.get("narrators") or [])
    ]

    return {
        "hadith_number": hadith_number,
        "full_text": full_text,
        "full_text_plain": strip_tashkeel(full_text),
        "matn": matn,
        "matn_plain": strip_tashkeel(matn),
        "narrators": narrators,
    }


def process_hadith_page(raw: dict) -> dict:
    """
    Transform one shamela_book_1681.jsonl record.
    Drops : url, breadcrumb_links, narrator url
    Adds  : full_text_plain, matn_plain
    """
    return {
        "status": raw.get("status"),
        "book_id": raw.get("book_id"),
        "page_number": raw.get("page_number"),
        "hadith_blocks": [
            _process_block(b) for b in (raw.get("hadith_blocks") or [])
        ],
    }


def process_narrator(raw: dict) -> dict:
    """
    Transform one shamela_narrators.jsonl record.
    Drops : url
    Adds  : name_plain
    """
    name = raw.get("name") or ""
    doc = {
        "status": raw.get("status"),
        "narrator_id": raw.get("narrator_id"),
        "name": name,
        "name_plain": strip_tashkeel(name),
    }
    for field in (
        "kunya", "nasab", "death_date", "birth_date",
        "tabaqa", "rank_ibn_hajar", "rank_dhahabi",
        "relations", "aqeeda", "jarh_wa_tadil",
    ):
        value = raw.get(field)
        if value is not None:
            doc[field] = value
    return doc


# ------------------------------------------------------------------
# JSONL writer
# ------------------------------------------------------------------

def process_file(src: pathlib.Path, dest: pathlib.Path, processor, sort_key=None):
    t0 = time.time()
    kept = skipped = 0
    print(f"\n{'='*60}")
    print(f"Input  : {src}")
    print(f"Output : {dest}")
    print(f"{'='*60}")

    records = []
    with open(src, encoding="utf-8") as fin:
        for lineno, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  [WARN] line {lineno}: JSON parse error — {exc}")
                skipped += 1
                continue

            if raw.get("status") != "success":
                skipped += 1
                continue

            records.append(processor(raw))
            kept += 1

    if sort_key:
        records.sort(key=sort_key)

    with open(dest, "w", encoding="utf-8") as fout:
        for rec in records:
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0
    print(f"  Written  : {kept}")
    print(f"  Skipped  : {skipped}  (non-success records)")
    print(f"  Time     : {elapsed:.1f}s")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

_ROOT = pathlib.Path(__file__).parent.parent
_FIRECRAWL = _ROOT / "extract_data_v2" / "firecrawl"
_OUT = pathlib.Path(__file__).parent / "processed"

def _hadith_sort_key(r: dict):
    """Sort hadith pages by first block's hadith_number; None values go last."""
    blocks = r.get("hadith_blocks")
    if blocks and blocks[0].get("hadith_number") is not None:
        return blocks[0]["hadith_number"]
    return float("inf")


SOURCES = [
    (_FIRECRAWL / "shamela_book_1681.jsonl", _OUT / "hadith_pages.jsonl", process_hadith_page, _hadith_sort_key),
    (_FIRECRAWL / "shamela_narrators.jsonl",  _OUT / "narrators.jsonl",    process_narrator,    None),
]


def main():
    _OUT.mkdir(exist_ok=True)
    print(f"Output directory: {_OUT}")

    for src, dest, processor, sort_key in SOURCES:
        if not src.exists():
            print(f"\n[SKIP] File not found: {src}")
            continue
        process_file(src, dest, processor, sort_key=sort_key)

    print("\nPre-processing complete.")
    print(f"Inspect the files in {_OUT} before running upload.py")


if __name__ == "__main__":
    main()
