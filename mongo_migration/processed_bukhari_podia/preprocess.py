"""
Pre-processing transformations for Bukhari Pedia (podia) JSONL data.

Run this FIRST to produce cleaned JSONL files, then run upload.py to push
them to MongoDB Atlas.

Input (from extract_data_v2/playwrite/):
  hadith_narrators_bukhari_pedia_playwrite_preprocessing.jsonl
  narrators_bukhari_pedia_playwrite.jsonl

Input (from this directory — user-provided):
  bukhari_narrators_tarajem.jsonl

Output (to this directory):
  bukhari_podia_hadiths.jsonl     → collection: bukhari_book_podia
  bukhari_podia_narrators.jsonl   → collection: narrators_podia
  narrators_tarajem.jsonl         → collection: narrators_tarajem_podia

Usage:
    python mongo_migration/processed_bukhari_podia/preprocess.py

Text cleaning applied:
- Hadith index prefixes removed (e.g. "7427_ " or "3- ") using hadith_indices
- Scrape glyphs removed (☺♦♣♠☻╡√~•|{}[])
- Footnote markers removed (e.g. "( 1 )")
- صلعم expanded to صلى الله عليه وسلم
- Tatweel/kashida (ـ) removed
- Whitespace normalized
- Both tashkeel and plain-text variants stored for all text fields
- Narrators deduplicated by rawi_id
"""

import json
import pathlib
import re
import time

# ------------------------------------------------------------------
# Regex helpers (from hadith_preprocessing.py)
# ------------------------------------------------------------------

_TASHKEEL_RE = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)
_SCRAPE_GLYPHS = re.compile(r"[☺♦♣♠☻╡√~•|{}\[\]]")
_FOOTNOTE_MARKERS = re.compile(r"\(\s*\d+\s*\)")
_SALLAM_ABBREV = re.compile(r"\bصلعم\b")
_TATWEEL = re.compile(r"\u0640+")
_CHAIN_NUM_RE = re.compile(r"\s*\(\s*\d+\s*\)\s*")


def strip_tashkeel(text: str) -> str:
    """Remove Arabic diacritical marks from text."""
    if not text:
        return text
    return _TASHKEEL_RE.sub("", text)


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def clean_hadith_text(text: str, hadith_indices: list) -> str:
    """Full text cleaning for hadith text (keeps tashkeel)."""
    if not text:
        return text
    # Remove hadith index prefixes (both at start and inline)
    # e.g. "7427_ " or "7428- " or "3_4_ "
    for idx in hadith_indices:
        text = re.sub(rf"(?:^|\s){idx}\s*[_\-\u2013\u2014]\s*", " ", text)
    text = _SCRAPE_GLYPHS.sub("", text)
    text = _FOOTNOTE_MARKERS.sub("", text)
    text = _SALLAM_ABBREV.sub("صلى الله عليه وسلم", text)
    text = _TATWEEL.sub("", text)
    text = normalize_whitespace(text)
    return text


def clean_name_in_chain(name: str) -> str:
    """Remove (N) annotations, scrape glyphs, tatweel, and strip punctuation."""
    if not name:
        return name
    cleaned = _CHAIN_NUM_RE.sub("", name)
    cleaned = _SCRAPE_GLYPHS.sub("", cleaned)
    cleaned = _TATWEEL.sub("", cleaned)
    return normalize_whitespace(cleaned).strip(" \t\n\r،,.:؛")


# ------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------

_ROOT = pathlib.Path(__file__).parent.parent.parent  # hadathna_backend_app/
_INPUT_DIR  = _ROOT / "extract_data_v2" / "playwrite"
_OUTPUT_DIR = pathlib.Path(__file__).parent  # processed_bukhari_podia/

_HADITH_IN    = _INPUT_DIR / "hadith_narrators_bukhari_pedia_playwrite_preprocessing.jsonl"
_NARRATOR_IN  = _INPUT_DIR / "narrators_bukhari_pedia_playwrite.jsonl"
_TARAJEM_IN   = _OUTPUT_DIR / "bukhari_narrators_tarajem.jsonl"

_HADITH_OUT   = _OUTPUT_DIR / "bukhari_podia_hadiths.jsonl"
_NARRATOR_OUT = _OUTPUT_DIR / "bukhari_podia_narrators.jsonl"
_TARAJEM_OUT  = _OUTPUT_DIR / "narrators_tarajem.jsonl"


# ------------------------------------------------------------------
# Processing
# ------------------------------------------------------------------

def process_hadiths():
    """Process raw podia hadith JSONL → cleaned output."""
    if not _HADITH_IN.exists():
        print(f"[SKIP] Input not found: {_HADITH_IN}")
        return 0

    count = skipped = 0
    t0 = time.time()

    with open(_HADITH_IN, encoding="utf-8") as fin, \
         open(_HADITH_OUT, "w", encoding="utf-8") as fout:
        for lineno, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  [WARN] line {lineno}: JSON parse error — {exc}")
                continue

            if not doc.get("is_hadith", False):
                skipped += 1
                continue

            raw_text = doc.get("hadith_text", "")
            hadith_indices = doc.get("hadith_indices", [])

            # Clean text (keeps tashkeel)
            hadith_text = clean_hadith_text(raw_text, hadith_indices)

            narrators = []
            for n in doc.get("narrators", []):
                rank = n.get("rank", "")
                name_clean = clean_name_in_chain(n.get("name_in_chain", ""))
                narrators.append({
                    "rawi_id": n["rawi_id"],
                    "name_in_chain": n.get("name_in_chain", ""),
                    "name_in_chain_clean": name_clean,
                    "name_in_chain_plain": strip_tashkeel(name_clean),
                    "full_name": n.get("full_name", ""),
                    "rank": rank,
                    "rank_plain": strip_tashkeel(rank),
                })

            out = {
                "hadith_url": doc.get("hadith_url", ""),
                "hadith_indices": hadith_indices,
                "source": "bukhari_podia",
                "book": doc.get("book", ""),
                "chapter": doc.get("chapter", ""),
                "hadith_text": hadith_text,
                "hadith_text_plain": strip_tashkeel(hadith_text),
                "narrators": narrators,
            }

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            count += 1

    elapsed = time.time() - t0
    print(f"Hadiths: {count} written, {skipped} skipped (is_hadith=false), {elapsed:.1f}s")
    print(f"  → {_HADITH_OUT}")
    return count


def process_narrators():
    """Process raw podia narrator JSONL → cleaned & deduplicated output."""
    if not _NARRATOR_IN.exists():
        print(f"[SKIP] Input not found: {_NARRATOR_IN}")
        return 0

    seen_ids = set()
    count = duplicates = 0
    t0 = time.time()

    with open(_NARRATOR_IN, encoding="utf-8") as fin, \
         open(_NARRATOR_OUT, "w", encoding="utf-8") as fout:
        for lineno, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  [WARN] line {lineno}: JSON parse error — {exc}")
                continue

            rawi_id = doc.get("rawi_id")
            if rawi_id in seen_ids:
                duplicates += 1
                continue
            seen_ids.add(rawi_id)

            full_name = doc.get("full_name", "")
            rank = doc.get("rank", "")

            name_clean = clean_name_in_chain(doc.get("name_in_chain", ""))
            out = {
                "rawi_id": rawi_id,
                "name_in_chain": doc.get("name_in_chain", ""),
                "name_in_chain_clean": name_clean,
                "name_in_chain_plain": strip_tashkeel(name_clean),
                "full_name": full_name,
                "full_name_plain": strip_tashkeel(full_name),
                "rank": rank,
                "rank_plain": strip_tashkeel(rank),
                "full_tooltip_info": doc.get("full_tooltip_info", ""),
            }

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            count += 1

    elapsed = time.time() - t0
    print(f"Narrators: {count} unique written, {duplicates} duplicates skipped, {elapsed:.1f}s")
    print(f"  → {_NARRATOR_OUT}")
    return count


def process_tarajem():
    """Process tarajem JSONL → cleaned output with plain text variants."""
    if not _TARAJEM_IN.exists():
        print(f"[SKIP] Input not found: {_TARAJEM_IN}")
        return 0

    seen_ids = set()
    count = duplicates = 0
    t0 = time.time()

    with open(_TARAJEM_IN, encoding="utf-8") as fin, \
         open(_TARAJEM_OUT, "w", encoding="utf-8") as fout:
        for lineno, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  [WARN] line {lineno}: JSON parse error — {exc}")
                continue

            rawi_id = doc.get("rawi_id")
            if rawi_id in seen_ids:
                duplicates += 1
                continue
            seen_ids.add(rawi_id)

            full_name = doc.get("full_name", "")
            rank = doc.get("rank", "")

            # Process tarajim entries
            tarajim = []
            for t in doc.get("tarajim", []):
                tarjama = t.get("tarjama", "")
                tarajim.append({
                    "source": t.get("source", ""),
                    "tarjama": tarjama,
                    "tarjama_plain": strip_tashkeel(tarjama),
                })

            # Process narrator_info entries
            narrator_info = []
            for ni in doc.get("narrator_info", []):
                text = ni.get("text", "")
                narrator_info.append({
                    "action": ni.get("action", ""),
                    "text": text,
                    "text_plain": strip_tashkeel(text),
                })

            name_clean = clean_name_in_chain(doc.get("name_in_chain", ""))
            out = {
                "rawi_id": rawi_id,
                "url": doc.get("url", ""),
                "name_in_chain": doc.get("name_in_chain", ""),
                "name_in_chain_clean": name_clean,
                "name_in_chain_plain": strip_tashkeel(name_clean),
                "full_name": full_name,
                "full_name_plain": strip_tashkeel(full_name),
                "rank": rank,
                "rank_plain": strip_tashkeel(rank),
                "narrator_info": narrator_info,
                "tarajim": tarajim,
            }

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            count += 1

    elapsed = time.time() - t0
    print(f"Tarajem: {count} unique written, {duplicates} duplicates skipped, {elapsed:.1f}s")
    print(f"  → {_TARAJEM_OUT}")
    return count


def main():
    print("=" * 60)
    print("Pre-processing Bukhari Podia data")
    print("=" * 60)

    process_hadiths()
    print()
    process_narrators()
    print()
    process_tarajem()

    print("\nDone.")


if __name__ == "__main__":
    main()
