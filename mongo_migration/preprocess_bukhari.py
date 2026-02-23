"""
Pre-processing for Bukhari LLM-extracted JSON data + tashkeel from CSV.

Sources:
  extract_data_v2/Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json
  extract_data_v2/Bukhari/Bukhari_Tashkel.csv   (one hadith per row, row N = hadith_index N)

Output:
  mongo_migration/processed/preprocessed_bukhari.jsonl

Schema per document:
  hadith_index          int        - 1-based Bukhari hadith number
  source                str        - "bukhari"
  hadith                str        - full hadith text WITH tashkeel (from CSV)
  hadith_plain          str        - full hadith text WITHOUT tashkeel
  matn_plain            list[str]  - matn segment(s) without tashkeel
                                     (LLM-extracted; tashkeel matn skipped because
                                      the LLM sometimes paraphrased rather than copied verbatim)
  n_matn                int        - number of matn segments
  chains                list       - narrator chain(s)
    chain_id            str
    type                str        - "primary" | "nested" | "follow_up"
    narrators           list       - ordered collector → lead (as in the sanad)
      name              str        - narrator name
      role              str        - "narrator" | "lead"
      narrator_id       int|null   - shamela narrator ID (null if unresolved)
  n_chains              int        - number of chains
  unique_narrators      list[obj]  - deduplicated {name, narrator_id} across all chains,
                                     ordered lead-first (idx 0 = lead / closest to Prophet)
                                     narrator_id is null if unresolved

Dropped fields (pipeline metadata):
  model_used, route_reason, narrator_id_resolution, attributes, name_plain

Usage:
    python3 mongo_migration/preprocess_bukhari.py
"""

import json
import pathlib
import re
import time
from typing import Optional

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TASHKEEL_RE = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)


def strip_tashkeel(text: str) -> str:
    if not text:
        return text
    return _TASHKEEL_RE.sub("", text)


def _cast_narrator_id(value) -> Optional[int]:
    """Convert narrator_id string/int to int; return None for null / unresolved."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Record transformation
# ---------------------------------------------------------------------------

def _process_narrator(n: dict) -> dict:
    name = n.get("name") or ""
    role = (n.get("attributes") or {}).get("role")
    return {
        "name": name,
        "role": role,
        "narrator_id": _cast_narrator_id(n.get("narrator_id")),
    }


def _process_chain(chain: dict) -> dict:
    return {
        "chain_id": chain.get("chain_id"),
        "type": chain.get("type"),
        "narrators": [_process_narrator(n) for n in (chain.get("narrators") or [])],
    }


def process_bukhari_hadith(raw: dict, hadith_tashkeel: str) -> dict:
    matn_segments = raw.get("matn_segments") or []
    chains = [_process_chain(c) for c in (raw.get("chains") or [])]

    # Build unique narrators list ordered lead-first (reversed sanad order).
    # Collect all narrators from all chains in reversed order so that the lead
    # (last in chain = closest to Prophet) appears first, then deduplicate by name
    # while preserving that lead-first ordering.
    seen_names: set = set()
    unique_narrators = []
    for chain in chains:
        for n in reversed(chain["narrators"]):
            if n["name"] and n["name"] not in seen_names:
                seen_names.add(n["name"])
                unique_narrators.append({"name": n["name"], "narrator_id": n["narrator_id"]})

    return {
        "hadith_index": raw.get("hadith_index"),
        "source": "bukhari",
        "hadith": hadith_tashkeel,
        "hadith_plain": strip_tashkeel(hadith_tashkeel),
        "matn_plain": [strip_tashkeel(s) for s in matn_segments if s],
        "n_matn": len(matn_segments),
        "n_chains": len(chains),
        "chains": chains,
        "unique_narrators": unique_narrators,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_ROOT = pathlib.Path(__file__).parent.parent
_SRC_JSON = _ROOT / "extract_data_v2" / "Bukhari" / "Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json"
_SRC_CSV = _ROOT / "extract_data_v2" / "Bukhari" / "Bukhari_Tashkel.csv"
_OUT_DIR = pathlib.Path(__file__).parent / "processed"
_OUT = _OUT_DIR / "preprocessed_bukhari.jsonl"


def _load_csv_texts(path: pathlib.Path) -> list:
    """Load CSV as a list of hadith texts (index 0 = hadith_index 1)."""
    lines = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            stripped = line.rstrip("\n")
            if stripped.strip():
                lines.append(stripped)
    # lines[0] is the header ("Sahih Bukhari"), skip it
    return lines[1:]


def main():
    for path in (_SRC_JSON, _SRC_CSV):
        if not path.exists():
            print(f"[ERROR] Source file not found: {path}")
            return

    _OUT_DIR.mkdir(exist_ok=True)

    t0 = time.time()
    print(f"\n{'='*60}")
    print(f"JSON   : {_SRC_JSON}")
    print(f"CSV    : {_SRC_CSV}")
    print(f"Output : {_OUT}")
    print(f"{'='*60}")

    with open(_SRC_JSON, encoding="utf-8") as f:
        records = json.load(f)

    csv_texts = _load_csv_texts(_SRC_CSV)

    if len(csv_texts) != len(records):
        print(f"[WARN] CSV rows ({len(csv_texts)}) != JSON records ({len(records)})")

    # Sort JSON records by hadith_index to ensure correct CSV alignment
    records.sort(key=lambda r: r.get("hadith_index") or 0)

    processed = []
    for r in records:
        idx = r.get("hadith_index", 0) - 1  # 0-based
        tashkeel_text = csv_texts[idx] if 0 <= idx < len(csv_texts) else ""
        processed.append(process_bukhari_hadith(r, tashkeel_text))

    with open(_OUT, "w", encoding="utf-8") as fout:
        for doc in processed:
            fout.write(json.dumps(doc, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0
    print(f"  Records  : {len(processed)}")
    print(f"  Time     : {elapsed:.1f}s")
    print(f"\nDone. Inspect {_OUT} then run upload.py to push to MongoDB.")


if __name__ == "__main__":
    main()
