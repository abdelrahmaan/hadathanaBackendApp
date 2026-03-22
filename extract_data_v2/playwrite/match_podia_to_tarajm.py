"""
Match podia narrator records to tarajm_people entries.

Input:
  - playwrite_bukhari_hadith_podia_clean.jsonl  (1,017 narrators with full_name, name_in_chain)
  - ../../tarajm/out_people_csv/tarajm_people.parquet  (26,771 people)

Matching priority per narrator:
  1. Exact match: normalize(full_name) or normalize(name_in_chain) == normalize(tarajm.name)
  2. Summary substring: normalize(full_name) appears inside normalize(tarajm.summary)
  3. Token Jaccard >= 0.25 on full_name tokens vs tarajm name tokens (top-3)

Output:
  - playwrite_bukhari_hadith_podia_with_tarajm.jsonl  — podia records + tarajm_candidates array
  - unmatched_podia_tarajm.jsonl                      — narrators with no tarajm match

Usage:
    python3 match_podia_to_tarajm.py
"""

import csv
import json
import re
import unicodedata
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE = Path(__file__).parent
PODIA_PATH   = BASE / "playwrite_bukhari_hadith_podia_clean.jsonl"
TARAJM_PATH  = BASE.parent.parent / "tarajm" / "out_people_csv" / "tarajm_people.csv"
OUT_PATH     = BASE / "playwrite_bukhari_hadith_podia_with_tarajm.jsonl"
UNMATCHED_PATH = BASE / "unmatched_podia_tarajm.jsonl"

# ---------------------------------------------------------------------------
# Thresholds (same as match_bukhari_narrators.py)
# ---------------------------------------------------------------------------

HIGH_CONFIDENCE = 0.5
LOW_CONFIDENCE  = 0.25
TOP_N = 3

TITLE_WORDS = {
    "الإمام", "الشيخ", "الحافظ", "الحجة", "الثقة", "القاضي", "الفقيه",
    "الأمير", "السيد", "سيدي", "مولانا", "الشريف", "العلامة", "الحاج",
    "أبو", "أبي", "ابن", "بن", "بنت", "ابنة", "آل",
}

# ---------------------------------------------------------------------------
# Normalization (copied from tarajm/match_bukhari_narrators.py)
# ---------------------------------------------------------------------------

def normalize(name: str) -> str:
    if not name:
        return ""
    name = "".join(c for c in unicodedata.normalize("NFD", name)
                   if unicodedata.category(c) != "Mn")
    name = re.sub(r"[أإآٱ]", "ا", name)
    name = re.sub(r"[ؤ]", "و", name)
    name = re.sub(r"[ئ]", "ي", name)
    name = re.sub(r"ة", "ه", name)
    name = re.sub(r"ـ", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def tokenize(name: str) -> set:
    tokens = set(name.split())
    return tokens - TITLE_WORDS


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)

# ---------------------------------------------------------------------------
# Load tarajm
# ---------------------------------------------------------------------------

def load_tarajm(path: Path) -> list[dict]:
    print(f"Loading tarajm from {path}...")
    csv.field_size_limit(10_000_000)
    rows = []
    with path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            nm = (row.get("name") or "").strip()
            if not nm:
                continue
            sm = (row.get("summary") or "").strip()
            norm_nm = normalize(nm)
            rows.append({
                "id": int(row["id"]),
                "name": nm,
                "summary": sm,
                "_norm_name": norm_nm,
                "_tokens": tokenize(norm_nm),
                "_norm_summary": normalize(sm),
            })
    print(f"  Loaded {len(rows)} tarajm records")
    return rows

# ---------------------------------------------------------------------------
# Load podia
# ---------------------------------------------------------------------------

def load_podia(path: Path) -> list[dict]:
    records = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    print(f"  Loaded {len(records)} podia records")
    return records

# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def find_candidates(full_name: str, name_in_chain: str, tarajm: list[dict]) -> list[dict]:
    norm_full  = normalize(full_name)
    norm_chain = normalize(name_in_chain)
    tokens_full = tokenize(norm_full)

    # 1. Exact match on name
    exact = [
        t for t in tarajm
        if t["_norm_name"] == norm_full or t["_norm_name"] == norm_chain
    ]
    if exact:
        seen: set[int] = set()
        deduped_exact = []
        for t in exact:
            if t["id"] not in seen:
                seen.add(t["id"])
                deduped_exact.append(t)
        return [_candidate(t, "exact", 1.0) for t in deduped_exact[:TOP_N]]

    # 2. Summary substring (normalized full_name appears in summary)
    if norm_full and len(norm_full) > 5:
        seen: set[int] = set()
        sub_matches = []
        for t in tarajm:
            if norm_full in t["_norm_summary"] and t["id"] not in seen:
                seen.add(t["id"])
                sub_matches.append(t)
        if sub_matches:
            return [_candidate(t, "summary_match", 0.9) for t in sub_matches[:TOP_N]]

    # 3. Token Jaccard on full_name tokens vs tarajm name tokens
    if not tokens_full:
        return []

    scored = []
    for t in tarajm:
        score = jaccard(tokens_full, t["_tokens"])
        if score >= LOW_CONFIDENCE:
            scored.append((score, t))

    if not scored:
        return []

    scored.sort(key=lambda x: x[0], reverse=True)
    # Deduplicate by tarajm id, keeping highest score
    seen_ids: set[int] = set()
    deduped = []
    for score, t in scored:
        if t["id"] not in seen_ids:
            seen_ids.add(t["id"])
            deduped.append((score, t))
    top = deduped[:TOP_N]
    top_score = top[0][0]

    if top_score >= HIGH_CONFIDENCE:
        label = "ambiguous" if len(top) > 1 and (top_score - top[1][0]) < 0.05 else "high_confidence"
    else:
        label = "low_confidence"

    return [_candidate(t, label, round(score, 3)) for score, t in top]


def _candidate(t: dict, match_type: str, score: float) -> dict:
    return {
        "tarajm_id": t["id"],
        "tarajm_name": t["name"],
        "match_type": match_type,
        "score": score,
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    podia  = load_podia(PODIA_PATH)
    tarajm = load_tarajm(TARAJM_PATH)

    print("Matching podia narrators to tarajm...")

    matched = 0
    unmatched_records = []
    type_counts: dict[str, int] = {}

    with OUT_PATH.open("w", encoding="utf-8") as out_f:
        for record in podia:
            full_name    = record.get("full_name") or ""
            name_in_chain = record.get("name_in_chain") or ""

            candidates = find_candidates(full_name, name_in_chain, tarajm)
            record["tarajm_candidates"] = candidates
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

            if candidates:
                matched += 1
                top_type = candidates[0]["match_type"]
                type_counts[top_type] = type_counts.get(top_type, 0) + 1
            else:
                unmatched_records.append({
                    "rawi_id": record.get("rawi_id"),
                    "name_in_chain": name_in_chain,
                    "full_name": full_name,
                })

    with UNMATCHED_PATH.open("w", encoding="utf-8") as uf:
        for r in unmatched_records:
            uf.write(json.dumps(r, ensure_ascii=False) + "\n")

    total = len(podia)
    print(f"\nDone.")
    print(f"  Total narrators : {total}")
    print(f"  Matched         : {matched} ({matched/total*100:.1f}%)")
    print(f"  Unmatched       : {total - matched}")
    print(f"\n  Breakdown by top match type:")
    for label, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {label:<20} {count}")
    print(f"\nOutput:")
    print(f"  {OUT_PATH}")
    print(f"  {UNMATCHED_PATH}")


if __name__ == "__main__":
    main()
