"""
Match unique Bukhari narrator names against tarajm_people.csv.

Pipeline:
  1. Extract all unique narrator names from Bukhari hadiths JSON.
  2. Normalize both sides (strip diacritics, normalize alef/hamza, remove titles).
  3. Exact match on normalized name.
  4. Token Jaccard scoring for the rest — each narrator gets top-N ranked candidates.
  5. Results marked: exact | high_confidence | low_confidence | ambiguous | not_found.
  6. Save matches and unmatched to CSV.

Usage:
    python3 match_bukhari_narrators.py
"""

import json
import re
import unicodedata
import csv

BUKHARI_PATH = "../extract_data_v2/Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn.json"
TARAJM_CSV = "out_people_csv/tarajm_people.csv"
TARAJM_PARQUET = "out_people_csv/tarajm_people.parquet"
OUTPUT_PATH = "out_people_csv/bukhari_narrators_tarajm_matches.csv"
UNMATCHED_PATH = "out_people_csv/bukhari_narrators_tarajm_matches_unmatched.csv"

# Jaccard score thresholds
HIGH_CONFIDENCE = 0.5   # clear match
LOW_CONFIDENCE  = 0.25  # possible match, needs review
TOP_N = 3               # max candidates to keep per narrator

# Titles/prefixes to strip before comparing
TITLE_WORDS = {
    "الإمام", "الشيخ", "الحافظ", "الحجة", "الثقة", "القاضي", "الفقيه",
    "الأمير", "السيد", "سيدي", "مولانا", "الشريف", "العلامة", "الحاج",
    "أبو", "أبي", "ابن", "بن", "بنت", "ابنة", "آل",
}


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def normalize(name: str) -> str:
    """Normalize an Arabic name for comparison."""
    if not name:
        return ""
    # Remove diacritics (tashkeel)
    name = "".join(c for c in unicodedata.normalize("NFD", name)
                   if unicodedata.category(c) != "Mn")
    # Normalize alef variants → ا
    name = re.sub(r"[أإآٱ]", "ا", name)
    # Normalize hamza on waw/ya
    name = re.sub(r"[ؤ]", "و", name)
    name = re.sub(r"[ئ]", "ي", name)
    # Normalize teh marbuta → ه
    name = re.sub(r"ة", "ه", name)
    # Remove tatweel
    name = re.sub(r"ـ", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def tokenize(name: str) -> set:
    """Split normalized name into word tokens, removing title words."""
    tokens = set(name.split())
    return tokens - TITLE_WORDS


# ---------------------------------------------------------------------------
# Jaccard similarity
# ---------------------------------------------------------------------------

def jaccard(tokens_a: set, tokens_b: set) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    inter = len(tokens_a & tokens_b)
    union = len(tokens_a | tokens_b)
    return inter / union


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_tarajm() -> list[dict]:
    """Load tarajm_people into a list of dicts using stdlib (no pandas needed)."""
    path = TARAJM_CSV
    print(f"Loading tarajm_people from {path}...")
    csv.field_size_limit(10_000_000)
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("name") or "").strip()
            if not name:
                continue
            rows.append({
                "id": row["id"],
                "name": name,
                "url": row.get("url", ""),
                "summary": row.get("summary", ""),
                "translation": row.get("translation", ""),
                "fields_json": row.get("fields_json", ""),
                # pre-compute normalized tokens once
                "_norm": normalize(name),
                "_tokens": tokenize(normalize(name)),
            })
    print(f"  Loaded {len(rows)} rows with a name.")
    return rows


def extract_unique_narrators(bukhari: list) -> list[str]:
    names = set()
    for hadith in bukhari:
        for chain in hadith.get("chains", []):
            for narrator in chain.get("narrators", []):
                name = narrator.get("name", "").strip()
                if name:
                    names.add(name)
    return sorted(names)


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def match_narrators(narrators: list[str], tarajm: list[dict]) -> tuple[list, list]:
    """
    For each narrator:
      - Try exact match on normalized name first.
      - Otherwise score all tarajm entries by Jaccard and keep top-N above threshold.
      - Tag each result with a match_type.
    Returns (matched_rows, unmatched_names).
    """
    results = []
    unmatched = []

    for narrator_name in narrators:
        norm_q = normalize(narrator_name)
        tokens_q = tokenize(norm_q)

        # 1. Exact match (normalized)
        exact = [t for t in tarajm if t["_norm"] == norm_q]
        if exact:
            for t in exact:
                results.append(_build_row(narrator_name, t, "exact", score=1.0, rank=1))
            continue

        # 2. Jaccard scoring
        scored = []
        for t in tarajm:
            score = jaccard(tokens_q, t["_tokens"])
            if score >= LOW_CONFIDENCE:
                scored.append((score, t))

        if not scored:
            unmatched.append(narrator_name)
            continue

        scored.sort(key=lambda x: x[0], reverse=True)
        top = scored[:TOP_N]
        top_score = top[0][0]

        # Determine confidence label
        if top_score >= HIGH_CONFIDENCE:
            # If top two scores are very close, mark ambiguous
            if len(top) > 1 and (top_score - top[1][0]) < 0.05:
                label = "ambiguous"
            else:
                label = "high_confidence"
        else:
            label = "low_confidence"

        for rank, (score, t) in enumerate(top, start=1):
            results.append(_build_row(narrator_name, t, label, score=round(score, 3), rank=rank))

    return results, unmatched


def _build_row(narrator_name: str, t: dict, match_type: str, score: float, rank: int) -> dict:
    return {
        "narrator_name": narrator_name,
        "tarajm_id": t["id"],
        "tarajm_name": t["name"],
        "url": t["url"],
        "summary": t["summary"],
        "translation": t["translation"],
        "fields_json": t["fields_json"],
        "match_type": match_type,
        "score": score,
        "rank": rank,
    }


# ---------------------------------------------------------------------------
# CSV output (stdlib, no pandas)
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict], path: str):
    if not rows:
        print(f"  (no rows to write to {path})")
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # 1. Load Bukhari
    print(f"Loading Bukhari from {BUKHARI_PATH}...")
    with open(BUKHARI_PATH, encoding="utf-8") as f:
        bukhari = json.load(f)

    unique_narrators = extract_unique_narrators(bukhari)
    print(f"Unique narrator names: {len(unique_narrators)}")

    # 2. Load tarajm
    tarajm = load_tarajm()

    # 3. Match
    print("Matching...")
    results, unmatched = match_narrators(unique_narrators, tarajm)

    # 4. Stats
    by_type: dict[str, set] = {}
    for r in results:
        by_type.setdefault(r["match_type"], set()).add(r["narrator_name"])

    print(f"\n{'Match type':<20} {'Narrators':>10}")
    print("-" * 32)
    for label, names in sorted(by_type.items()):
        print(f"  {label:<18} {len(names):>10}")
    print(f"  {'not_found':<18} {len(unmatched):>10}")
    print(f"  {'TOTAL':<18} {len(unique_narrators):>10}")
    print(f"\nTotal result rows: {len(results)}")

    # 5. Save
    write_csv(results, OUTPUT_PATH)
    print(f"Saved matches  -> {OUTPUT_PATH}")

    unmatched_rows = [{"narrator_name": n} for n in unmatched]
    write_csv(unmatched_rows, UNMATCHED_PATH)
    print(f"Saved unmatched -> {UNMATCHED_PATH}")


if __name__ == "__main__":
    main()
