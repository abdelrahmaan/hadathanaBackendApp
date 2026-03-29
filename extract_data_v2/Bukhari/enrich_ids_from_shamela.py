"""
enrich_ids_from_shamela.py
==========================
Goal: For every Bukhari hadith, match it to its Shamela counterpart (using the
already-computed match_results.json), then copy narrator IDs from Shamela into
the Bukhari narrator objects.

Pipeline
--------
Step 1 — Load match_results.json (text-based matches from match_bukhari_to_shamela.py)
Step 2 — Build a Shamela name→id lookup from ALL narrator appearances across
          all matched blocks (context-scoped: per matched hadith, not global)
Step 3 — For each Bukhari hadith that has a unique match:
    a. Collect all Shamela narrators for that hadith (from matched block(s))
    b. Build a local name→id map for this hadith's narrators only
    c. For each Bukhari narrator (in every chain):
         - Try exact normalized name match
         - Try suffix match  (Shamela adds laqab prefix: "الحميدي عبد الله" ↔ "عبد الله")
         - Try prefix match  (Bukhari has long name; Shamela has shorter form)
       If match found and unambiguous → assign id + resolution method
Step 4 — Narrator overlap validation:
    Compute what % of Bukhari narrators per hadith got an id from Shamela.
    Flag hadiths where overlap is < OVERLAP_WARNING_THRESHOLD.
Step 5 — Write enriched output JSON

Output: Bukhari_enriched_with_shamela_ids.json
  Same structure as Bukhari_Without_Tashkel_results_advanced_with_matn.json
  Each narrator object gains:
    "shamela_id":             "3654"   (or null)
    "shamela_id_resolution":  "exact" | "suffix" | "prefix" | null
    "shamela_match_method":   the text-match method used (matn_exact, substring, …)
    "shamela_hadith_number":  the Shamela hadith number (integer or null)
"""

import json
import re
import unicodedata
from pathlib import Path
from collections import defaultdict

# ── paths ─────────────────────────────────────────────────────────────────────
BASE          = Path(__file__).parent
BUKHARI_PATH  = BASE / "Bukhari_Without_Tashkel_results_advanced_with_matn.json"
MATCHES_PATH  = BASE / "match_results.json"   # produced by match_bukhari_to_shamela.py
OUTPUT_PATH   = BASE / "Bukhari_enriched_with_shamela_ids.json"

# hadiths where Bukhari narrator overlap with Shamela is below this fraction
# will be flagged with  "shamela_overlap_warning": true
OVERLAP_WARNING_THRESHOLD = 0.5


# ── normalization (identical to match script) ─────────────────────────────────
_TASHKEEL_RE = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)
_SYMBOLS_RE  = re.compile(r"[«»\(\)\[\]٠-٩0-9﵁ﷺ﴿﴾\u06DD\uFDFA\uFDFB]+")
_LIGATURE_RE = re.compile(r"[\u2997\u2998\u2026\u002A\u2013\uFD40-\uFD4F\uFDFF\uFDFD]+")
_PUNCT_RE    = re.compile(r"[.,،؛;:?!\-_/\\\"'`«»]+")

def normalize(text: str) -> str:
    if not text:
        return ""
    text = _TASHKEEL_RE.sub("", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = _SYMBOLS_RE.sub("", text)
    text = _LIGATURE_RE.sub("", text)
    text = _PUNCT_RE.sub("", text)
    text = text.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    text = text.replace("ئ", "ي").replace("ؤ", "و")
    text = text.replace("ة", "ه")
    text = text.replace("ى", "ي")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ── name matching helpers ─────────────────────────────────────────────────────

def try_match_name(bukhari_norm: str, sham_map: dict[str, list[str]]) -> tuple[str | None, str | None]:
    """
    Try to find a Shamela ID for a normalized Bukhari narrator name.

    sham_map: normalized_shamela_name → [id, ...]   (built per hadith)

    Returns (id, resolution_method) or (None, None).

    Resolution methods (in priority order):
      "exact"  — normalize(bukhari) == normalize(shamela)
      "suffix" — normalize(bukhari) is a suffix of normalize(shamela)
                 e.g. "عبد الله بن الزبير" matches "الحميدي عبد الله بن الزبير"
      "prefix" — normalize(bukhari) starts with normalize(shamela)
                 e.g. "سفيان بن عيينة الهلالي" matches "سفيان"  [only if Shamela is >= 2 words]
    """
    # 1. Exact
    if bukhari_norm in sham_map:
        ids = sham_map[bukhari_norm]
        if len(ids) == 1:
            return ids[0], "exact"
        return None, None  # ambiguous exact collision

    # 2. Suffix — bukhari_norm is a trailing portion of a shamela name
    #    (Shamela prepends a laqab/kunya)
    candidates = []
    for sname, ids in sham_map.items():
        if sname.endswith(bukhari_norm) and len(sname) > len(bukhari_norm):
            # make sure it's a word boundary (space before the match start)
            prefix_part = sname[: len(sname) - len(bukhari_norm)]
            if prefix_part.endswith(" "):
                candidates.extend(ids)
    if candidates:
        unique_ids = list(dict.fromkeys(candidates))   # preserve order, deduplicate
        if len(unique_ids) == 1:
            return unique_ids[0], "suffix"

    # 3. Prefix — shamela name is a short form contained at start of bukhari name
    #    only when shamela name is at least 2 words (avoid matching "علي" → anything)
    candidates = []
    for sname, ids in sham_map.items():
        if len(sname.split()) >= 2 and bukhari_norm.startswith(sname):
            remainder = bukhari_norm[len(sname):]
            if remainder == "" or remainder.startswith(" "):
                candidates.extend(ids)
    if candidates:
        unique_ids = list(dict.fromkeys(candidates))
        if len(unique_ids) == 1:
            return unique_ids[0], "prefix"

    return None, None


def build_sham_map(shamela_narrators: list[dict]) -> dict[str, list[str]]:
    """
    Build  normalized_name → [id, ...]  from a list of Shamela narrator dicts.
    Multiple entries for the same norm = collision within this hadith's block.
    """
    result: dict[str, list[str]] = defaultdict(list)
    for n in shamela_narrators:
        norm = normalize(n["name"])
        nid  = n["id"]
        if nid not in result[norm]:
            result[norm].append(nid)
    return dict(result)


# ── load data ─────────────────────────────────────────────────────────────────
print("Loading Bukhari …")
with open(BUKHARI_PATH, encoding="utf-8") as f:
    bukhari_hadiths = json.load(f)

print("Loading match results …")
with open(MATCHES_PATH, encoding="utf-8") as f:
    match_results = json.load(f)

# index match results by hadith_index
match_by_idx = {r["hadith_index"]: r for r in match_results}


# ── enrich ────────────────────────────────────────────────────────────────────
print("Enriching narrator IDs …")

stats = {
    "hadiths_total":          len(bukhari_hadiths),
    "hadiths_with_match":     0,
    "hadiths_no_match":       0,
    "hadiths_flagged":        0,
    "narrators_total":        0,
    "narrators_exact":        0,
    "narrators_suffix":       0,
    "narrators_prefix":       0,
    "narrators_no_id":        0,
}

enriched_hadiths = []

for hadith in bukhari_hadiths:
    hid   = hadith["hadith_index"]
    match = match_by_idx.get(hid, {})

    match_method         = match.get("match_method")
    shamela_match_count  = match.get("match_count", 0)
    shamela_matches      = match.get("shamela_matches", [])

    # ── collect all Shamela narrators for this hadith (from all matched blocks) ──
    # Each matched block may belong to a different isnāds split in Shamela.
    # Merge them all — gives us every narrator ID available for this hadith.
    all_shamela_narrators: list[dict] = []
    shamela_hadith_numbers: list[int] = []
    for sm in shamela_matches:
        all_shamela_narrators.extend(sm.get("narrators", []))
        n = sm.get("shamela_hadith_number")
        if n is not None:
            shamela_hadith_numbers.append(n)

    sham_map = build_sham_map(all_shamela_narrators)

    # ── enrich each chain ────────────────────────────────────────────────────
    total_in_hadith    = 0
    resolved_in_hadith = 0

    enriched_chains = []
    for chain in hadith.get("chains", []):
        enriched_narrators = []
        for narrator in chain.get("narrators", []):
            total_in_hadith += 1
            stats["narrators_total"] += 1

            buk_norm = normalize(narrator["name"])
            shamela_id, resolution = try_match_name(buk_norm, sham_map)

            enriched_narrator = {
                **narrator,
                "shamela_id":            shamela_id,
                "shamela_id_resolution": resolution,
            }

            if shamela_id:
                resolved_in_hadith += 1
                stats[f"narrators_{resolution}"] += 1
            else:
                stats["narrators_no_id"] += 1

            enriched_narrators.append(enriched_narrator)

        enriched_chains.append({**chain, "narrators": enriched_narrators})

    # ── overlap ratio ────────────────────────────────────────────────────────
    overlap_ratio = resolved_in_hadith / total_in_hadith if total_in_hadith else 0
    flagged       = shamela_match_count > 0 and overlap_ratio < OVERLAP_WARNING_THRESHOLD

    if shamela_match_count > 0:
        stats["hadiths_with_match"] += 1
    else:
        stats["hadiths_no_match"] += 1

    if flagged:
        stats["hadiths_flagged"] += 1

    enriched_hadith = {
        **hadith,
        "chains": enriched_chains,
        # ── Shamela match metadata ──
        "shamela_match_method":    match_method,
        "shamela_match_count":     shamela_match_count,
        "shamela_hadith_numbers":  shamela_hadith_numbers,
        "shamela_narrator_overlap": round(overlap_ratio, 3),
        "shamela_overlap_warning": flagged,
    }
    enriched_hadiths.append(enriched_hadith)


# ── summary ───────────────────────────────────────────────────────────────────
total_n = stats["narrators_total"]
total_h = stats["hadiths_total"]

print(f"\n{'─'*54}")
print(f"Hadiths total              : {total_h}")
print(f"  with Shamela match       : {stats['hadiths_with_match']:>5}  ({stats['hadiths_with_match']/total_h*100:.1f}%)")
print(f"  no Shamela match         : {stats['hadiths_no_match']:>5}  ({stats['hadiths_no_match']/total_h*100:.1f}%)")
print(f"  overlap warning (<50%)   : {stats['hadiths_flagged']:>5}")
print()
print(f"Narrators total            : {total_n}")
print(f"  id via exact match       : {stats['narrators_exact']:>5}  ({stats['narrators_exact']/total_n*100:.1f}%)")
print(f"  id via suffix match      : {stats['narrators_suffix']:>5}  ({stats['narrators_suffix']/total_n*100:.1f}%)")
print(f"  id via prefix match      : {stats['narrators_prefix']:>5}  ({stats['narrators_prefix']/total_n*100:.1f}%)")
print(f"  no id (not in Shamela)   : {stats['narrators_no_id']:>5}  ({stats['narrators_no_id']/total_n*100:.1f}%)")
resolved = total_n - stats["narrators_no_id"]
print(f"  TOTAL with shamela_id    : {resolved:>5}  ({resolved/total_n*100:.1f}%)")
print(f"{'─'*54}")


# ── write output ──────────────────────────────────────────────────────────────
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(enriched_hadiths, f, ensure_ascii=False, indent=2)

print(f"\nOutput written to: {OUTPUT_PATH}")
