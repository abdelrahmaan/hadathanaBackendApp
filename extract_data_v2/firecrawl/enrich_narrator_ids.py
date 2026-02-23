"""
Enrich Bukhari hadith JSON with narrator IDs from narrator_hadith_names.json.

For each narrator in the Bukhari chains, looks up the narrator ID by exact name match
after normalizing (stripping tashkeel and leading waw connector) all names in the
narrator lookup table.

Output: a copy of the input file with narrator_id added to each narrator object.
"""

import json
import copy
import unicodedata
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE = Path(__file__).parent

FILE1 = BASE.parent / "Bukhari" / "Bukhari_Without_Tashkel_results_advanced_with_matn.json"
FILE2 = BASE / "narrator_hadith_names.json"
OUTPUT = BASE.parent / "Bukhari" / "Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json"


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

# Arabic tashkeel (diacritics) Unicode ranges
_TASHKEEL_RE = re.compile(
    "["
    "\u0610-\u061A"   # Arabic extended
    "\u064B-\u065F"   # Fathatan … Shadda, Sukun, etc.
    "\u0670"          # Superscript alef
    "\u06D6-\u06DC"   # Arabic small high ligatures
    "\u06DF-\u06E4"   # Arabic extended marks
    "\u06E7-\u06E8"   # Arabic small high yeh / noon
    "\u06EA-\u06ED"   # Arabic extended low marks
    "]"
)

# Parenthetical content: (١) (2) (text) — strip entirely
_PARENS_RE = re.compile(r"\(.*?\)")

# Punctuation to remove (Arabic and Latin)
_PUNCT_RE = re.compile(r"[.,،؛;:؟?!\"'«»\-_/\\]")


def normalize(name: str) -> str:
    """
    Full Arabic name normalization:
      1. Strip tashkeel (diacritics)
      2. Remove parenthetical content like (١) or (text)
      3. Remove punctuation
      4. Unify hamza variants (أ إ آ ء ئ ؤ) → ا / base letter
      5. Ta marbuta (ة) → ha (ه)
      6. Alef maqsura (ى) → ya (ي)
      7. Strip leading waw connector (و)
      8. Collapse whitespace
    """
    # 1. Strip tashkeel
    name = _TASHKEEL_RE.sub("", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    # 2. Remove parenthetical content
    name = _PARENS_RE.sub("", name)
    # 3. Remove punctuation
    name = _PUNCT_RE.sub("", name)
    # 4. Unify hamza variants → bare alef
    name = name.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    name = name.replace("ئ", "ي").replace("ؤ", "و")
    # ء (standalone hamza) stays — removing it would mangle words like مسألة
    # 5. Ta marbuta → ha
    name = name.replace("ة", "ه")
    # 6. Alef maqsura → ya
    name = name.replace("ى", "ي")
    # 7. Strip leading waw connector
    name = re.sub(r"^و\s*", "", name)
    # 8. Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


# ---------------------------------------------------------------------------
# Build lookup from File 2
# ---------------------------------------------------------------------------

def build_lookup(narrator_names: dict) -> tuple[dict, set]:
    """
    Returns:
        lookup: normalized_name -> narrator_id (str)
        collision_set: normalized names that map to 2+ distinct IDs
    """
    lookup: dict[str, str] = {}
    collision_set: set[str] = set()

    for narrator_id, names in narrator_names.items():
        for raw_name in names:
            norm = normalize(raw_name)
            if not norm:
                continue
            if norm in lookup:
                if lookup[norm] != narrator_id:
                    collision_set.add(norm)
            else:
                lookup[norm] = narrator_id

    return lookup, collision_set


# ---------------------------------------------------------------------------
# Enrich File 1
# ---------------------------------------------------------------------------

def enrich(hadiths: list, lookup: dict, collision_set: set) -> tuple[list, dict]:
    enriched = copy.deepcopy(hadiths)

    total = 0
    matched = 0
    ambiguous = 0
    unmatched_names: set[str] = set()

    for hadith in enriched:
        for chain in hadith.get("chains", []):
            for narrator in chain.get("narrators", []):
                total += 1
                raw = narrator.get("name", "")
                norm = normalize(raw)

                if norm in collision_set:
                    narrator["narrator_id"] = None
                    narrator["narrator_id_ambiguous"] = True
                    ambiguous += 1
                elif norm in lookup:
                    narrator["narrator_id"] = lookup[norm]
                    matched += 1
                else:
                    narrator["narrator_id"] = None
                    unmatched_names.add(raw)

    stats = {
        "total": total,
        "matched": matched,
        "ambiguous": ambiguous,
        "unmatched": total - matched - ambiguous,
        "unmatched_names": sorted(unmatched_names),
    }
    return enriched, stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Loading {FILE1.name} ...")
    with open(FILE1, encoding="utf-8") as f:
        hadiths = json.load(f)

    print(f"Loading {FILE2.name} ...")
    with open(FILE2, encoding="utf-8") as f:
        narrator_names = json.load(f)

    print("Building lookup table ...")
    lookup, collision_set = build_lookup(narrator_names)
    print(f"  Unique normalized name forms : {len(lookup)}")
    print(f"  Ambiguous (collision) forms  : {len(collision_set)}")

    print("Enriching hadiths ...")
    enriched, stats = enrich(hadiths, lookup, collision_set)

    print(f"\nWriting output to {OUTPUT.name} ...")
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    print("\n--- Match Statistics ---")
    print(f"  Total narrator mentions : {stats['total']}")
    print(f"  Matched                 : {stats['matched']}  ({stats['matched']/stats['total']*100:.1f}%)")
    print(f"  Ambiguous (multi-ID)    : {stats['ambiguous']}  ({stats['ambiguous']/stats['total']*100:.1f}%)")
    print(f"  Unmatched               : {stats['unmatched']}  ({stats['unmatched']/stats['total']*100:.1f}%)")

    if stats["unmatched_names"]:
        print(f"\n--- Unmatched narrator names ({len(stats['unmatched_names'])} unique) ---")
        for name in stats["unmatched_names"]:
            print(f"  {name}")

    if collision_set:
        print(f"\n--- Ambiguous name forms ({len(collision_set)}) ---")
        for name in sorted(collision_set):
            print(f"  {name}")

    print("\nDone.")


if __name__ == "__main__":
    main()
