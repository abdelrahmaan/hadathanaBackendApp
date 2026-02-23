"""
resolve_remaining_narrators.py

3-pass resolution of narrators that enrich_narrator_ids.py left as null:

Pass 1 — Shamela JSONL cross-reference
    shamela_book_1681.jsonl already has narrator IDs from shamela.ws.
    Strip tashkeel from every Shamela narrator name → build name→id lookup.
    If a stripped name maps to exactly one unique ID → assign it.

Pass 2 — Context mappings (resolved_context_mappings.json)
    Key: "narrator_name|student_name"  (student = next narrator in chain,
    i.e. the narrator closer to the collector who received from this one).
    Value: canonical full name.
    Canonical name → then look up in the Shamela or narrator_hadith_names lookup.

Pass 3 — Narrator name mappings fallback (narrator_mappings.json)
    Simple short-name → canonical-name dict.
    Only apply when the canonical name resolves to exactly one ID.

Each resolved narrator also gets a narrator_id_resolution tag:
    "exact_match"       — already resolved by Phase 1 (enrich_narrator_ids.py)
    "shamela_cross_ref" — resolved by Pass 1 of this script
    "context_mapping"   — resolved by Pass 2
    "name_mapping"      — resolved by Pass 3
    null                — still unresolved
"""

import json
import re
import unicodedata
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE = Path(__file__).parent
EXTRACT_V2 = BASE.parent

INPUT_FILE   = EXTRACT_V2 / "Bukhari" / "Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json"
OUTPUT_FILE  = INPUT_FILE   # overwrite in-place
SHAMELA_JSONL = BASE / "shamela_book_1681.jsonl"
CONTEXT_FILE  = EXTRACT_V2 / "Bukhari" / "resolved_context_mappings.json"
MAPPING_FILE  = EXTRACT_V2 / "narrator_mappings.json"
LOOKUP_FILE   = BASE / "narrator_hadith_names.json"

# ---------------------------------------------------------------------------
# Normalization (same logic as enrich_narrator_ids.py)
# ---------------------------------------------------------------------------

_TASHKEEL_RE = re.compile(
    "["
    "\u0610-\u061A"
    "\u064B-\u065F"
    "\u0670"
    "\u06D6-\u06DC"
    "\u06DF-\u06E4"
    "\u06E7-\u06E8"
    "\u06EA-\u06ED"
    "]"
)

_PARENS_RE = re.compile(r"\(.*?\)")
_PUNCT_RE = re.compile(r"[.,،؛;:؟?!\"'«»\-_/\\]")


def normalize(name: str) -> str:
    """
    Full Arabic name normalization:
      1. Strip tashkeel
      2. Remove parenthetical content like (١)
      3. Remove punctuation
      4. Unify hamza variants → ا / base letter
      5. Ta marbuta (ة) → ha (ه)
      6. Alef maqsura (ى) → ya (ي)
      7. Strip leading waw connector (و)
      8. Collapse whitespace
    """
    name = _TASHKEEL_RE.sub("", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = _PARENS_RE.sub("", name)
    name = _PUNCT_RE.sub("", name)
    name = name.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    name = name.replace("ئ", "ي").replace("ؤ", "و")
    name = name.replace("ة", "ه")
    name = name.replace("ى", "ي")
    name = re.sub(r"^و\s*", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


# ---------------------------------------------------------------------------
# Build lookups
# ---------------------------------------------------------------------------

def build_shamela_lookup(jsonl_path: Path) -> tuple[dict, set]:
    """
    Returns:
        lookup:        normalized_name -> narrator_id  (only when unambiguous)
        collision_set: normalized names mapping to 2+ distinct IDs
    """
    raw: dict[str, set] = {}  # name -> set of IDs seen

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("status") != "success":
                continue
            for block in obj.get("hadith_blocks", []):
                for narrator in block.get("narrators", []):
                    nid = str(narrator.get("id", "")).strip()
                    nname = narrator.get("name", "")
                    if not nid or not nname:
                        continue
                    norm = normalize(nname)
                    if not norm:
                        continue
                    raw.setdefault(norm, set()).add(nid)

    lookup: dict[str, str] = {}
    collision_set: set[str] = set()
    for norm, ids in raw.items():
        if len(ids) == 1:
            lookup[norm] = next(iter(ids))
        else:
            collision_set.add(norm)

    return lookup, collision_set


def build_shamela_matn_index(jsonl_path: Path) -> dict:
    """
    Build a matn → Shamela narrator list index.

    Returns:
        index: matn_key -> list of { narrator_ids: set[str], narrator_map: {id: name}, page: int }
        matn_key = first 100 chars of normalize(matn or full_text)

    Used by Pass 4 to match Bukhari hadiths to Shamela blocks by text content,
    so that the Shamela narrator IDs can fill in null entries in the Bukhari chains.
    """
    index: dict[str, list] = {}

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("status") != "success":
                continue
            for block in obj.get("hadith_blocks", []):
                text = block.get("matn") or block.get("full_text", "")
                key = normalize(text)[:100]
                if len(key) < 15:
                    continue  # skip trivially short / empty texts
                narrator_map = {
                    n["id"]: n["name"]
                    for n in block.get("narrators", [])
                    if n.get("id")
                }
                index.setdefault(key, []).append({
                    "narrator_ids": set(narrator_map),
                    "narrator_map": narrator_map,
                    "page": obj["page_number"],
                })

    return index


def build_narrator_name_lookup(lookup_path: Path) -> tuple[dict, set, dict]:
    """
    Build lookup from narrator_hadith_names.json (same as enrich script).

    Also returns collision_index: normalized_short_name -> {narrator_id -> [normalized variants]}
    Used to disambiguate collisions by finding the ID whose longer variant best
    matches a canonical name (e.g. 'عايشه بنت ابي بكر' disambiguates 3026 vs 3027).
    """
    with open(lookup_path, encoding="utf-8") as f:
        narrator_names = json.load(f)

    # Build per-ID variant lists: id -> set of normalized name forms
    id_to_norms: dict[str, set] = {}
    for narrator_id, names in narrator_names.items():
        norms = set()
        for raw_name in names:
            norm = normalize(raw_name)
            if norm:
                norms.add(norm)
        if norms:
            id_to_norms[narrator_id] = norms

    lookup: dict[str, str] = {}
    collision_set: set[str] = set()
    # norm -> set of IDs that own it
    norm_to_ids: dict[str, set] = {}

    for narrator_id, norms in id_to_norms.items():
        for norm in norms:
            norm_to_ids.setdefault(norm, set()).add(narrator_id)

    for norm, ids in norm_to_ids.items():
        if len(ids) == 1:
            lookup[norm] = next(iter(ids))
        else:
            collision_set.add(norm)

    # collision_index: short_norm -> {narrator_id -> sorted list of all normalized variants}
    # Only populated for names in collision_set
    collision_index: dict[str, dict] = {}
    for norm in collision_set:
        ids = norm_to_ids[norm]
        collision_index[norm] = {nid: sorted(id_to_norms[nid], key=len, reverse=True)
                                  for nid in ids}

    return lookup, collision_set, collision_index


def load_context_mappings(path: Path) -> dict:
    """Returns {key: canonical_name} where key is 'narrator|student'."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("context_mappings", {})


def load_name_mappings(path: Path) -> dict:
    """Returns {short_name: canonical_name}, skipping _comment_ keys."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    raw = data.get("mappings", {})
    return {k: v for k, v in raw.items() if not k.startswith("_comment")}


def lookup_canonical(canonical: str, shamela_lookup: dict, narrator_lookup: dict,
                     shamela_collision: set, narrator_collision: set,
                     narrator_collision_index: Optional[dict] = None) -> Optional[str]:
    """
    Try to resolve a canonical name to a unique ID.

    Strategy:
    1. Try the full normalized canonical name in both lookups.
    2. If not found, try progressively shorter prefixes (drop last word each time).
    3. When a prefix hits a collision in narrator_lookup, use collision_index to
       find the one ID whose longer name variant is a prefix of the canonical
       (while no other colliding ID has a conflicting longer variant that also
       fits). This handles عائشة بنت أبي بكر → 3026 vs 3027 (عائشة بنت سعد).
    """
    def _try(norm: str) -> Optional[str]:
        if norm in shamela_lookup and norm not in shamela_collision:
            return shamela_lookup[norm]
        if norm in narrator_lookup and norm not in narrator_collision:
            return narrator_lookup[norm]
        return None

    def _try_disambiguate(prefix: str, full_norm: str) -> Optional[str]:
        """
        When `prefix` is in narrator_collision_index, try to pick the unique
        ID that is consistent with full_norm.

        Two strategies:
        A) Positive fit: exactly one ID has a longer variant that IS a prefix
           of full_norm (e.g. 'عايشه بنت ابي' is a prefix of canonical).
        B) Elimination: one or more IDs have a longer variant that CONFLICTS
           with full_norm (starts with prefix but diverges). If only one ID
           has no conflicting longer variant, pick it.

        A conflicting variant v: v starts with prefix and len(v) > len(prefix)
           but full_norm does NOT start with v.
        """
        if narrator_collision_index is None:
            return None
        if prefix not in narrator_collision_index:
            return None
        candidates = narrator_collision_index[prefix]  # {id -> [norms sorted longest first]}

        # Strategy A: find IDs with a longer variant that is a prefix of full_norm
        positive_ids = []
        for nid, variants in candidates.items():
            if any(v != prefix and len(v) > len(prefix) and full_norm.startswith(v)
                   for v in variants):
                positive_ids.append(nid)
        if len(positive_ids) == 1:
            return positive_ids[0]

        # Strategy B: eliminate IDs that have a conflicting longer variant
        # A variant v conflicts if: starts with prefix, longer than prefix,
        # but full_norm does NOT start with v.
        non_conflicting_ids = []
        for nid, variants in candidates.items():
            has_conflict = any(
                v != prefix and len(v) > len(prefix)
                and v.startswith(prefix)
                and not full_norm.startswith(v)
                for v in variants
            )
            if not has_conflict:
                non_conflicting_ids.append(nid)
        if len(non_conflicting_ids) == 1:
            return non_conflicting_ids[0]

        return None

    norm = normalize(canonical)

    # Full match first
    result = _try(norm)
    if result:
        return result

    # Progressive prefix fallback (drop last word each iteration)
    parts = norm.split()
    for end in range(len(parts) - 1, 0, -1):
        prefix = " ".join(parts[:end])
        result = _try(prefix)
        if result:
            return result
        # If prefix is in collision, try to disambiguate using longer variants
        if prefix in narrator_collision:
            result = _try_disambiguate(prefix, norm)
            if result:
                return result

    return None


# ---------------------------------------------------------------------------
# 3-pass resolution
# ---------------------------------------------------------------------------

def resolve(hadiths: list,
            shamela_lookup: dict, shamela_collision: set,
            context_mappings: dict,
            name_mappings: dict,
            narrator_lookup: dict, narrator_collision: set,
            narrator_collision_index: Optional[dict] = None) -> dict:

    stats = {
        "already_matched": 0,
        "pass1_shamela": 0,
        "pass2_context": 0,
        "pass3_mapping": 0,
        "still_null": 0,
    }
    still_null_names: set[str] = set()

    for hadith in hadiths:
        for chain in hadith.get("chains", []):
            narrators = chain.get("narrators", [])
            for idx, narrator in enumerate(narrators):
                # Already resolved by Phase 1
                if narrator.get("narrator_id") is not None:
                    narrator["narrator_id_resolution"] = "exact_match"
                    stats["already_matched"] += 1
                    continue

                raw_name = narrator.get("name", "")
                norm_name = normalize(raw_name)

                # Determine student: the narrator at idx-1 (closer to collector)
                student_name = narrators[idx - 1]["name"] if idx > 0 else ""

                resolved_id = None
                resolution_tag = None

                # --- Pass 1: Shamela cross-reference ---
                if norm_name in shamela_lookup and norm_name not in shamela_collision:
                    resolved_id = shamela_lookup[norm_name]
                    resolution_tag = "shamela_cross_ref"

                # --- Pass 2: Context mappings ---
                if resolved_id is None and student_name:
                    ctx_key = f"{raw_name}|{student_name}"
                    if ctx_key in context_mappings:
                        canonical = context_mappings[ctx_key]
                        resolved_id = lookup_canonical(
                            canonical, shamela_lookup, narrator_lookup,
                            shamela_collision, narrator_collision,
                            narrator_collision_index
                        )
                        if resolved_id:
                            resolution_tag = "context_mapping"

                # --- Pass 3: Name mappings fallback ---
                if resolved_id is None and raw_name in name_mappings:
                    canonical = name_mappings[raw_name]
                    resolved_id = lookup_canonical(
                        canonical, shamela_lookup, narrator_lookup,
                        shamela_collision, narrator_collision,
                        narrator_collision_index
                    )
                    if resolved_id:
                        resolution_tag = "name_mapping"

                # Apply result
                if resolved_id:
                    narrator["narrator_id"] = resolved_id
                    narrator["narrator_id_resolution"] = resolution_tag
                    # Clear the ambiguous flag if it was set
                    narrator.pop("narrator_id_ambiguous", None)
                else:
                    narrator["narrator_id"] = None
                    narrator["narrator_id_resolution"] = None
                    still_null_names.add(raw_name)

    return stats, still_null_names


# ---------------------------------------------------------------------------
# Pass 4 — Matn-based chain matching
# ---------------------------------------------------------------------------

def resolve_pass4_matn(hadiths: list, shamela_matn_index: dict) -> tuple[int, set]:
    """
    Pass 4: for each hadith that still has null narrators, look up its matn
    in the Shamela matn index.  If exactly one Shamela block matches AND the
    number of extra Shamela narrator IDs equals the number of null narrator
    slots, assign those IDs positionally (same chain order in both sources).

    Returns (resolved_count, still_null_names_set).
    """
    resolved_count = 0
    still_null: set[str] = set()

    for hadith in hadiths:
        # Collect null narrators across all chains (in order)
        null_slots: list = []
        for chain in hadith.get("chains", []):
            for narrator in chain.get("narrators", []):
                if narrator.get("narrator_id") is None:
                    null_slots.append(narrator)

        if not null_slots:
            continue

        matn_segments = hadith.get("matn_segments", [])
        if not matn_segments:
            for n in null_slots:
                still_null.add(n.get("name", ""))
            continue

        matn_key = normalize(matn_segments[0])[:100]
        shamela_matches = shamela_matn_index.get(matn_key, [])

        # Only act on an unambiguous single match
        if len(shamela_matches) != 1:
            for n in null_slots:
                still_null.add(n.get("name", ""))
            continue

        match = shamela_matches[0]

        # IDs already resolved in this hadith (across all chains)
        resolved_ids = set()
        for chain in hadith.get("chains", []):
            for narrator in chain.get("narrators", []):
                if narrator.get("narrator_id") is not None:
                    resolved_ids.add(narrator["narrator_id"])

        # Extra IDs in Shamela = candidates for the null slots
        extra_ids = match["narrator_ids"] - resolved_ids

        if len(extra_ids) != len(null_slots):
            # Can't assign safely — counts don't match
            for n in null_slots:
                still_null.add(n.get("name", ""))
            continue

        # Assign positionally — Shamela and Bukhari chains share the same order
        for narrator, new_id in zip(null_slots, sorted(extra_ids)):
            narrator["narrator_id"] = new_id
            narrator["narrator_id_resolution"] = "matn_chain_match"
            narrator.pop("narrator_id_ambiguous", None)
            resolved_count += 1

    return resolved_count, still_null


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Loading Phase 1 output ...")
    with open(INPUT_FILE, encoding="utf-8") as f:
        hadiths = json.load(f)

    print("Building Shamela cross-reference lookup ...")
    shamela_lookup, shamela_collision = build_shamela_lookup(SHAMELA_JSONL)
    print(f"  Unique unambiguous name forms in Shamela : {len(shamela_lookup)}")
    print(f"  Ambiguous (collision) forms in Shamela   : {len(shamela_collision)}")

    print("Loading narrator_hadith_names lookup ...")
    narrator_lookup, narrator_collision, narrator_collision_index = build_narrator_name_lookup(LOOKUP_FILE)
    print(f"  Collision entries with disambiguation index: {len(narrator_collision_index)}")

    print("Loading context mappings ...")
    context_mappings = load_context_mappings(CONTEXT_FILE)
    print(f"  Context mapping rules : {len(context_mappings)}")

    print("Loading name mappings ...")
    name_mappings = load_name_mappings(MAPPING_FILE)
    print(f"  Name mapping rules    : {len(name_mappings)}")

    print("Resolving narrators (3 passes) ...")
    stats, still_null_names = resolve(
        hadiths,
        shamela_lookup, shamela_collision,
        context_mappings,
        name_mappings,
        narrator_lookup, narrator_collision,
        narrator_collision_index,
    )

    print("Building Shamela matn index for Pass 4 ...")
    shamela_matn_index = build_shamela_matn_index(SHAMELA_JSONL)
    print(f"  Unique matn keys in Shamela : {len(shamela_matn_index)}")

    print("Resolving narrators (Pass 4 — matn chain match) ...")
    pass4_count, pass4_still_null = resolve_pass4_matn(hadiths, shamela_matn_index)
    print(f"  Pass 4 resolved : {pass4_count}")

    # Recalculate totals from scratch for all passes
    totals = {"already_matched": 0, "shamela_cross_ref": 0,
              "context_mapping": 0, "name_mapping": 0,
              "matn_chain_match": 0, "still_null": 0}
    for hadith in hadiths:
        for chain in hadith.get("chains", []):
            for narrator in chain.get("narrators", []):
                tag = narrator.get("narrator_id_resolution")
                if tag == "exact_match":
                    totals["already_matched"] += 1
                elif tag == "shamela_cross_ref":
                    totals["shamela_cross_ref"] += 1
                elif tag == "context_mapping":
                    totals["context_mapping"] += 1
                elif tag == "name_mapping":
                    totals["name_mapping"] += 1
                elif tag == "matn_chain_match":
                    totals["matn_chain_match"] += 1
                else:
                    totals["still_null"] += 1

    total = sum(totals.values())

    print(f"\nWriting output to {OUTPUT_FILE.name} ...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(hadiths, f, ensure_ascii=False, indent=2)

    print("\n--- Resolution Summary ---")
    print(f"  Total narrator mentions   : {total}")
    print(f"  Phase 1 exact match       : {totals['already_matched']}  ({totals['already_matched']/total*100:.1f}%)")
    print(f"  Pass 1 Shamela cross-ref  : {totals['shamela_cross_ref']}  ({totals['shamela_cross_ref']/total*100:.1f}%)")
    print(f"  Pass 2 Context mapping    : {totals['context_mapping']}  ({totals['context_mapping']/total*100:.1f}%)")
    print(f"  Pass 3 Name mapping       : {totals['name_mapping']}  ({totals['name_mapping']/total*100:.1f}%)")
    print(f"  Pass 4 Matn chain match   : {totals['matn_chain_match']}  ({totals['matn_chain_match']/total*100:.1f}%)")
    resolved_total = total - totals["still_null"]
    print(f"  Total resolved            : {resolved_total}  ({resolved_total/total*100:.1f}%)")
    print(f"  Still null                : {totals['still_null']}  ({totals['still_null']/total*100:.1f}%)")

    # Merge still-null names from all passes
    all_still_null = pass4_still_null
    if all_still_null:
        print(f"\n--- Still Unresolved ({len(all_still_null)} unique names) ---")
        for name in sorted(all_still_null):
            print(f"  {name}")

    print("\nDone.")


if __name__ == "__main__":
    main()
