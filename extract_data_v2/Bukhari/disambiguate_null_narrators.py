"""
disambiguate_null_narrators.py
==============================
Resolve ambiguous narrator names (e.g. "عبد الله", "علي") that still have
final_narrator_id = null, using chain-neighbor context.

Strategy (2 passes):
--------------------
Pass 1 — Intra-hadith sibling chain lookup
  For each null narrator N at position i in chain C of hadith H:
    - Get prev_id and next_id (IDs of neighbors in the chain)
    - At least ONE neighbor must be resolved (not None) — skip (None,None)
    - Look at all OTHER chains in the same hadith
    - Find a resolved narrator with the SAME neighbor context
    - Name check: the resolved name must textually relate to the short name
    - If exactly 1 unique candidate ID found → assign it

Pass 2 — Global neighbor+name lookup
  Build a global lookup from ALL resolved narrators across all hadiths:
    key = (neighbor_id, exact_name, side="prev"|"next")
    value = set of IDs seen with this combo
  For each still-null narrator:
    - Require BOTH prev and next lookups to agree (intersection), OR
    - If only one side available, require exactly 1 candidate
  Single-word names (e.g. "علي") require BOTH sides to match — stricter.

Output: Bukhari_final_with_narrator_ids_v2.json
"""

import json
import copy
from pathlib import Path
from collections import defaultdict

BASE        = Path(__file__).parent
INPUT_PATH  = BASE / "Bukhari_final_with_narrator_ids.json"
OUTPUT_PATH = BASE / "Bukhari_final_with_narrator_ids_v2.json"

print("Loading …")
with open(INPUT_PATH, encoding="utf-8") as f:
    hadiths = json.load(f)

# Deep copy so pass 1 mutations don't contaminate pass 2's index building
hadiths_for_index = copy.deepcopy(hadiths)


# ── helper ────────────────────────────────────────────────────────────────────
def names_related(short: str, long: str) -> bool:
    """Check if two names plausibly refer to the same person.
    e.g. "عبد الله" ↔ "عبد الله بن عمر" → True
         "علي"      ↔ "علي بن أبي طالب" → True
         "عبد الله" ↔ "ابن عمر"          → False
    """
    if short == long:
        return True
    # long starts with short (short is prefix of full name)
    if long.startswith(short + " "):
        return True
    # long ends with short (kunya/laqab prefix case)
    if long.endswith(" " + short):
        return True
    # short starts with long (long is a shorter alias)
    if short.startswith(long + " "):
        return True
    # short ends with long
    if short.endswith(" " + long):
        return True
    return False


def is_single_word(name: str) -> bool:
    return " " not in name.strip()


# ── pass 1: intra-hadith sibling-chain lookup ─────────────────────────────────
print("Pass 1: intra-hadith sibling chain lookup …")

pass1_resolved = 0

for h in hadiths:
    # Build context_map for this hadith from resolved narrators:
    # (prev_id, next_id) → list of (resolved_id, resolved_name)
    context_map: dict[tuple, list[tuple]] = defaultdict(list)

    for chain in h.get("chains", []):
        narrators = chain.get("narrators", [])
        for i, n in enumerate(narrators):
            rid = n.get("final_narrator_id")
            if rid is None:
                continue
            prev_id = narrators[i-1].get("final_narrator_id") if i > 0 else None
            next_id = narrators[i+1].get("final_narrator_id") if i < len(narrators)-1 else None
            context_map[(prev_id, next_id)].append((rid, n["name"]))

    # Resolve nulls
    for chain in h.get("chains", []):
        narrators = chain.get("narrators", [])
        for i, n in enumerate(narrators):
            if n.get("final_narrator_id") is not None:
                continue

            short = n["name"]
            prev_id = narrators[i-1].get("final_narrator_id") if i > 0 else None
            next_id = narrators[i+1].get("final_narrator_id") if i < len(narrators)-1 else None

            # FIX: skip (None, None) — no neighbor context = unreliable
            if prev_id is None and next_id is None:
                continue

            candidates = []

            # Try exact context (prev_id, next_id)
            for (rid, rname) in context_map.get((prev_id, next_id), []):
                if names_related(short, rname):
                    candidates.append(rid)

            # Fallback: try partial context, but only if we have a real neighbor
            if not candidates and prev_id is not None:
                for (rid, rname) in context_map.get((prev_id, None), []):
                    if names_related(short, rname):
                        candidates.append(rid)

            if not candidates and next_id is not None:
                for (rid, rname) in context_map.get((None, next_id), []):
                    if names_related(short, rname):
                        candidates.append(rid)

            unique_candidates = list(dict.fromkeys(candidates))
            if len(unique_candidates) == 1:
                n["final_narrator_id"]        = unique_candidates[0]
                n["final_narrator_id_source"] = "neighbor_disambiguation"
                pass1_resolved += 1

print(f"  Pass 1 resolved: {pass1_resolved}")


# ── pass 2: global neighbor+name lookup ───────────────────────────────────────
print("Pass 2: global neighbor-name lookup …")

# Build the global map from the ORIGINAL data (before pass-1 mutations)
# so pass-1 errors don't cascade into pass 2.
global_map: dict[tuple, set] = defaultdict(set)

for h in hadiths_for_index:
    for chain in h.get("chains", []):
        narrators = chain.get("narrators", [])
        for i, n in enumerate(narrators):
            rid = n.get("final_narrator_id")
            if rid is None:
                continue
            rname = n["name"]
            prev_id = narrators[i-1].get("final_narrator_id") if i > 0 else None
            next_id = narrators[i+1].get("final_narrator_id") if i < len(narrators)-1 else None
            if prev_id is not None:
                global_map[(prev_id, rname, "prev")].add(rid)
            if next_id is not None:
                global_map[(next_id, rname, "next")].add(rid)

pass2_resolved = 0

for h in hadiths:
    for chain in h.get("chains", []):
        narrators = chain.get("narrators", [])
        for i, n in enumerate(narrators):
            if n.get("final_narrator_id") is not None:
                continue

            short = n["name"]
            prev_id = narrators[i-1].get("final_narrator_id") if i > 0 else None
            next_id = narrators[i+1].get("final_narrator_id") if i < len(narrators)-1 else None

            prev_candidates = None
            next_candidates = None

            if prev_id is not None:
                ids = global_map.get((prev_id, short, "prev"), set())
                if ids:
                    prev_candidates = ids

            if next_id is not None:
                ids = global_map.get((next_id, short, "next"), set())
                if ids:
                    next_candidates = ids

            # Single-word names (e.g. "علي", "موسى") — require BOTH sides
            if is_single_word(short):
                if prev_candidates is not None and next_candidates is not None:
                    candidates = prev_candidates & next_candidates
                else:
                    candidates = set()
            else:
                # Multi-word names — intersect if both available, else use one side
                if prev_candidates is not None and next_candidates is not None:
                    candidates = prev_candidates & next_candidates
                elif prev_candidates is not None:
                    candidates = prev_candidates
                elif next_candidates is not None:
                    candidates = next_candidates
                else:
                    candidates = set()

            if len(candidates) == 1:
                n["final_narrator_id"]        = candidates.pop()
                n["final_narrator_id_source"] = "neighbor_disambiguation"
                pass2_resolved += 1

print(f"  Pass 2 resolved: {pass2_resolved}")


# ── summary ───────────────────────────────────────────────────────────────────
total = 0
null_count = 0
source_counts: dict[str, int] = defaultdict(int)

for h in hadiths:
    for chain in h.get("chains", []):
        for n in chain.get("narrators", []):
            total += 1
            src = n.get("final_narrator_id_source")
            if src is None:
                null_count += 1
            else:
                source_counts[src] += 1

resolved = total - null_count
print(f"\n{'─'*54}")
print(f"Total narrators          : {total}")
for src, cnt in sorted(source_counts.items(), key=lambda x: -x[1]):
    print(f"  {src:<30s}: {cnt:>6}  ({cnt/total*100:.1f}%)")
print(f"  {'still null':<30s}: {null_count:>6}  ({null_count/total*100:.1f}%)")
print(f"  {'TOTAL RESOLVED':<30s}: {resolved:>6}  ({resolved/total*100:.1f}%)")
print(f"  newly resolved (pass1) : {pass1_resolved}")
print(f"  newly resolved (pass2) : {pass2_resolved}")
print(f"{'─'*54}")


# ── write ──────────────────────────────────────────────────────────────────────
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(hadiths, f, ensure_ascii=False, indent=2)

print(f"\nOutput written to: {OUTPUT_PATH}")
