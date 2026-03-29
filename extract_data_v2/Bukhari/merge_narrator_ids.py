"""
merge_narrator_ids.py
=====================
Merge two narrator-ID pipelines into one final file.

Sources
-------
A) _with_ids.json  (narrator_id field)
   — from the existing pipeline: exact name match + context mapping + matn chain
   — 94% resolved, most reliable source

B) Bukhari_enriched_with_shamela_ids.json  (shamela_id field)
   — from the new Shamela text-match pipeline
   — fills nulls left by pipeline A

Priority rule (per narrator)
-----------------------------
  1. If narrator_id (A) is not null  → keep it as final_narrator_id
  2. Else if shamela_id (B) is not null → use it as final_narrator_id
  3. Else → final_narrator_id = null

Output: Bukhari_final_with_narrator_ids.json
  Every narrator gets:
    "final_narrator_id"         — the resolved ID (or null)
    "final_narrator_id_source"  — "pipeline_a" | "shamela_enrichment" | null
"""

import json
from pathlib import Path

BASE         = Path(__file__).parent
WITH_IDS     = BASE / "Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json"
ENRICHED     = BASE / "Bukhari_enriched_with_shamela_ids.json"
OUTPUT_PATH  = BASE / "Bukhari_final_with_narrator_ids.json"

# ── load ──────────────────────────────────────────────────────────────────────
print("Loading pipeline-A (_with_ids) …")
with open(WITH_IDS, encoding="utf-8") as f:
    with_ids = json.load(f)

print("Loading pipeline-B (shamela enrichment) …")
with open(ENRICHED, encoding="utf-8") as f:
    enriched = json.load(f)

# ── build shamela_id lookup: (hadith_index, chain_id, narrator_name) → shamela_id
print("Building Shamela ID lookup …")
shamela_lookup: dict[tuple, str | None] = {}
for h in enriched:
    hid = h["hadith_index"]
    for chain in h.get("chains", []):
        cid = chain["chain_id"]
        for n in chain.get("narrators", []):
            key = (hid, cid, n["name"])
            shamela_lookup[key] = n.get("shamela_id")

# ── merge ─────────────────────────────────────────────────────────────────────
print("Merging …")

stats = {
    "total_narrators":    0,
    "from_pipeline_a":    0,
    "from_shamela":       0,
    "still_null":         0,
}

final_hadiths = []

for h in with_ids:
    hid = h["hadith_index"]
    merged_chains = []

    for chain in h.get("chains", []):
        cid = chain["chain_id"]
        merged_narrators = []

        for n in chain.get("narrators", []):
            stats["total_narrators"] += 1
            nid_a = n.get("narrator_id")        # pipeline A
            key   = (hid, cid, n["name"])
            nid_b = shamela_lookup.get(key)     # pipeline B

            if nid_a is not None:
                final_id     = nid_a
                final_source = "pipeline_a"
                stats["from_pipeline_a"] += 1
            elif nid_b is not None:
                final_id     = nid_b
                final_source = "shamela_enrichment"
                stats["from_shamela"] += 1
            else:
                final_id     = None
                final_source = None
                stats["still_null"] += 1

            merged_narrators.append({
                **n,
                "final_narrator_id":        final_id,
                "final_narrator_id_source": final_source,
            })

        merged_chains.append({**chain, "narrators": merged_narrators})

    final_hadiths.append({**h, "chains": merged_chains})

# ── summary ───────────────────────────────────────────────────────────────────
total = stats["total_narrators"]
resolved = total - stats["still_null"]

print(f"\n{'─'*54}")
print(f"Total narrators          : {total}")
print(f"  from pipeline_a        : {stats['from_pipeline_a']:>6}  ({stats['from_pipeline_a']/total*100:.1f}%)")
print(f"  from shamela_enrichment: {stats['from_shamela']:>6}  ({stats['from_shamela']/total*100:.1f}%)")
print(f"  still null             : {stats['still_null']:>6}  ({stats['still_null']/total*100:.1f}%)")
print(f"  TOTAL RESOLVED         : {resolved:>6}  ({resolved/total*100:.1f}%)")
print(f"{'─'*54}")

# ── write ─────────────────────────────────────────────────────────────────────
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(final_hadiths, f, ensure_ascii=False, indent=2)

print(f"\nOutput written to: {OUTPUT_PATH}")
