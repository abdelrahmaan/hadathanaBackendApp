"""
Enrich Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.jsonl:
  - Rename each narrator's `rawi_id` → `podia_rawi_id`
  - Add `tarajm_candidates` from playwrite_bukhari_hadith_podia_with_tarajm.jsonl
    (keyed by rawi_id)

Also writes unmatched_bukhari_narrators.jsonl: one entry per unique narrator
name where podia_rawi_id is null (i.e. no podia match was found).

Output overwrites Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.jsonl in-place.
"""

import json
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).parent

BUKHARI_PATH   = BASE / "Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.jsonl"
PODIA_TARAJM   = BASE / "playwrite_bukhari_hadith_podia_with_tarajm.jsonl"
UNMATCHED_PATH = BASE / "unmatched_bukhari_narrators.jsonl"
TMP_PATH       = BASE / "_tmp_bukhari_enriched.jsonl"


def load_rawi_tarajm_map(path: Path) -> dict[int, list]:
    """rawi_id → tarajm_candidates list"""
    mapping = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rid = rec.get("rawi_id")
            if rid is not None:
                mapping[rid] = rec.get("tarajm_candidates", [])
    return mapping


def main():
    print("Loading podia→tarajm mapping...")
    rawi_tarajm = load_rawi_tarajm_map(PODIA_TARAJM)
    print(f"  {len(rawi_tarajm)} rawi_id entries loaded")

    # unmatched: narrator name → list of hadith_index (podia_rawi_id is null)
    unmatched: dict[str, list[int]] = defaultdict(list)

    total = matched = 0

    print("Enriching Bukhari JSONL...")
    with BUKHARI_PATH.open(encoding="utf-8") as in_f, \
         TMP_PATH.open("w", encoding="utf-8") as out_f:

        for line in in_f:
            line = line.strip()
            if not line:
                continue
            hadith = json.loads(line)

            for chain in hadith.get("chains", []):
                for narrator in chain.get("narrators", []):
                    total += 1
                    name = narrator.get("name", "")

                    # Rename rawi_id → podia_rawi_id
                    rid = narrator.pop("rawi_id", None)
                    narrator["podia_rawi_id"] = rid

                    # Add tarajm_candidates
                    if rid is not None:
                        narrator["tarajm_candidates"] = rawi_tarajm.get(rid, [])
                        matched += 1
                    else:
                        narrator["tarajm_candidates"] = []
                        unmatched[name].append(hadith["hadith_index"])

            out_f.write(json.dumps(hadith, ensure_ascii=False) + "\n")

    # Replace original with enriched
    TMP_PATH.replace(BUKHARI_PATH)

    # Write unmatched file
    with UNMATCHED_PATH.open("w", encoding="utf-8") as uf:
        for name, indices in sorted(unmatched.items()):
            uf.write(json.dumps({
                "name": name,
                "hadith_count": len(indices),
                "hadith_indices": indices[:20],
            }, ensure_ascii=False) + "\n")

    print(f"\nDone.")
    print(f"  Total narrator mentions : {total}")
    print(f"  With podia_rawi_id      : {matched} ({matched/total*100:.1f}%)")
    print(f"  Without (null)          : {total - matched} — {len(unmatched)} unique names")
    print(f"\nOutput:")
    print(f"  {BUKHARI_PATH}  (updated in-place)")
    print(f"  {UNMATCHED_PATH}")


if __name__ == "__main__":
    main()
