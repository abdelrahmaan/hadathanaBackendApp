"""
Map narrator names from Bukhari hadith chains to rawi_id from podia clean file.

Matching priority:
  1. Exact match on name_in_chain
  2. Exact match on full_name
  3. Substring: name inside name_in_chain/full_name, or vice versa
  4. Token Jaccard similarity >= 0.5 (word overlap)
  5. null if no match

Outputs:
  - Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.jsonl
      Copy of Bukhari JSON as JSONL with rawi_id added to each narrator
  - unmatched_narrators.jsonl
      One record per unique unmatched narrator name with hadith_index examples
"""

import json
from pathlib import Path
from collections import defaultdict

PODIA_PATH = Path(__file__).parent / "playwrite_bukhari_hadith_podia_clean.jsonl"
BUKHARI_PATH = Path(__file__).parent.parent / "Bukhari" / "Bukhari_Without_Tashkel_results_advanced_with_matn.json"
OUT_WITH_IDS = Path(__file__).parent / "Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.jsonl"
OUT_UNMATCHED = Path(__file__).parent / "unmatched_narrators.jsonl"


def tokenize(text: str | None) -> set:
    if not text:
        return set()
    return set(text.strip().split())


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def load_podia(path: Path) -> list[dict]:
    records = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def build_lookup(records: list[dict]) -> tuple[dict, dict, list]:
    exact_chain = {}   # name_in_chain -> rawi_id
    exact_full = {}    # full_name -> rawi_id
    fuzzy_list = []    # list of (name_in_chain, full_name, rawi_id, tokens_chain, tokens_full)

    for r in records:
        rid = r["rawi_id"]
        nc = r.get("name_in_chain") or ""
        fn = r.get("full_name") or ""
        if nc:
            exact_chain[nc] = rid
        if fn:
            exact_full[fn] = rid
        fuzzy_list.append((nc, fn, rid, tokenize(nc), tokenize(fn)))

    return exact_chain, exact_full, fuzzy_list


def find_rawi_id(name: str, exact_chain: dict, exact_full: dict, fuzzy_list: list) -> int | None:
    # 1. Exact on name_in_chain
    if name in exact_chain:
        return exact_chain[name]

    # 2. Exact on full_name
    if name in exact_full:
        return exact_full[name]

    # 3. Substring
    best_sub = None
    for nc, fn, rid, _, _ in fuzzy_list:
        if name in nc or nc in name:
            best_sub = rid
            break
        if name in fn or fn.startswith(name):
            best_sub = rid
            break
    if best_sub is not None:
        return best_sub

    # 4. Token Jaccard >= 0.5
    name_tokens = tokenize(name)
    best_score = 0.0
    best_rid = None
    for nc, fn, rid, tc, tf in fuzzy_list:
        score = max(jaccard(name_tokens, tc), jaccard(name_tokens, tf))
        if score > best_score:
            best_score = score
            best_rid = rid
    if best_score >= 0.5:
        return best_rid

    return None


def main():
    print("Loading podia narrators...")
    podia = load_podia(PODIA_PATH)
    exact_chain, exact_full, fuzzy_list = build_lookup(podia)
    print(f"  Loaded {len(podia)} podia records")

    print("Loading Bukhari hadith...")
    with BUKHARI_PATH.open(encoding="utf-8") as f:
        hadiths = json.load(f)
    print(f"  Loaded {len(hadiths)} hadiths")

    # Cache: narrator name -> rawi_id (or None)
    cache: dict[str, int | None] = {}
    # Unmatched: name -> list of hadith_index where it appears
    unmatched: dict[str, list[int]] = defaultdict(list)

    total = 0
    matched = 0

    with OUT_WITH_IDS.open("w", encoding="utf-8") as out_f:
        for hadith in hadiths:
            for chain in hadith.get("chains", []):
                for narrator in chain.get("narrators", []):
                    name = narrator.get("name", "").strip()
                    if not name:
                        continue
                    total += 1

                    if name not in cache:
                        cache[name] = find_rawi_id(name, exact_chain, exact_full, fuzzy_list)

                    rid = cache[name]
                    narrator["rawi_id"] = rid

                    if rid is not None:
                        matched += 1
                    else:
                        unmatched[name].append(hadith["hadith_index"])

            out_f.write(json.dumps(hadith, ensure_ascii=False) + "\n")

    # Write unmatched file
    with OUT_UNMATCHED.open("w", encoding="utf-8") as uf:
        for name, indices in sorted(unmatched.items()):
            uf.write(json.dumps({
                "name": name,
                "hadith_count": len(indices),
                "hadith_indices": indices[:20]  # cap at 20 examples
            }, ensure_ascii=False) + "\n")

    unmatched_count = len(unmatched)
    print(f"\nDone.")
    print(f"  Total narrator mentions : {total}")
    print(f"  Matched                 : {matched} ({matched/total*100:.1f}%)")
    print(f"  Unmatched (unique names): {unmatched_count}")
    print(f"\nOutput files:")
    print(f"  {OUT_WITH_IDS}")
    print(f"  {OUT_UNMATCHED}")


if __name__ == "__main__":
    main()
