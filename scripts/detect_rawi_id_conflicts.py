#!/usr/bin/env python3
"""Detect hadiths where the same rawi_id maps to two different narrator names.

These indicate bad data in bukhari_pedia_advanced_extraction_results.json where
the LLM-based matching incorrectly assigned the same ID to different people.

Usage:
    python scripts/detect_rawi_id_conflicts.py
    python scripts/detect_rawi_id_conflicts.py --input path/to/advanced_extraction.json
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

DEFAULT_INPUT = (
    Path(__file__).parent.parent
    / "data_snapshots/full_backup/2026-03-29"
    / "extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json"
)


def strip_tashkeel(text: str) -> str:
    """Strip Arabic diacritics for display normalisation."""
    tashkeel = "\u0610\u0611\u0612\u0613\u0614\u0615\u0616\u0617\u0618\u0619\u061a\u064b\u064c\u064d\u064e\u064f\u0650\u0651\u0652\u0653\u0654\u0655\u0656\u0657\u0658\u0659\u065a\u065b\u065c\u065d\u065e\u065f"
    return text.translate(str.maketrans("", "", tashkeel))


def find_conflicts(data: list[dict]) -> list[dict]:
    """Return one record per conflict: hadith_indices, chain_id, rawi_id, names."""
    conflicts = []

    for hadith in data:
        hadith_indices = hadith.get("hadith_indices", [])
        hadith_url = hadith.get("hadith_url", "")

        for chain in hadith.get("chains", []):
            chain_id = chain.get("chain_id", "?")
            # Map rawi_id → set of plain names seen in this chain
            id_to_names: dict[int, set[str]] = defaultdict(set)

            for n in chain.get("narrators", []):
                rawi_id = n.get("rawi_id")
                name = n.get("name", "")
                if rawi_id is None:
                    continue
                id_to_names[rawi_id].add(strip_tashkeel(name))

            for rawi_id, names in id_to_names.items():
                if len(names) > 1:
                    conflicts.append({
                        "hadith_indices": hadith_indices,
                        "hadith_url": hadith_url,
                        "chain_id": chain_id,
                        "rawi_id": rawi_id,
                        "names": sorted(names),
                    })

    return conflicts


def main():
    parser = argparse.ArgumentParser(
        description="Find narrator rawi_id conflicts in advanced extraction JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/detect_rawi_id_conflicts.py\n"
            "  python scripts/detect_rawi_id_conflicts.py --input data.json\n"
        ),
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to bukhari_pedia_advanced_extraction_results.json",
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"ERROR: File not found: {args.input}", file=sys.stderr)
        print("Pull from R2 first: python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {args.input} ...")
    with open(args.input, encoding="utf-8") as f:
        data = json.load(f)
    print(f"  {len(data)} hadiths loaded.")

    conflicts = find_conflicts(data)

    if not conflicts:
        print("\nNo rawi_id conflicts found. Data looks clean.")
        return

    print(f"\nFound {len(conflicts)} conflict(s):\n")
    for c in conflicts:
        names_str = " vs ".join(f'"{n}"' for n in c["names"])
        print(
            f"  hadith_indices={c['hadith_indices']}  "
            f"chain={c['chain_id']}  "
            f"rawi_id={c['rawi_id']}  "
            f"names: {names_str}"
        )
        print(f"    url: {c['hadith_url']}")

    print(f"\nTotal conflicts: {len(conflicts)}")
    print("\nTo fix: identify the correct rawi_id for the wrongly-assigned narrator,")
    print("then patch the source JSON and re-run preprocess.py.")


if __name__ == "__main__":
    main()
