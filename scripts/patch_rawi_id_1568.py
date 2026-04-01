#!/usr/bin/env python3
"""Patch rawi_id=1568 (نافع) incorrectly applied to ابن عمر in advanced extraction JSON.

Background:
  rawi_id=1568 belongs to نافع (mawla of Ibn ʿUmar).
  rawi_id=772  belongs to عبد الله بن عمر (Ibn ʿUmar, the Companion).

  The LLM matching assigned رawi_id=1568 to both narrators in chains like:
    ... → نافع → ابن عمر → ...
  causing ~292 hadiths to have a duplicate rawi_id in chain_1.

Fix:
  Any chain entry with rawi_id=1568 whose plain name is NOT one of نافع's known
  name forms is reassigned to rawi_id=772.

  نافع's valid name forms (rawi_id=1568 is CORRECT for these):
    - نافع           (primary name)
    - أبو عبد الله   (kunya)
    - أبيه           (used in chains where عمر بن نافع narrates from his father)

Usage:
    python scripts/patch_rawi_id_1568.py [--dry-run] [--input FILE] [--output FILE]
"""

import argparse
import json
import re
import sys
from pathlib import Path

DEFAULT_INPUT = (
    Path(__file__).parent.parent
    / "data_snapshots/full_backup/2026-03-29"
    / "extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json"
)

NAFIE_RAWI_ID = 1568
IBN_UMAR_RAWI_ID = 772

# Plain-text name forms that correctly belong to نافع (rawi_id=1568)
NAFIE_NAMES = {"نافع", "أبو عبد الله", "أبيه"}


def strip_tashkeel(text: str) -> str:
    tashkeel = (
        "\u0610\u0611\u0612\u0613\u0614\u0615\u0616\u0617\u0618\u0619\u061a"
        "\u064b\u064c\u064d\u064e\u064f\u0650\u0651\u0652\u0653\u0654\u0655"
        "\u0656\u0657\u0658\u0659\u065a\u065b\u065c\u065d\u065e\u065f"
    )
    return text.translate(str.maketrans("", "", tashkeel))


def needs_patch(name: str) -> bool:
    """Return True if a name assigned rawi_id=1568 should be patched to 772."""
    plain = strip_tashkeel(name).strip()
    return plain not in NAFIE_NAMES


def patch_data(data: list[dict], dry_run: bool) -> tuple[int, int]:
    """Patch all incorrect rawi_id=1568 entries. Returns (hadiths_changed, entries_changed)."""
    entries_changed = 0
    hadiths_changed = set()

    for hadith in data:
        hadith_key = tuple(hadith.get("hadith_indices", []))
        for chain in hadith.get("chains", []):
            for n in chain.get("narrators", []):
                if n.get("rawi_id") == NAFIE_RAWI_ID and needs_patch(n.get("name", "")):
                    if not dry_run:
                        n["rawi_id"] = IBN_UMAR_RAWI_ID
                    hadiths_changed.add(hadith_key)
                    entries_changed += 1

    return len(hadiths_changed), entries_changed


def main():
    parser = argparse.ArgumentParser(
        description="Patch rawi_id=1568 (نافع) wrongly applied to ابن عمر.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/patch_rawi_id_1568.py --dry-run\n"
            "  python scripts/patch_rawi_id_1568.py\n"
        ),
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path (default: overwrite input file)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing",
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"ERROR: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    output_path = args.output or args.input

    print(f"Loading {args.input} ...")
    with open(args.input, encoding="utf-8") as f:
        data = json.load(f)
    print(f"  {len(data)} hadiths loaded.")

    hadiths_changed, entries_changed = patch_data(data, dry_run=args.dry_run)

    if args.dry_run:
        print(f"\n[DRY RUN] Would patch {entries_changed} chain entries across {hadiths_changed} hadiths.")
        print(f"  rawi_id {NAFIE_RAWI_ID} → {IBN_UMAR_RAWI_ID} (نافع → ابن عمر)")
        print("\nRun without --dry-run to apply.")
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\nPatched {entries_changed} entries across {hadiths_changed} hadiths.")
        print(f"  rawi_id {NAFIE_RAWI_ID} → {IBN_UMAR_RAWI_ID} (نافع → ابن عمر)")
        print(f"  Written to: {output_path}")
        print("\nNext steps:")
        print("  python mongo_migration/processed_bukhari_podia/preprocess.py")
        print("  python mongo_migration/upload.py")


if __name__ == "__main__":
    main()
