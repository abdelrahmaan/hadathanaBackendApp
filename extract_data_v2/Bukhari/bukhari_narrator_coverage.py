"""
bukhari_narrator_coverage.py

Builds a JSONL report of narrator coverage for the Bukhari enriched file.

For each unique narrator_id found in the Bukhari chains, looks up the full
biography from shamela_narrators.jsonl and writes one line per narrator.

Output: bukhari_narrator_coverage.jsonl
Each line has:
  {
    "narrator_id": "3654",
    "mention_count": 42,          # times this narrator appears in Bukhari chains
    "resolution_methods": [...],  # which passes resolved this ID
    "name": "...",                # from shamela_narrators.jsonl (or null)
    "kunya": "...",
    "nasab": "...",
    "tabaqa": "...",
    "rank_ibn_hajar": "...",
    "rank_dhahabi": "...",
    "death_date": "...",
    "bio_status": "found" | "not_in_shamela"
  }

Also prints a summary:
  - Total unique narrator IDs in Bukhari
  - How many have a bio in shamela_narrators.jsonl
  - How many are missing
  - Breakdown by resolution method
"""

import json
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE = Path(__file__).parent
EXTRACT_V2 = BASE.parent

BUKHARI_IDS_FILE = EXTRACT_V2 / "Bukhari" / "Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json"
SHAMELA_NARRATORS = BASE / "shamela_narrators.jsonl"
OUTPUT_FILE = BASE / "bukhari_narrator_coverage.jsonl"


# ---------------------------------------------------------------------------
# Step 1: Collect narrator stats from Bukhari enriched file
# ---------------------------------------------------------------------------

def collect_bukhari_stats(bukhari_path: Path) -> tuple[dict, int, int]:
    """
    Returns:
        narrator_stats: narrator_id -> {mention_count, resolution_methods (set)}
        total_mentions: total narrator slot count
        null_count: narrator slots with no ID
    """
    narrator_stats: dict[str, dict] = {}
    total_mentions = 0
    null_count = 0

    with open(bukhari_path, encoding="utf-8") as f:
        hadiths = json.load(f)

    for hadith in hadiths:
        for chain in hadith.get("chains", []):
            for narrator in chain.get("narrators", []):
                total_mentions += 1
                nid = narrator.get("narrator_id")
                if nid is None:
                    null_count += 1
                    continue
                resolution = narrator.get("narrator_id_resolution") or "exact_match"
                if nid not in narrator_stats:
                    narrator_stats[nid] = {"mention_count": 0, "resolution_methods": set()}
                narrator_stats[nid]["mention_count"] += 1
                narrator_stats[nid]["resolution_methods"].add(resolution)

    return narrator_stats, total_mentions, null_count


# ---------------------------------------------------------------------------
# Step 2: Load shamela_narrators.jsonl index
# ---------------------------------------------------------------------------

def load_shamela_bios(jsonl_path: Path) -> dict:
    """Returns narrator_id -> bio dict (only status=success rows)."""
    bios: dict[str, dict] = {}
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("status") != "success":
                continue
            nid = str(obj.get("narrator_id", "")).strip()
            if nid:
                bios[nid] = obj
    return bios


# ---------------------------------------------------------------------------
# Step 3: Build and write report
# ---------------------------------------------------------------------------

def build_report(narrator_stats: dict, bios: dict, output_path: Path):
    rows = []
    found_count = 0
    missing_count = 0

    for nid, stats in sorted(narrator_stats.items(), key=lambda x: -x[1]["mention_count"]):
        bio = bios.get(nid)
        if bio:
            found_count += 1
            bio_status = "found"
        else:
            missing_count += 1
            bio_status = "not_in_shamela"

        row = {
            "narrator_id": nid,
            "mention_count": stats["mention_count"],
            "resolution_methods": sorted(stats["resolution_methods"]),
            "bio_status": bio_status,
            "name": bio.get("name") if bio else None,
            "kunya": bio.get("kunya") if bio else None,
            "nasab": bio.get("nasab") if bio else None,
            "tabaqa": bio.get("tabaqa") if bio else None,
            "rank_ibn_hajar": bio.get("rank_ibn_hajar") if bio else None,
            "rank_dhahabi": bio.get("rank_dhahabi") if bio else None,
            "death_date": bio.get("death_date") if bio else None,
        }
        rows.append(row)

    with open(output_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return found_count, missing_count, rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Loading Bukhari enriched file ...")
    narrator_stats, total_mentions, null_count = collect_bukhari_stats(BUKHARI_IDS_FILE)
    unique_ids = len(narrator_stats)

    print("Loading shamela_narrators.jsonl ...")
    bios = load_shamela_bios(SHAMELA_NARRATORS)
    print(f"  Narrator bios available : {len(bios)}")

    print("Building coverage report ...")
    found_count, missing_count, rows = build_report(narrator_stats, bios, OUTPUT_FILE)

    # Resolution method breakdown (across all mentions with an ID)
    method_counts: dict[str, int] = defaultdict(int)
    for stats in narrator_stats.values():
        for method in stats["resolution_methods"]:
            method_counts[method] += stats["mention_count"]

    resolved_mentions = total_mentions - null_count

    print(f"\nWriting report to {OUTPUT_FILE.name} ...")
    print(f"\n--- Bukhari Narrator Coverage ---")
    print(f"  Total narrator mentions    : {total_mentions}")
    print(f"  Resolved (have an ID)      : {resolved_mentions}  ({resolved_mentions/total_mentions*100:.1f}%)")
    print(f"  Still null (no ID)         : {null_count}  ({null_count/total_mentions*100:.1f}%)")
    print(f"\n  Unique narrator IDs        : {unique_ids}")
    print(f"  With bio in Shamela        : {found_count}  ({found_count/unique_ids*100:.1f}%)")
    print(f"  Missing from Shamela bios  : {missing_count}  ({missing_count/unique_ids*100:.1f}%)")
    print(f"\n  Resolution method breakdown (by mention count):")
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        print(f"    {method:<20}: {count}  ({count/total_mentions*100:.1f}%)")

    # Top 10 most frequent narrators
    print(f"\n  Top 10 narrators by mention count:")
    print(f"  {'ID':<8} {'Mentions':<10} {'Bio?':<8} Name")
    print(f"  {'-'*8} {'-'*10} {'-'*8} {'-'*30}")
    for row in rows[:10]:
        name = (row["name"] or "—")[:40]
        print(f"  {row['narrator_id']:<8} {row['mention_count']:<10} {row['bio_status']:<8} {name}")

    print("\nDone.")


if __name__ == "__main__":
    main()
