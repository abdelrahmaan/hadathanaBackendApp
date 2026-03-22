"""
Run the full narrator ID matching pipeline in order:

  Stage 0 — clean_narrators.py            Clean raw Podia JSONL (remove tashkeel, etc.)
  Stage 1 — match_narrators.py            Bukhari names → podia_rawi_id
  Stage 2 — match_podia_to_tarajm.py      Podia names → tarajm_candidates
  Stage 3 — enrich_bukhari_with_tarajm.py Merge both into final JSONL

Usage:
    python3 run_pipeline.py               # run all stages (0 → 3)
    python3 run_pipeline.py --stage 1     # start from Stage 1 (skip clean)
    python3 run_pipeline.py --stage 2     # tarajm CSV updated — run Stage 2 + 3
    python3 run_pipeline.py --stage 3     # merge only

When to run what:
  - playwrite_bukhari_hadith_podia.jsonl updated (new Podia scrape) → run all (--stage 0)
  - tarajm_people.csv updated (new tarajm scrape)                   → --stage 2
  - Bukhari JSON updated                                             → --stage 1
"""

import argparse
import importlib.util
import time
from pathlib import Path

BASE = Path(__file__).parent

STAGES = [
    ("Stage 0 — Clean Podia JSONL",  BASE / "clean_narrators.py"),
    ("Stage 1 — Bukhari → Podia",    BASE / "match_narrators.py"),
    ("Stage 2 — Podia → Tarajm",     BASE / "match_podia_to_tarajm.py"),
    ("Stage 3 — Merge into JSONL",   BASE / "enrich_bukhari_with_tarajm.py"),
]


def run_stage(label: str, script_path: Path):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    t0 = time.time()

    spec = importlib.util.spec_from_file_location("_stage", script_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()

    elapsed = time.time() - t0
    print(f"\n  Completed in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Run narrator matching pipeline")
    parser.add_argument(
        "--stage", type=int, default=0, choices=[0, 1, 2, 3],
        help="Start from this stage (default: 0 = run all including clean)"
    )
    args = parser.parse_args()

    stages_to_run = STAGES[args.stage:]
    print(f"Running {len(stages_to_run)} stage(s) starting from Stage {args.stage}...")

    total_start = time.time()
    for label, path in stages_to_run:
        run_stage(label, path)

    total = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"  Pipeline complete in {total:.1f}s")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
