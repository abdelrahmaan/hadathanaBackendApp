"""
Backfill `summary.kunya_search` on existing narrator biographies.

`summary.kunya` is stored in formal Arabic (with أ/ة), but the chatbot
`search_narrators` tool queries with a normalized pattern. Without a
normalized mirror field, queries miss every kunya. This script adds
`summary.kunya_search` = normalize_for_search(summary.kunya) to every
biography that has a non-null kunya.

Idempotent: safe to re-run. Skips docs already populated unless --force.

Usage:
  PYTHON=/home/abdo_kamar/Projects/.venv/bin/python
  $PYTHON scripts/backfill_kunya_search.py                     # dev DB
  $PYTHON scripts/backfill_kunya_search.py --db HadithData     # prod
  $PYTHON scripts/backfill_kunya_search.py --force             # rebuild all
  $PYTHON scripts/backfill_kunya_search.py --dry-run           # count only
"""
import argparse
import sys
from pathlib import Path

from pymongo import MongoClient, UpdateOne

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
from app.normalization import normalize_for_search  # noqa: E402

COLLECTION = "processed_podia_narrator_biographies"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--uri", default="mongodb://localhost:27017/")
    p.add_argument("--db", default="HadithDataDev")
    p.add_argument("--force", action="store_true",
                   help="Recompute kunya_search even if already set.")
    p.add_argument("--dry-run", action="store_true",
                   help="Count target docs and exit without writing.")
    return p.parse_args()


def main():
    args = parse_args()
    client = MongoClient(args.uri)
    col = client[args.db][COLLECTION]

    base = {"summary.kunya": {"$exists": True, "$ne": None}}
    if args.force:
        query = base
    else:
        query = {**base, "summary.kunya_search": {"$exists": False}}

    total = col.count_documents(query)
    print(f"[{args.db}.{COLLECTION}] target docs: {total}")

    if args.dry_run or total == 0:
        return

    ops = []
    for doc in col.find(query, {"rawi_id": 1, "summary.kunya": 1}):
        kunya = doc["summary"]["kunya"]
        ops.append(UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {"summary.kunya_search": normalize_for_search(kunya)}},
        ))
        if len(ops) >= 500:
            res = col.bulk_write(ops, ordered=False)
            print(f"  matched: {res.matched_count}, modified: {res.modified_count}")
            ops = []

    if ops:
        res = col.bulk_write(ops, ordered=False)
        print(f"  matched: {res.matched_count}, modified: {res.modified_count}")

    print("done.")


if __name__ == "__main__":
    main()
