#!/usr/bin/env python3
"""
Backfill `topics` from MongoDB into bukhari_podia_hadiths.jsonl.

Reads the JSONL file, fetches topics from processed_podia_books (matched by
hadith_url), merges, and overwrites the file in-place.

USAGE
-----
    python scripts/backfill_topics_to_jsonl.py

    # Dry run — show stats without writing
    python scripts/backfill_topics_to_jsonl.py --dry-run

ENV VARS REQUIRED
-----------------
    MONGODB_URI_READ   — Atlas read-only URI (or MONGODB_URI_READ_WRITE)
    DB_NAME            — optional, default: HadithData
"""

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

_REPO_ROOT = Path(__file__).resolve().parent.parent
JSONL_PATH = _REPO_ROOT / "mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl"
COLLECTION_NAME = "processed_podia_books"
DB_NAME_DEFAULT = "HadithData"


def load_env() -> tuple[str, str]:
    load_dotenv(_REPO_ROOT / ".env")
    mongo_uri = (
        os.environ.get("MONGODB_URI_READ")
        or os.environ.get("MONGODB_URI_READ_WRITE")
        or os.environ.get("MONGODB_URI")
    )
    if not mongo_uri:
        sys.exit("ERROR: MONGODB_URI_READ not set in .env")
    db_name = os.environ.get("DB_NAME", DB_NAME_DEFAULT)
    return mongo_uri, db_name


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Show stats without writing.")
    args = parser.parse_args()

    mongo_uri, db_name = load_env()

    # Load JSONL
    print(f"Reading {JSONL_PATH} ...")
    with open(JSONL_PATH, encoding="utf-8") as f:
        docs = [json.loads(line) for line in f if line.strip()]
    print(f"  Loaded {len(docs):,} documents")

    # Fetch topics from MongoDB keyed by hadith_url
    print(f"Fetching topics from MongoDB ({db_name}.{COLLECTION_NAME}) ...")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    col = client[db_name][COLLECTION_NAME]

    cursor = col.find({"topics": {"$exists": True}}, {"hadith_url": 1, "topics": 1, "_id": 0})
    topics_map: dict[str, list[str]] = {row["hadith_url"]: row["topics"] for row in cursor}
    print(f"  Found topics for {len(topics_map):,} documents in MongoDB")

    # Merge
    n_updated = 0
    n_missing = 0
    for doc in docs:
        url = doc.get("hadith_url")
        if url and url in topics_map:
            doc["topics"] = topics_map[url]
            n_updated += 1
        else:
            n_missing += 1

    print(f"  Updated : {n_updated:,}")
    print(f"  No match: {n_missing:,}")

    if args.dry_run:
        print("[dry-run] No changes written.")
        return

    # Overwrite in-place
    print(f"Writing {JSONL_PATH} ...")
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    print("Done.")


if __name__ == "__main__":
    main()
