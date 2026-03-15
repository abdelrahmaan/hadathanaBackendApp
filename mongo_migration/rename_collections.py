"""
One-time script to rename MongoDB collections to the new naming schema.

Run this ONCE against Atlas BEFORE deploying updated code.
Safe to re-run: skips renames where the target already exists.

Usage:
    python mongo_migration/rename_collections.py

Requires MONGODB_URI_READ_WRITE (or MONGODB_URI) in .env with
permissions to rename collections (typically Atlas admin/readWrite role).
"""

import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure

load_dotenv()

MONGODB_URI = os.environ.get("MONGODB_URI_READ_WRITE") or os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    sys.exit("ERROR: MONGODB_URI_READ_WRITE (or MONGODB_URI) not found in environment / .env file")

DB_NAME = os.environ.get("DB_NAME", "HadithData")

# (old_name, new_name)
RENAMES = [
    ("bukhari_book",            "raw_shamela_books"),
    ("narrators",               "raw_shamela_narrators"),
    ("hadith_pages",            "raw_shamela_hadith_pages"),
    ("narrator_stats",          "analytics_narrator_stats_shamela"),
    ("analytics_narrator_stats", "analytics_narrator_stats_shamela"),
    ("bukhari_book_podia",      "raw_podia_books"),
    ("narrators_podia",         "raw_podia_narrators"),
    ("narrators_tarajem_podia", "raw_podia_narrator_biographies"),
    ("narrator_stats_podia",    "analytics_narrator_stats_podia"),
]


def main():
    print("Connecting to MongoDB Atlas …")
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    print(f"Connected. DB: {DB_NAME}\n")

    db = client[DB_NAME]
    existing = set(db.list_collection_names())

    for old, new in RENAMES:
        if new in existing:
            print(f"  [SKIP] '{new}' already exists — skipping rename of '{old}'")
            continue
        if old not in existing:
            print(f"  [WARN] '{old}' not found — skipping")
            continue
        try:
            # renameCollection must be run against the admin db with full namespaces
            client.admin.command(
                "renameCollection",
                f"{DB_NAME}.{old}",
                to=f"{DB_NAME}.{new}",
            )
            print(f"  [OK]   '{old}' → '{new}'")
        except OperationFailure as exc:
            print(f"  [ERR]  '{old}' → '{new}': {exc}")

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
