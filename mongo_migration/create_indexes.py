"""
Create MongoDB indexes for the hadathana-api collections.

Run once after uploading data. Safe to re-run — MongoDB ignores indexes
that already exist with the same definition.

Usage:
    python mongo_migration/create_indexes.py
"""

import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, TEXT

load_dotenv()

MONGODB_URI = os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    sys.exit("ERROR: MONGODB_URI not found in environment / .env file")

DB_NAME = os.environ.get("DB_NAME", "HadithData")

INDEXES = {
    "bukhari_book": [
        # Narrator ID filter (GET /hadiths?narrator_id=...)
        ([("unique_narrators.narrator_id", ASCENDING)], {}),
        # Chain type filter (GET /hadiths?chain_type=...)
        ([("chains.type", ASCENDING)], {}),
        # Hadith index lookup (GET /hadiths/{id})
        ([("hadith_index", ASCENDING)], {"unique": True}),
    ],
    "narrators": [
        # Narrator ID lookup (GET /narrators/{narrator_id})
        ([("narrator_id", ASCENDING)], {"unique": True}),
        # Name search (GET /narrators?name_plain=...)
        ([("name_plain", ASCENDING)], {}),
        # Kunya / nasab filters
        ([("kunya", ASCENDING)], {}),
        ([("nasab", ASCENDING)], {}),
    ],
    "narrator_stats": [
        # Primary lookup (GET /narrators/{id}/stats) — already created by
        # compute_narrator_stats.py but listed here for completeness.
        ([("narrator_id", ASCENDING)], {"unique": True}),
    ],
}


def main():
    print("Connecting to MongoDB …")
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    print(f"Connected. DB: {DB_NAME}\n")

    db = client[DB_NAME]

    for collection_name, index_specs in INDEXES.items():
        print(f"Collection: {collection_name}")
        col = db[collection_name]
        for keys, kwargs in index_specs:
            name = "_".join(f"{k}_{v}" for k, v in keys)
            try:
                result = col.create_index(keys, **kwargs)
                print(f"  ✓ {result}")
            except Exception as exc:
                print(f"  ✗ {name}: {exc}")
        print()

    client.close()
    print("Done.")


if __name__ == "__main__":
    main()
