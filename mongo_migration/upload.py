"""
Upload pre-processed JSONL data to MongoDB Atlas.

Run pre_processing.py FIRST to generate the files in processed/.
Then run this script to upload them.

Collections:
  hadith_pages  ← mongo_migration/processed/hadith_pages.jsonl
  narrators     ← mongo_migration/processed/narrators.jsonl
  bukhari_book  ← mongo_migration/processed/preprocessed_bukhari.jsonl

Usage:
    python mongo_migration/upload.py
"""

import json
import os
import pathlib
import sys
import time

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

load_dotenv()

MONGODB_URI = os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    sys.exit("ERROR: MONGODB_URI not found in environment / .env file")

DB_NAME = "hadith_graph"
BATCH_SIZE = 500

_PROCESSED = pathlib.Path(__file__).parent / "processed"

# (jsonl_path, collection_name, upsert_key_fields)
# upsert_key_fields: tuple of doc fields used to identify an existing document.
SOURCES = [
    (_PROCESSED / "hadith_pages.jsonl",         "hadith_pages",  ("book_id", "page_number")),
    (_PROCESSED / "narrators.jsonl",             "narrators",     ("narrator_id",)),
    (_PROCESSED / "preprocessed_bukhari.jsonl",  "bukhari_book",  ("source", "hadith_index")),
]


# ------------------------------------------------------------------
# Upload logic
# ------------------------------------------------------------------

def upload(client: MongoClient, jsonl_path: pathlib.Path, collection_name: str, upsert_keys: tuple):
    if not jsonl_path.exists():
        print(f"\n[SKIP] Processed file not found: {jsonl_path}")
        print(f"       Run pre_processing.py first.")
        return

    col = client[DB_NAME][collection_name]
    batch: list = []
    upserted = inserted = errors = 0
    t0 = time.time()

    print(f"\n{'='*60}")
    print(f"Source     : {jsonl_path.name}")
    print(f"Collection : {DB_NAME}.{collection_name}")
    print(f"Upsert key : {upsert_keys}")
    print(f"{'='*60}")

    def _flush(batch):
        nonlocal upserted, inserted, errors
        ops = [
            UpdateOne(
                {k: doc[k] for k in upsert_keys if k in doc},
                {"$set": doc},
                upsert=True,
            )
            for doc in batch
        ]
        try:
            result = col.bulk_write(ops, ordered=False)
            upserted += result.upserted_count
            inserted += result.modified_count
        except BulkWriteError as bwe:
            upserted += bwe.details.get("nUpserted", 0)
            inserted += bwe.details.get("nModified", 0)
            errors   += len(bwe.details.get("writeErrors", []))

    with open(jsonl_path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  [WARN] line {lineno}: JSON parse error — {exc}")
                continue

            batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                _flush(batch)
                batch = []

    if batch:
        _flush(batch)

    elapsed = time.time() - t0
    print(f"  Inserted (new) : {upserted}")
    print(f"  Updated (exist): {inserted}")
    if errors:
        print(f"  Errors         : {errors}  (bulk write failures)")
    print(f"  Time           : {elapsed:.1f}s")


def main():
    print("Connecting to MongoDB Atlas …")
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    print("Connected.")

    for jsonl_path, collection_name, upsert_keys in SOURCES:
        upload(client, jsonl_path, collection_name, upsert_keys)

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
