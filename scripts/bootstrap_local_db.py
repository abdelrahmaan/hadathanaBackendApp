"""
Bootstrap a local MongoDB database from JSONL files.

Checks if the target database already has data. If empty (or --force),
imports all JSONL files, creates indexes, and computes narrator stats.

Usage:
    python scripts/bootstrap_local_db.py                  # default: HadithDataDev
    python scripts/bootstrap_local_db.py --db HadithData  # specify database
    python scripts/bootstrap_local_db.py --force           # re-import even if data exists
    python scripts/bootstrap_local_db.py --uri mongodb://mongo:27017/  # custom URI
"""

import argparse
import json
import pathlib
import subprocess
import sys
import time

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from normalization import normalize_for_search

load_dotenv()

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PYTHON = sys.executable

_PROCESSED_SHAMELA = REPO_ROOT / "mongo_migration" / "processed_bukhari_shamela"
_PROCESSED_PODIA = REPO_ROOT / "mongo_migration" / "processed_bukhari_podia"

# Same mapping as upload.py: (jsonl_path, collection_name, upsert_key_fields)
SOURCES = [
    (_PROCESSED_SHAMELA / "hadith_pages.jsonl", "raw_shamela_hadith_pages", ("book_id", "page_number")),
    (_PROCESSED_SHAMELA / "narrators.jsonl", "raw_shamela_narrators", ("narrator_id",)),
    (_PROCESSED_SHAMELA / "preprocessed_bukhari.jsonl", "raw_shamela_books", ("source", "hadith_index")),
    (_PROCESSED_PODIA / "bukhari_podia_hadiths.jsonl", "processed_podia_books", ("hadith_url",)),
    (_PROCESSED_PODIA / "bukhari_podia_raw_hadiths.jsonl", "raw_podia_books", ("hadith_url",)),
    (_PROCESSED_PODIA / "bukhari_podia_narrators.jsonl", "processed_podia_narrators", ("rawi_id",)),
    (_PROCESSED_PODIA / "narrators_tarajem.jsonl", "processed_podia_narrator_biographies", ("rawi_id",)),
]

# Enrichment files: slim JSONL with just (hadith_url + field).
# These are merged into processed_podia_books via $set (NOT full-document upsert).
# Using mongoimport --mode=upsert on these would REPLACE entire documents — don't do that.
ENRICHMENTS = [
    (REPO_ROOT / "hadith_topics.jsonl", "processed_podia_books", "hadith_url", "topics"),
    (REPO_ROOT / "hadith_embeddings.jsonl", "processed_podia_books", "hadith_url", "matn_embedding"),
]

BATCH_SIZE = 500


def apply_enrichment(client: MongoClient, db_name: str, jsonl_path: pathlib.Path,
                     collection_name: str, match_key: str, field_name: str):
    """Merge a slim JSONL enrichment file into existing documents via $set."""
    if not jsonl_path.exists():
        print(f"  [SKIP] Enrichment file not found: {jsonl_path.name}")
        return

    col = client[db_name][collection_name]
    ops = []
    count = 0
    t0 = time.time()

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                continue
            ops.append(UpdateOne(
                {match_key: doc[match_key]},
                {"$set": {field_name: doc[field_name]}},
            ))
            if len(ops) >= BATCH_SIZE:
                col.bulk_write(ops, ordered=False)
                count += len(ops)
                ops = []

    if ops:
        col.bulk_write(ops, ordered=False)
        count += len(ops)

    elapsed = time.time() - t0
    print(f"  {field_name} → {collection_name}: {count} docs ({elapsed:.1f}s)")


def import_collection(client: MongoClient, db_name: str, jsonl_path: pathlib.Path,
                      collection_name: str, upsert_keys: tuple) -> int:
    """Import a JSONL file into a collection. Returns document count."""
    if not jsonl_path.exists():
        print(f"  [SKIP] File not found: {jsonl_path}")
        return 0

    col = client[db_name][collection_name]
    batch: list = []
    upserted = modified = 0
    t0 = time.time()

    def _flush(batch):
        nonlocal upserted, modified
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
            modified += result.modified_count
        except BulkWriteError as bwe:
            upserted += bwe.details.get("nUpserted", 0)
            modified += bwe.details.get("nModified", 0)

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                batch.append(json.loads(line))
            except json.JSONDecodeError:
                continue
            if len(batch) >= BATCH_SIZE:
                _flush(batch)
                batch = []

    if batch:
        _flush(batch)

    elapsed = time.time() - t0
    total = upserted + modified
    print(f"  {collection_name}: {upserted} new, {modified} updated ({elapsed:.1f}s)")
    return col.estimated_document_count()


def run_script(script_path: str, uri: str, db_name: str):
    """Run a migration script with the appropriate env vars."""
    env_vars = {
        "MONGODB_URI_READ_WRITE": uri,
        "DB_NAME": db_name,
    }
    import os
    env = {**os.environ, **env_vars}
    result = subprocess.run(
        [PYTHON, str(REPO_ROOT / script_path)],
        env=env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  [ERROR] {script_path} failed:\n{result.stderr}")
    else:
        # Print last few meaningful lines
        lines = [l for l in result.stdout.strip().split("\n") if l.strip()]
        for line in lines[-3:]:
            print(f"  {line}")


def _add_search_fields(client: MongoClient, db_name: str):
    """
    Derive _search fields from existing _plain fields via normalize_for_search().
    Safe to re-run — overwrites existing _search values.
    """
    db = client[db_name]

    def _bulk(col, ops, label):
        if ops:
            result = col.bulk_write(ops, ordered=False)
            print(f"  {label}: {result.modified_count} docs")

    # processed_podia_books
    col = db["processed_podia_books"]
    ops = []
    for doc in col.find({}, {"hadith_text_plain": 1, "sanad_text_plain": 1, "matn_text_plain": 1}):
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {
            "hadith_text_search": normalize_for_search(doc.get("hadith_text_plain") or ""),
            "sanad_text_search":  normalize_for_search(doc.get("sanad_text_plain") or ""),
            "matn_text_search":   normalize_for_search(doc.get("matn_text_plain") or ""),
        }}))
        if len(ops) >= BATCH_SIZE:
            _bulk(col, ops, "processed_podia_books (batch)")
            ops = []
    _bulk(col, ops, "processed_podia_books")

    # raw_shamela_books
    col = db["raw_shamela_books"]
    ops = []
    for doc in col.find({}, {"hadith_plain": 1}):
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {
            "hadith_search": normalize_for_search(doc.get("hadith_plain") or ""),
        }}))
        if len(ops) >= BATCH_SIZE:
            _bulk(col, ops, "raw_shamela_books (batch)")
            ops = []
    _bulk(col, ops, "raw_shamela_books")

    # raw_shamela_narrators
    col = db["raw_shamela_narrators"]
    ops = []
    for doc in col.find({}, {"name_plain": 1, "kunya": 1, "nasab": 1}):
        upd = {"name_search": normalize_for_search(doc.get("name_plain") or "")}
        if doc.get("kunya"):
            upd["kunya_search"] = normalize_for_search(doc["kunya"])
        if doc.get("nasab"):
            upd["nasab_search"] = normalize_for_search(doc["nasab"])
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": upd}))
        if len(ops) >= BATCH_SIZE:
            _bulk(col, ops, "raw_shamela_narrators (batch)")
            ops = []
    _bulk(col, ops, "raw_shamela_narrators")

    # processed_podia_narrators
    col = db["processed_podia_narrators"]
    ops = []
    for doc in col.find({}, {"name_in_chain_plain": 1, "full_name_plain": 1}):
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {
            "name_in_chain_search": normalize_for_search(doc.get("name_in_chain_plain") or ""),
            "full_name_search":     normalize_for_search(doc.get("full_name_plain") or ""),
        }}))
        if len(ops) >= BATCH_SIZE:
            _bulk(col, ops, "processed_podia_narrators (batch)")
            ops = []
    _bulk(col, ops, "processed_podia_narrators")


def main():
    parser = argparse.ArgumentParser(description="Bootstrap local MongoDB from JSONL files")
    parser.add_argument("--db", default="HadithDataDev", help="Target database name")
    parser.add_argument("--uri", default="mongodb://localhost:27017/", help="MongoDB URI")
    parser.add_argument("--force", action="store_true", help="Re-import even if data exists")
    args = parser.parse_args()

    print(f"Connecting to {args.uri} ...")
    client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception as exc:
        sys.exit(f"ERROR: Cannot connect to MongoDB at {args.uri}: {exc}")
    print("Connected.\n")

    db = client[args.db]
    existing_collections = db.list_collection_names()

    if existing_collections and not args.force:
        # Check if data actually exists
        total_docs = sum(db[c].estimated_document_count() for c in existing_collections)
        if total_docs > 0:
            print(f"Database '{args.db}' already has {len(existing_collections)} collections "
                  f"with {total_docs:,} total documents.")
            print("Use --force to re-import anyway.")
            # Print summary
            for c in sorted(existing_collections):
                count = db[c].estimated_document_count()
                print(f"  {c}: {count:,}")
            client.close()
            return

    print(f"Importing into '{args.db}' ...\n")

    # Step 1: Import JSONL files
    print("=== Step 1: Import JSONL files ===")
    for jsonl_path, collection_name, upsert_keys in SOURCES:
        import_collection(client, args.db, jsonl_path, collection_name, upsert_keys)

    # Step 2: Create indexes
    print("\n=== Step 2: Create indexes ===")
    run_script("mongo_migration/create_indexes.py", args.uri, args.db)

    # Step 3: Compute stats
    print("\n=== Step 3: Compute narrator stats (Podia) ===")
    run_script("mongo_migration/processed_bukhari_podia/compute_stats.py", args.uri, args.db)

    print("\n=== Step 4: Compute narrator stats (Shamela) ===")
    run_script("mongo_migration/processed_bukhari_shamela/compute_stats.py", args.uri, args.db)

    # Step 5: Apply enrichments (topics, embeddings)
    print("\n=== Step 5: Apply enrichments (topics, embeddings) ===")
    for jsonl_path, collection_name, match_key, field_name in ENRICHMENTS:
        apply_enrichment(client, args.db, jsonl_path, collection_name, match_key, field_name)

    # Step 6: Add _search fields (normalized versions of _plain fields for fuzzy search)
    print("\n=== Step 6: Add _search fields ===")
    _add_search_fields(client, args.db)

    # Summary
    print("\n=== Summary ===")
    for c in sorted(db.list_collection_names()):
        count = db[c].estimated_document_count()
        print(f"  {c}: {count:,}")

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
