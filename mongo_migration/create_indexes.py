"""
Create MongoDB indexes for the hadathana-api collections.

Run once after uploading data. Safe to re-run — MongoDB ignores indexes
that already exist with the same definition.

Usage:
    python mongo_migration/create_indexes.py
"""

import os
import sys
import time
import logging

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    OperationFailure,
    AutoReconnect,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("create_indexes")

MONGODB_URI = os.environ.get("MONGODB_URI_READ_WRITE") or os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    sys.exit("ERROR: MONGODB_URI_READ_WRITE (or MONGODB_URI) not found in environment / .env file")

DB_NAME = os.environ.get("DB_NAME", "HadithData")

MAX_RETRIES = 5
RETRY_BASE_DELAY = 3  # seconds

INDEXES = {
    "raw_shamela_books": [
        # Narrator ID filter (GET /hadiths?narrator_id=...)
        ([("unique_narrators.narrator_id", ASCENDING)], {}),
        # Chain type filter (GET /hadiths?chain_type=...)
        ([("chains.type", ASCENDING)], {}),
        # Hadith index lookup (GET /hadiths/{id})
        ([("hadith_index", ASCENDING)], {"unique": True}),
    ],
    "raw_shamela_narrators": [
        # Narrator ID lookup (GET /narrators/{narrator_id})
        ([("narrator_id", ASCENDING)], {"unique": True}),
        # Name search (GET /narrators?name_plain=...)
        ([("name_plain", ASCENDING)], {}),
        # Kunya / nasab filters
        ([("kunya", ASCENDING)], {}),
        ([("nasab", ASCENDING)], {}),
    ],
    "analytics_narrator_stats_shamela": [
        # Primary lookup (GET /narrators/{id}/stats) — already created by
        # compute_narrator_stats.py but listed here for completeness.
        ([("narrator_id", ASCENDING)], {"unique": True}),
    ],
    # --- Podia collections ---
    "processed_podia_books": [
        # Query by any hadith index in the array
        ([("hadith_indices", ASCENDING)], {}),
        # Query by narrator rawi_id
        ([("narrators.rawi_id", ASCENDING)], {}),
        # URL-based unique lookup (upsert key)
        ([("hadith_url", ASCENDING)], {"unique": True}),
        # Book browse
        ([("book", ASCENDING)], {}),
        # Topic filter (GET /api/v2/hadiths?topic=...)
        ([("topics", ASCENDING)], {}),
    ],
    "raw_podia_books": [
        # URL-based unique lookup (upsert key)
        ([("hadith_url", ASCENDING)], {"unique": True}),
        # Query by any hadith index in the array
        ([("hadith_indices", ASCENDING)], {}),
        # Query by narrator rawi_id
        ([("narrators.rawi_id", ASCENDING)], {}),
        # Book browse
        ([("book", ASCENDING)], {}),
    ],
    "processed_podia_narrators": [
        # Primary lookup (GET /podia/narrators/{rawi_id})
        ([("rawi_id", ASCENDING)], {"unique": True}),
        # Name search
        ([("full_name_plain", ASCENDING)], {}),
        ([("name_in_chain_plain", ASCENDING)], {}),
    ],
    "analytics_narrator_stats_podia": [
        ([("rawi_id", ASCENDING)], {"unique": True}),
    ],
    "processed_podia_narrator_biographies": [
        ([("rawi_id", ASCENDING)], {"unique": True}),
        ([("full_name_plain", ASCENDING)], {}),
    ],
}


def connect_with_retry() -> MongoClient:
    """Connect to MongoDB with retries for DNS/network transient failures."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Connecting to MongoDB (attempt %d/%d) …", attempt, MAX_RETRIES)
            client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=30_000,
                connectTimeoutMS=20_000,
                socketTimeoutMS=30_000,
            )
            client.admin.command("ping")
            log.info("Connected. DB: %s", DB_NAME)
            return client
        except (ConnectionFailure, ServerSelectionTimeoutError, AutoReconnect) as exc:
            log.warning("Connection attempt %d failed: %s", attempt, exc)
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.info("Retrying in %ds …", delay)
                time.sleep(delay)
            else:
                log.error("All %d connection attempts failed. Aborting.", MAX_RETRIES)
                raise


def create_index_with_retry(col, keys, kwargs, index_name: str) -> bool:
    """Create a single index with retries for transient errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = col.create_index(keys, **kwargs)
            log.info("  ✓ %s", result)
            return True
        except (AutoReconnect, ConnectionFailure) as exc:
            log.warning("  ⚠ %s (attempt %d): %s", index_name, attempt, exc)
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.info("    Retrying in %ds …", delay)
                time.sleep(delay)
            else:
                log.error("  ✗ %s: failed after %d attempts: %s", index_name, MAX_RETRIES, exc)
                return False
        except OperationFailure as exc:
            log.error("  ✗ %s: MongoDB error: %s", index_name, exc)
            return False
        except Exception as exc:
            log.error("  ✗ %s: unexpected error: %s", index_name, exc)
            return False
    return False


def main():
    client = connect_with_retry()
    db = client[DB_NAME]

    total = 0
    succeeded = 0
    failed = 0
    failed_indexes = []

    for collection_name, index_specs in INDEXES.items():
        log.info("Collection: %s (%d indexes)", collection_name, len(index_specs))
        col = db[collection_name]
        for keys, kwargs in index_specs:
            total += 1
            index_name = "_".join(f"{k}_{v}" for k, v in keys)
            ok = create_index_with_retry(col, keys, kwargs, index_name)
            if ok:
                succeeded += 1
            else:
                failed += 1
                failed_indexes.append(f"{collection_name}.{index_name}")
        log.info("")

    client.close()

    log.info("=" * 50)
    log.info("Summary: %d/%d succeeded, %d failed", succeeded, total, failed)
    if failed_indexes:
        log.error("Failed indexes:")
        for idx in failed_indexes:
            log.error("  - %s", idx)
        sys.exit(1)
    else:
        log.info("All indexes created successfully.")


if __name__ == "__main__":
    main()
