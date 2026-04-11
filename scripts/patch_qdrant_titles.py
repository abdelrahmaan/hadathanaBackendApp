#!/usr/bin/env python3
"""
Patch the `metadata.title` field in existing Qdrant points from MongoDB.

Does NOT re-embed or replace full payloads — only sets the nested key
`metadata.title` on each point using Qdrant's set_payload with dot notation.
Matches points by metadata.hadith_url (works even when hadith_indices is missing).
Safe to re-run (idempotent).

Usage:
    python scripts/patch_qdrant_titles.py
    python scripts/patch_qdrant_titles.py --uri mongodb://mongo:27017/ --db HadithDataDev
    python scripts/patch_qdrant_titles.py --qdrant-url http://localhost:6333
"""
import argparse
import asyncio
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from motor.motor_asyncio import AsyncIOMotorClient
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from app.chatbot.config import QDRANT_COLLECTION


async def build_url_title_map(db, collection: str) -> dict[str, str]:
    """Load all hadith_url → title pairs from MongoDB."""
    url_to_title: dict[str, str] = {}
    cursor = db[collection].find(
        {"title": {"$exists": True}, "hadith_url": {"$exists": True}},
        {"hadith_url": 1, "title": 1, "_id": 0},
    )
    async for doc in cursor:
        url = doc.get("hadith_url")
        title = doc.get("title", "")
        if url and title:
            url_to_title[url] = title
    return url_to_title


def patch_qdrant(qdrant: QdrantClient, url_to_title: dict[str, str]) -> int:
    """Scroll all Qdrant points and patch metadata.title where we have a title.

    Qdrant set_payload does not support dot-notation for nested keys in older
    server versions — so we read the existing metadata dict, merge title in,
    and overwrite the whole metadata key. Also cleans up any stale
    "metadata.title" top-level key left by a previous failed attempt.
    """
    total_patched = 0
    offset = None

    while True:
        result = qdrant.scroll(
            collection_name=QDRANT_COLLECTION,
            limit=200,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        points, next_offset = result

        for point in points:
            payload = point.payload or {}
            metadata = dict(payload.get("metadata") or {})
            hadith_url = metadata.get("hadith_url", "")
            title = url_to_title.get(hadith_url, "")
            if not title:
                continue

            metadata["title"] = title

            # Build a clean full payload and overwrite_payload (full replace, no merge)
            clean_payload = {
                k: v for k, v in payload.items()
                if k != "metadata.title"  # drop stale top-level key
            }
            clean_payload["metadata"] = metadata
            qdrant.overwrite_payload(
                collection_name=QDRANT_COLLECTION,
                payload=clean_payload,
                points=[point.id],
            )
            total_patched += 1

        if total_patched and total_patched % 500 == 0:
            print(f"  patched {total_patched} points...")

        if next_offset is None:
            break
        offset = next_offset

    return total_patched


async def main() -> None:
    parser = argparse.ArgumentParser(description="Patch metadata.title in Qdrant from MongoDB")
    parser.add_argument("--uri", default="mongodb://localhost:27017/", help="MongoDB URI")
    parser.add_argument("--db", default="HadithDataDev", help="Database name")
    parser.add_argument("--qdrant-url", default="http://localhost:6333", help="Qdrant base URL")
    args = parser.parse_args()

    print(f"MongoDB : {args.uri}  db={args.db}")
    mongo = AsyncIOMotorClient(args.uri, serverSelectionTimeoutMS=10_000)
    try:
        await mongo.admin.command("ping")
        print("MongoDB  OK")
    except Exception as exc:
        sys.exit(f"ERROR: MongoDB connection failed: {exc}")

    print(f"Qdrant  : {args.qdrant_url}")
    qdrant = QdrantClient(url=args.qdrant_url, check_compatibility=False)

    print("Loading url→title map from MongoDB...")
    url_to_title = await build_url_title_map(mongo[args.db], "processed_podia_books")
    print(f"  {len(url_to_title):,} titles loaded")

    if not url_to_title:
        sys.exit("ERROR: no titles found in MongoDB — run mongoimport first")

    print("Scrolling Qdrant and patching metadata.title...")
    total = patch_qdrant(qdrant, url_to_title)

    print(f"\nDone. {total} Qdrant points patched with metadata.title.")
    mongo.close()


if __name__ == "__main__":
    asyncio.run(main())
