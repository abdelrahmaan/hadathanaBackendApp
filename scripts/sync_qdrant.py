"""
Sync the Qdrant hadiths_matn collection from MongoDB.

Reads processed_podia_books (matn_embedding already stored), computes BM25 sparse
vectors locally, and upserts into Qdrant. Idempotent — skips if counts match.

Usage:
    python scripts/sync_qdrant.py
    python scripts/sync_qdrant.py --uri mongodb://mongo:27017/ --db HadithDataDev
    python scripts/sync_qdrant.py --qdrant-url http://qdrant:6333
    python scripts/sync_qdrant.py --force   # re-sync even if counts match
"""
import argparse
import asyncio
import pathlib
import sys

# Allow importing from app/ when run as a standalone script
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from motor.motor_asyncio import AsyncIOMotorClient
from qdrant_client import QdrantClient

from app.chatbot.indexer import ensure_collection, sync_hadiths


async def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Qdrant hadiths_matn from MongoDB")
    parser.add_argument("--uri", default="mongodb://localhost:27017/", help="MongoDB URI")
    parser.add_argument("--db", default="HadithDataDev", help="Database name")
    parser.add_argument("--qdrant-url", default="http://localhost:6333", help="Qdrant base URL")
    parser.add_argument("--force", action="store_true", help="Re-sync even if counts match")
    args = parser.parse_args()

    print(f"MongoDB : {args.uri}  db={args.db}")
    mongo = AsyncIOMotorClient(args.uri, serverSelectionTimeoutMS=10_000)
    try:
        await mongo.admin.command("ping")
        print("MongoDB  OK")
    except Exception as exc:
        sys.exit(f"ERROR: MongoDB connection failed: {exc}")

    print(f"Qdrant  : {args.qdrant_url}")
    qdrant = QdrantClient(url=args.qdrant_url)

    ensure_collection(qdrant)
    print("Collection ready.")

    n = await sync_hadiths(qdrant, mongo[args.db], force=args.force)
    print(f"Done. {n} points upserted.")
    mongo.close()


if __name__ == "__main__":
    asyncio.run(main())
