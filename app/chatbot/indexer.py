"""
Populate the Qdrant `hadiths_matn` collection from MongoDB.

Reuses stored matn_embedding vectors — does NOT re-embed.
BM25 sparse vectors are computed locally via FastEmbedSparse.
"""
import logging

from fastembed import SparseTextEmbedding
from motor.motor_asyncio import AsyncIOMotorDatabase
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from app.chatbot.config import QDRANT_COLLECTION, EMBED_DIM

logger = logging.getLogger("hadathana.chatbot.indexer")

_PROJECTION = {
    "_id": 0,
    "hadith_url": 1,
    "hadith_indices": 1,
    "book": 1,
    "chapter": 1,
    "matn_text": 1,
    "matn_text_plain": 1,
    "topics": 1,
    "title": 1,
    "narrators": 1,
    "matn_embedding": 1,
}


def ensure_collection(client: QdrantClient) -> None:
    """Create hadiths_matn collection if it doesn't exist. Idempotent."""
    if client.collection_exists(QDRANT_COLLECTION):
        return
    client.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config={"dense": qmodels.VectorParams(size=EMBED_DIM, distance=qmodels.Distance.COSINE)},
        sparse_vectors_config={"text-sparse": qmodels.SparseVectorParams()},
    )
    logger.info("collection_created", extra={"event": "collection_created", "collection": QDRANT_COLLECTION})


async def sync_hadiths(
    client: QdrantClient,
    db: AsyncIOMotorDatabase,
    *,
    force: bool = False,
    batch_size: int = 200,
) -> int:
    """
    Upsert hadiths from Mongo into Qdrant. Returns count of points upserted.
    Skips if Qdrant count already matches Mongo count (unless force=True).
    """
    mongo_count = await db["processed_podia_books"].count_documents(
        {"matn_embedding": {"$exists": True}}
    )
    qdrant_count = client.count(QDRANT_COLLECTION).count

    if not force and qdrant_count == mongo_count:
        logger.info(
            "sync_skipped",
            extra={"event": "sync_skipped", "collection": QDRANT_COLLECTION, "count": qdrant_count},
        )
        return 0

    logger.info(
        "sync_start",
        extra={"event": "sync_start", "mongo_count": mongo_count, "qdrant_count": qdrant_count},
    )

    sparse_model = SparseTextEmbedding(model_name="Qdrant/bm25")

    cursor = db["processed_podia_books"].find(
        {"matn_embedding": {"$exists": True}},
        _PROJECTION,
    )

    batch: list[dict] = []
    total_upserted = 0

    async def _flush(docs: list[dict]) -> None:
        nonlocal total_upserted
        texts = [d.get("matn_text_plain") or "" for d in docs]
        sparse_results = list(sparse_model.embed(texts))

        points = []
        for doc, sparse in zip(docs, sparse_results):
            point_id = doc["hadith_indices"][0] if doc.get("hadith_indices") else None
            if point_id is None:
                continue
            rawi_ids = [
                n["rawi_id"]
                for n in doc.get("narrators", [])
                if n.get("rawi_id") is not None
            ]
            points.append(
                qmodels.PointStruct(
                    id=point_id,
                    vector={
                        "dense": doc["matn_embedding"],
                        "text-sparse": qmodels.SparseVector(
                            indices=sparse.indices.tolist(),
                            values=sparse.values.tolist(),
                        ),
                    },
                    payload={
                        # QdrantVectorStore reads page_content from content_payload_key
                        "matn_text_plain": doc.get("matn_text_plain", ""),
                        # QdrantVectorStore reads metadata from metadata_payload_key (default "metadata")
                        "metadata": {
                            "_id": str(point_id),
                            "hadith_url": doc.get("hadith_url", ""),
                            "book": doc.get("book", ""),
                            "chapter": doc.get("chapter", ""),
                            "matn_text": doc.get("matn_text", ""),
                            "topics": doc.get("topics", []),
                            "title": doc.get("title", ""),
                            "rawi_ids": rawi_ids,
                        },
                    },
                )
            )

        if points:
            client.upsert(collection_name=QDRANT_COLLECTION, points=points)
            total_upserted += len(points)

    async for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            await _flush(batch)
            logger.info("sync_progress", extra={"event": "sync_progress", "upserted": total_upserted})
            batch = []

    if batch:
        await _flush(batch)

    logger.info(
        "sync_done",
        extra={"event": "sync_done", "collection": QDRANT_COLLECTION, "total_upserted": total_upserted},
    )
    return total_upserted
