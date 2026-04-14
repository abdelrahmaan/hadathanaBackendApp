import logging

from fastapi import APIRouter, Query
from pydantic import ValidationError

from ..database import get_client, get_db, get_podia_hadiths_collection
from ..models.hadith_podia import (
    PaginatedPodiaHadiths,
    PodiaHadith,
    TopicCount,
    TopicsResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["search-podia"])


def _doc_to_hadith(doc: dict) -> PodiaHadith:
    doc["id"] = str(doc.pop("_id"))
    return PodiaHadith(**doc)


@router.get("/topics", response_model=TopicsResponse)
async def list_topics():
    """
    Return all distinct topics with their hadith counts, sorted by count descending.
    """
    db = get_db(get_client())
    collection = get_podia_hadiths_collection(db)

    pipeline = [
        {"$unwind": "$topics"},
        {"$group": {"_id": "$topics", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$project": {"_id": 0, "topic": "$_id", "count": 1}},
    ]

    items = []
    async for doc in collection.aggregate(pipeline):
        items.append(TopicCount(topic=doc["topic"], count=doc["count"]))

    logger.info("topics_list", extra={"event": "topics_list", "total": len(items)})
    return TopicsResponse(items=items, total=len(items))


@router.get("/topics/{topic}/hadiths", response_model=PaginatedPodiaHadiths)
async def hadiths_by_topic(
    topic: str,
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
):
    """
    Return all hadiths tagged with a specific topic.
    Topic must be an exact match (case-sensitive Arabic string).
    """
    db = get_db(get_client())
    collection = get_podia_hadiths_collection(db)

    query_filter = {"topics": topic}

    cursor = collection.find(query_filter).skip(skip).limit(limit)
    total = await collection.count_documents(query_filter)

    logger.info("topic_hadiths", extra={
        "event": "topic_hadiths",
        "topic": topic,
        "skip": skip,
        "limit": limit,
        "total": total,
    })

    items = []
    async for doc in cursor:
        doc_id = doc.get("_id")
        try:
            items.append(_doc_to_hadith(doc))
        except (ValidationError, Exception) as e:
            logger.warning("Skipping malformed doc _id=%s: %s", doc_id, e)

    return PaginatedPodiaHadiths(items=items, total=total)
