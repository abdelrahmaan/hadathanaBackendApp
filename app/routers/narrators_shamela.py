import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import ValidationError

from ..database import get_client, get_db, get_narrators_collection, get_narrator_stats_collection
from ..models.narrator import Narrator, NarratorStats, PaginatedNarrators

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/narrators", tags=["narrators-shamela"])


def _doc_to_narrator(doc: dict) -> Narrator:
    doc["id"] = str(doc.pop("_id"))
    return Narrator(**doc)


@router.get("", response_model=PaginatedNarrators)
async def list_narrators(
    name_plain: str | None = Query(default=None),
    kunya: str | None = Query(default=None),
    nasab: str | None = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
):
    query_filter: dict = {}
    if name_plain:
        query_filter["name_plain"] = {"$regex": name_plain, "$options": "i"}
    if kunya:
        query_filter["kunya"] = {"$regex": kunya, "$options": "i"}
    if nasab:
        query_filter["nasab"] = {"$regex": nasab, "$options": "i"}

    db = get_db(get_client())
    collection = get_narrators_collection(db)

    cursor = collection.find(query_filter).skip(skip).limit(limit)
    total = await collection.count_documents(query_filter)

    active_filters = {k: v for k, v in {
        "name_plain": name_plain,
        "kunya": kunya,
        "nasab": nasab,
    }.items() if v is not None}
    logger.info("search", extra={
        "event": "search",
        "endpoint": "/api/v1/narrators",
        "filters": active_filters,
        "skip": skip,
        "limit": limit,
        "total": total,
    })

    items = []
    async for doc in cursor:
        doc_id = doc.get("_id")
        try:
            items.append(_doc_to_narrator(doc))
        except (ValidationError, Exception) as e:
            logger.warning("Skipping malformed narrator doc _id=%s: %s", doc_id, e)

    return PaginatedNarrators(items=items, total=total)


@router.get("/{narrator_id}", response_model=Narrator)
async def get_narrator(narrator_id: int):
    db = get_db(get_client())
    collection = get_narrators_collection(db)

    doc = await collection.find_one({"$or": [{"narrator_id": narrator_id}, {"narrator_id": str(narrator_id)}]})

    if not doc:
        raise HTTPException(status_code=404, detail="Narrator not found.")

    try:
        return _doc_to_narrator(doc)
    except (ValidationError, Exception) as e:
        logger.error("Failed to parse narrator id=%s: %s", narrator_id, e)
        raise HTTPException(status_code=500, detail="Narrator data is malformed.")


@router.get("/{narrator_id}/stats", response_model=NarratorStats)
async def get_narrator_stats(narrator_id: int):
    db = get_db(get_client())
    collection = get_narrator_stats_collection(db)

    doc = await collection.find_one({"narrator_id": narrator_id})

    if not doc:
        raise HTTPException(status_code=404, detail="Narrator stats not found.")

    try:
        doc.pop("_id", None)
        return NarratorStats(**doc)
    except (ValidationError, Exception) as e:
        logger.error("Failed to parse narrator stats id=%s: %s", narrator_id, e)
        raise HTTPException(status_code=500, detail="Narrator stats data is malformed.")
