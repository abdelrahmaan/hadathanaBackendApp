import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import ValidationError

from ..database import get_client, get_db, get_hadiths_collection
from ..models.hadith import Hadith, PaginatedHadiths

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/hadiths", tags=["hadiths-shamela"])


def _normalize_narrator_id(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _doc_to_hadith(doc: dict) -> Hadith:
    doc["id"] = str(doc.pop("_id"))
    for n in doc.get("unique_narrators", []):
        n["narrator_id"] = _normalize_narrator_id(n.get("narrator_id"))
    for chain in doc.get("chains", []):
        for n in chain.get("narrators", []):
            n["narrator_id"] = _normalize_narrator_id(n.get("narrator_id"))
    return Hadith(**doc)


@router.get("", response_model=PaginatedHadiths)
async def list_hadiths(
    hadith_plain: str | None = Query(default=None),
    narrator_id: int | None = Query(default=None),
    chain_type: str | None = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
):
    query_filter: dict = {}
    if hadith_plain:
        query_filter["hadith_plain"] = {"$regex": hadith_plain, "$options": "i"}
    if narrator_id is not None:
        query_filter["unique_narrators.narrator_id"] = {"$in": [narrator_id, str(narrator_id)]}
    if chain_type:
        query_filter["chains.type"] = chain_type

    db = get_db(get_client())
    collection = get_hadiths_collection(db)

    cursor = collection.find(query_filter).skip(skip).limit(limit)
    total = await collection.count_documents(query_filter)

    active_filters = {k: v for k, v in {
        "hadith_plain": hadith_plain,
        "narrator_id": narrator_id,
        "chain_type": chain_type,
    }.items() if v is not None}
    logger.info("search", extra={
        "event": "search",
        "endpoint": "/api/v1/hadiths",
        "filters": active_filters,
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
            logger.warning("Skipping malformed hadith doc _id=%s: %s", doc_id, e)

    return PaginatedHadiths(items=items, total=total)


@router.get("/{hadith_id}", response_model=Hadith)
async def get_hadith(hadith_id: int):
    db = get_db(get_client())
    collection = get_hadiths_collection(db)

    doc = await collection.find_one({"hadith_index": hadith_id})

    if not doc:
        raise HTTPException(status_code=404, detail="Hadith not found.")

    try:
        return _doc_to_hadith(doc)
    except (ValidationError, Exception) as e:
        logger.error("Failed to parse hadith id=%s: %s", hadith_id, e)
        raise HTTPException(status_code=500, detail="Hadith data is malformed.")
