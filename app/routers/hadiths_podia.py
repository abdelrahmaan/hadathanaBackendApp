import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import ValidationError

from ..database import get_client, get_db, get_podia_hadiths_collection
from ..models.hadith_podia import PodiaHadith, PaginatedPodiaHadiths

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/hadiths", tags=["hadiths-podia"])


def _doc_to_hadith(doc: dict) -> PodiaHadith:
    doc["id"] = str(doc.pop("_id"))
    return PodiaHadith(**doc)


@router.get("", response_model=PaginatedPodiaHadiths)
async def list_hadiths(
    hadith_text_plain: str | None = Query(default=None),
    rawi_id: int | None = Query(default=None),
    book: str | None = Query(default=None),
    hadith_index: int | None = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
):
    query_filter: dict = {}
    if hadith_text_plain:
        query_filter["hadith_text_plain"] = {"$regex": hadith_text_plain, "$options": "i"}
    if rawi_id is not None:
        query_filter["narrators.rawi_id"] = rawi_id
    if book:
        query_filter["book"] = {"$regex": book, "$options": "i"}
    if hadith_index is not None:
        query_filter["hadith_indices"] = hadith_index

    db = get_db(get_client())
    collection = get_podia_hadiths_collection(db)

    cursor = collection.find(query_filter).skip(skip).limit(limit)
    total = await collection.count_documents(query_filter)

    active_filters = {k: v for k, v in {
        "hadith_text_plain": hadith_text_plain,
        "rawi_id": rawi_id,
        "book": book,
        "hadith_index": hadith_index,
    }.items() if v is not None}
    logger.info("search", extra={
        "event": "search",
        "endpoint": "/api/v2/hadiths",
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

    return PaginatedPodiaHadiths(items=items, total=total)


@router.get("/{hadith_index}", response_model=PodiaHadith)
async def get_hadith(hadith_index: int):
    db = get_db(get_client())
    collection = get_podia_hadiths_collection(db)

    doc = await collection.find_one({"hadith_indices": hadith_index})

    if not doc:
        raise HTTPException(status_code=404, detail="Hadith not found.")

    try:
        return _doc_to_hadith(doc)
    except (ValidationError, Exception) as e:
        logger.error("Failed to parse hadith index=%s: %s", hadith_index, e)
        raise HTTPException(status_code=500, detail="Hadith data is malformed.")
