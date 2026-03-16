from fastapi import APIRouter, HTTPException, Query

from ..database import get_client, get_db, get_podia_hadiths_collection
from ..models.hadith_podia import PodiaHadith, PaginatedPodiaHadiths

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
    items = [_doc_to_hadith(doc) async for doc in cursor]

    return PaginatedPodiaHadiths(items=items, total=total)


@router.get("/{hadith_index}", response_model=PodiaHadith)
async def get_hadith(hadith_index: int):
    db = get_db(get_client())
    collection = get_podia_hadiths_collection(db)

    doc = await collection.find_one({"hadith_indices": hadith_index})

    if not doc:
        raise HTTPException(status_code=404, detail="Hadith not found.")

    return _doc_to_hadith(doc)
