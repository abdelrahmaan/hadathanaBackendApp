from fastapi import APIRouter, HTTPException, Query

from ..database import get_client, get_db, get_hadiths_collection
from ..config import settings
from ..models.hadith import Hadith, PaginatedHadiths

router = APIRouter(prefix="/api/v1/hadiths", tags=["hadiths"])


def _doc_to_hadith(doc: dict) -> Hadith:
    doc["id"] = str(doc.pop("_id"))
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
        query_filter["unique_narrators.narrator_id"] = narrator_id
    if chain_type:
        query_filter["chains.type"] = chain_type

    client = get_client()
    db = get_db(client)
    collection = get_hadiths_collection(db)

    cursor = collection.find(query_filter).skip(skip).limit(limit)
    total = await collection.count_documents(query_filter)
    items = [_doc_to_hadith(doc) async for doc in cursor]

    client.close()
    return PaginatedHadiths(items=items, total=total)


@router.get("/{hadith_id}", response_model=Hadith)
async def get_hadith(hadith_id: int):
    client = get_client()
    db = get_db(client)
    collection = get_hadiths_collection(db)

    doc = await collection.find_one({"hadith_index": hadith_id})
    client.close()

    if not doc:
        raise HTTPException(status_code=404, detail="Hadith not found.")

    return _doc_to_hadith(doc)
