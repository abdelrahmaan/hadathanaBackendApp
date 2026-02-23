from fastapi import APIRouter, HTTPException, Query

from ..database import get_client, get_db, get_narrators_collection
from ..models.narrator import Narrator, PaginatedNarrators

router = APIRouter(prefix="/api/v1/narrators", tags=["narrators"])


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

    client = get_client()
    db = get_db(client)
    collection = get_narrators_collection(db)

    cursor = collection.find(query_filter).skip(skip).limit(limit)
    total = await collection.count_documents(query_filter)
    items = [_doc_to_narrator(doc) async for doc in cursor]

    client.close()
    return PaginatedNarrators(items=items, total=total)


@router.get("/{narrator_id}", response_model=Narrator)
async def get_narrator(narrator_id: int):
    client = get_client()
    db = get_db(client)
    collection = get_narrators_collection(db)

    doc = await collection.find_one({"$or": [{"narrator_id": narrator_id}, {"narrator_id": str(narrator_id)}]})
    client.close()

    if not doc:
        raise HTTPException(status_code=404, detail="Narrator not found.")

    return _doc_to_narrator(doc)
