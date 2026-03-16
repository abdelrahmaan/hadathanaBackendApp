from fastapi import APIRouter, HTTPException, Query

from ..database import get_client, get_db, get_podia_narrators_collection, get_podia_narrator_stats_collection, get_podia_narrators_tarajem_collection
from ..models.narrator_podia import PodiaNarrator, PodiaNarratorStats, PodiaNarratorTarajem, PaginatedPodiaNarrators

router = APIRouter(prefix="/api/v2/narrators", tags=["narrators-podia"])


def _doc_to_narrator(doc: dict) -> PodiaNarrator:
    doc["id"] = str(doc.pop("_id"))
    return PodiaNarrator(**doc)


@router.get("", response_model=PaginatedPodiaNarrators)
async def list_narrators(
    full_name_plain: str | None = Query(default=None),
    rank: str | None = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
):
    query_filter: dict = {}
    if full_name_plain:
        query_filter["full_name_plain"] = {"$regex": full_name_plain, "$options": "i"}
    if rank:
        query_filter["rank"] = {"$regex": rank, "$options": "i"}

    db = get_db(get_client())
    collection = get_podia_narrators_collection(db)

    cursor = collection.find(query_filter).skip(skip).limit(limit)
    total = await collection.count_documents(query_filter)
    items = [_doc_to_narrator(doc) async for doc in cursor]

    return PaginatedPodiaNarrators(items=items, total=total)


@router.get("/{rawi_id}", response_model=PodiaNarrator)
async def get_narrator(rawi_id: int):
    db = get_db(get_client())
    collection = get_podia_narrators_collection(db)

    doc = await collection.find_one({"rawi_id": rawi_id})

    if not doc:
        raise HTTPException(status_code=404, detail="Narrator not found.")

    return _doc_to_narrator(doc)


@router.get("/{rawi_id}/tarajem", response_model=PodiaNarratorTarajem)
async def get_narrator_tarajem(rawi_id: int):
    db = get_db(get_client())
    collection = get_podia_narrators_tarajem_collection(db)

    doc = await collection.find_one({"rawi_id": rawi_id})

    if not doc:
        raise HTTPException(status_code=404, detail="Narrator tarajem not found.")

    doc["id"] = str(doc.pop("_id"))
    return PodiaNarratorTarajem(**doc)


@router.get("/{rawi_id}/stats", response_model=PodiaNarratorStats)
async def get_narrator_stats(rawi_id: int):
    db = get_db(get_client())
    collection = get_podia_narrator_stats_collection(db)

    doc = await collection.find_one({"rawi_id": rawi_id})

    if not doc:
        raise HTTPException(status_code=404, detail="Narrator stats not found.")

    doc.pop("_id", None)
    return PodiaNarratorStats(**doc)
