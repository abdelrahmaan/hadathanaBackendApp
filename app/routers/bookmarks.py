"""Authenticated bookmarks router.

Endpoints:
    GET    /api/v1/bookmarks         — list the authenticated user's bookmarks (paginated)
    POST   /api/v1/bookmarks         — add a new bookmark
    DELETE /api/v1/bookmarks/{hadith_url} — remove a bookmark
"""

import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from ..auth.config import current_active_user
from ..auth.models import User
from ..database import get_bookmarks_collection, get_client, get_db

logger = logging.getLogger("hadathana.bookmarks")

router = APIRouter(prefix="/api/v1/bookmarks", tags=["bookmarks"])


# ------------------------------------------------------------------
# Pydantic models
# ------------------------------------------------------------------


class BookmarkCreate(BaseModel):
    hadith_url: str
    source: str  # "shamela" or "podia"


class BookmarkRead(BaseModel):
    hadith_url: str
    source: str
    created_at: datetime


class PaginatedBookmarks(BaseModel):
    items: list[BookmarkRead]
    total: int


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.get("", response_model=PaginatedBookmarks)
async def list_bookmarks(
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    user: User = Depends(current_active_user),
) -> PaginatedBookmarks:
    """Return the authenticated user's bookmarks, newest first."""
    db = get_db(get_client())
    col = get_bookmarks_collection(db)
    query = {"user_id": str(user.id)}
    total = await col.count_documents(query)
    cursor = col.find(query).skip(skip).limit(limit).sort("created_at", -1)
    items: list[BookmarkRead] = []
    async for doc in cursor:
        items.append(
            BookmarkRead(
                hadith_url=doc["hadith_url"],
                source=doc["source"],
                created_at=doc["created_at"],
            )
        )
    return PaginatedBookmarks(items=items, total=total)


@router.post("", response_model=BookmarkRead, status_code=201)
async def add_bookmark(
    body: BookmarkCreate,
    user: User = Depends(current_active_user),
) -> BookmarkRead:
    """Add a hadith bookmark for the authenticated user."""
    db = get_db(get_client())
    col = get_bookmarks_collection(db)
    existing = await col.find_one(
        {"user_id": str(user.id), "hadith_url": body.hadith_url}
    )
    if existing:
        raise HTTPException(status_code=409, detail="Bookmark already exists.")
    doc = {
        "user_id": str(user.id),
        "hadith_url": body.hadith_url,
        "source": body.source,
        "created_at": datetime.now(timezone.utc),
    }
    await col.insert_one(doc)
    logger.info(
        "bookmark_added",
        extra={"user_id": str(user.id), "hadith_url": body.hadith_url},
    )
    return BookmarkRead(
        hadith_url=doc["hadith_url"],
        source=doc["source"],
        created_at=doc["created_at"],
    )


@router.delete("/{hadith_url:path}", status_code=204)
async def remove_bookmark(
    hadith_url: str,
    user: User = Depends(current_active_user),
) -> None:
    """Remove a bookmark for the authenticated user."""
    db = get_db(get_client())
    col = get_bookmarks_collection(db)
    result = await col.delete_one(
        {"user_id": str(user.id), "hadith_url": hadith_url}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Bookmark not found.")
    logger.info(
        "bookmark_removed",
        extra={"user_id": str(user.id), "hadith_url": hadith_url},
    )
