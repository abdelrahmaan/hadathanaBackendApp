"""Tests for /api/v2/bookmarks endpoints.

Endpoints:
    GET    /api/v2/bookmarks            — list (paginated), requires auth
    POST   /api/v2/bookmarks            — add bookmark, requires auth
    DELETE /api/v2/bookmarks/{url}      — remove bookmark, requires auth

Test cases:
    1. GET without auth → 401
    2. GET with mocked user → 200, empty list
    3. POST with mocked user, no existing → 201
    4. POST with mocked user, existing → 409
    5. DELETE with mocked user, found → 204
"""

import sys
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


def _make_other_collection():
    col = MagicMock()
    col.find.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    col.count_documents = AsyncMock(return_value=0)
    col.find_one = AsyncMock(return_value=None)
    col.insert_one = AsyncMock(return_value=MagicMock(inserted_id="mock_id"))
    col.delete_one = AsyncMock(return_value=MagicMock(deleted_count=1))
    return col


class _AsyncIter:
    """Minimal async iterator that yields nothing."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


def _make_bookmarks_collection(find_one_return=None, deleted_count=1):
    """Return a mock bookmarks collection."""
    col = MagicMock()
    col.find_one = AsyncMock(return_value=find_one_return)
    col.insert_one = AsyncMock(return_value=MagicMock(inserted_id="mock_id"))
    col.delete_one = AsyncMock(return_value=MagicMock(deleted_count=deleted_count))
    col.count_documents = AsyncMock(return_value=0)
    # find().skip().limit().sort() must return a proper async iterable
    cursor_mock = _AsyncIter()
    skip_mock = MagicMock()
    skip_mock.limit.return_value.sort.return_value = cursor_mock
    col.find.return_value.skip.return_value = skip_mock
    return col


def _make_mock_user():
    """Return a User-like object for dependency_overrides."""
    from app.auth.models import User

    return User(
        id=uuid.uuid4(),
        email="user@example.com",
        hashed_password="$2b$12$fakehash",
        is_active=True,
        is_superuser=False,
        is_verified=False,
    )


def _all_patches(user_col, bookmarks_col, other_col):
    return [
        patch("app.database.connect", new_callable=AsyncMock),
        patch("app.database.disconnect", new_callable=AsyncMock),
        patch("app.database.validate_connection", new_callable=AsyncMock),
        patch("app.database.get_client", return_value=MagicMock()),
        patch("app.database.get_db", return_value=MagicMock()),
        patch("app.database.get_hadiths_collection", return_value=other_col),
        patch("app.database.get_narrators_collection", return_value=other_col),
        patch("app.database.get_podia_hadiths_collection", return_value=other_col),
        patch("app.database.get_podia_narrators_collection", return_value=other_col),
        patch("app.database.get_auth_users_collection", return_value=user_col),
        patch("app.database.get_bookmarks_collection", return_value=bookmarks_col),
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def unauth_client():
    """Client with no auth override — current_active_user will reject requests."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    other_col = _make_other_collection()
    bm_col = _make_bookmarks_collection()

    patches = _all_patches(other_col, bm_col, other_col)
    for p in patches:
        p.start()

    try:
        from app.main import app

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
    finally:
        for p in reversed(patches):
            p.stop()


@pytest_asyncio.fixture
async def auth_client_no_bookmark():
    """Authenticated client; bookmarks collection has no existing doc (find_one→None)."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    other_col = _make_other_collection()
    bm_col = _make_bookmarks_collection(find_one_return=None, deleted_count=1)

    patches = _all_patches(other_col, bm_col, other_col)
    for p in patches:
        p.start()

    try:
        from app.auth.config import current_active_user
        from app.main import app

        mock_user = _make_mock_user()
        app.dependency_overrides[current_active_user] = lambda: mock_user

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

        app.dependency_overrides.clear()
    finally:
        for p in reversed(patches):
            p.stop()


@pytest_asyncio.fixture
async def auth_client_with_existing_bookmark():
    """Authenticated client; bookmarks collection returns an existing doc (→ 409)."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    other_col = _make_other_collection()
    existing_doc = {
        "user_id": "some-user-id",
        "hadith_url": "https://example.com/hadith/1",
        "source": "podia",
        "created_at": datetime.now(timezone.utc),
    }
    bm_col = _make_bookmarks_collection(find_one_return=existing_doc)

    patches = _all_patches(other_col, bm_col, other_col)
    for p in patches:
        p.start()

    try:
        from app.auth.config import current_active_user
        from app.main import app

        mock_user = _make_mock_user()
        app.dependency_overrides[current_active_user] = lambda: mock_user

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

        app.dependency_overrides.clear()
    finally:
        for p in reversed(patches):
            p.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_bookmarks_unauthenticated(unauth_client):
    """GET /api/v2/bookmarks without auth cookie → 401."""
    response = await unauth_client.get("/api/v2/bookmarks")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_bookmarks_authenticated(auth_client_no_bookmark):
    """GET /api/v2/bookmarks with mocked user → 200, empty list."""
    response = await auth_client_no_bookmark.get("/api/v2/bookmarks")
    assert response.status_code == 200
    data = response.json()
    assert data["items"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_add_bookmark_success(auth_client_no_bookmark):
    """POST /api/v2/bookmarks, no existing → 201 with bookmark data."""
    response = await auth_client_no_bookmark.post(
        "/api/v2/bookmarks",
        json={"hadith_url": "https://example.com/hadith/1", "source": "podia"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["hadith_url"] == "https://example.com/hadith/1"
    assert data["source"] == "podia"
    assert "created_at" in data


@pytest.mark.asyncio
async def test_add_bookmark_duplicate(auth_client_with_existing_bookmark):
    """POST /api/v2/bookmarks when bookmark already exists → 409."""
    response = await auth_client_with_existing_bookmark.post(
        "/api/v2/bookmarks",
        json={"hadith_url": "https://example.com/hadith/1", "source": "podia"},
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_delete_bookmark_success(auth_client_no_bookmark):
    """DELETE /api/v2/bookmarks/{url} with found bookmark → 204."""
    response = await auth_client_no_bookmark.delete(
        "/api/v2/bookmarks/https://example.com/hadith/1"
    )
    assert response.status_code == 204
