import sys
import uuid
from contextlib import ExitStack
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import app.chatbot.quota as _quota_module
from app.auth.models import User
from app.chatbot.quota import check_quota

# Capture the database module that check_quota actually uses.
# test_chatbot_sessions.py purges sys.modules["app.*"] between tests, which
# can cause later imports to get a fresh module object. By capturing it once
# here at collection time (before any purge) we always patch the right object.
_quota_db = _quota_module.database


def _make_user(tier: str = "free") -> User:
    return User(
        id=uuid.uuid4(),
        email="quota@example.com",
        hashed_password="$2b$12$fakehash",
        is_active=True,
        is_superuser=False,
        is_verified=False,
        tier=tier,
    )


class _quota_patches:
    """Context manager patching the database layer for quota tests.

    Patches _client on the exact database module object that quota.py holds a
    reference to, so get_client() never returns None regardless of sys.modules
    state left by tests that purge app.* modules.
    """

    def __init__(self, collection):
        self._collection = collection
        self._stack = ExitStack()

    def __enter__(self):
        self._stack.enter_context(patch.object(_quota_db, "_client", MagicMock()))
        self._stack.enter_context(patch.object(_quota_db, "get_db", return_value=MagicMock()))
        self._stack.enter_context(patch.object(_quota_db, "get_user_quotas_collection", return_value=self._collection))
        return self

    def __exit__(self, *args):
        self._stack.close()


@pytest.mark.asyncio
async def test_check_quota_allows_first_request():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(return_value={"request_count": 1})

    with _quota_patches(collection):
        await check_quota(_make_user("free"))


@pytest.mark.asyncio
async def test_check_quota_allows_request_at_limit():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(return_value={"request_count": 3})

    with _quota_patches(collection):
        await check_quota(_make_user("free"))


@pytest.mark.asyncio
async def test_check_quota_rejects_request_over_limit():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(return_value={"request_count": 4})

    with _quota_patches(collection):
        with pytest.raises(HTTPException) as exc_info:
            await check_quota(_make_user("free"))

    exc = exc_info.value
    assert getattr(exc, "status_code", None) == 429
    assert exc.detail["upgrade_hint"] == "supporter"
    assert exc.detail["ar"]


@pytest.mark.asyncio
async def test_check_quota_skips_unlimited_tier():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock()

    with _quota_patches(collection):
        await check_quota(_make_user("unlimited"))

    collection.find_one_and_update.assert_not_called()


# ---------------------------------------------------------------------------
# GET /api/v2/chat/quota endpoint tests
# ---------------------------------------------------------------------------

_BASE_DB_PATCHES = [
    ("app.database.connect", dict(new_callable=AsyncMock)),
    ("app.database.disconnect", dict(new_callable=AsyncMock)),
    ("app.database.validate_connection", dict(new_callable=AsyncMock)),
    ("app.database.get_client", dict(return_value=MagicMock())),
    ("app.database.get_db", dict(return_value=MagicMock())),
    ("app.database.get_hadiths_collection", dict(return_value=MagicMock())),
    ("app.database.get_narrators_collection", dict(return_value=MagicMock())),
    ("app.database.get_podia_hadiths_collection", dict(return_value=MagicMock())),
    ("app.database.get_podia_narrators_collection", dict(return_value=MagicMock())),
    ("app.database.get_auth_users_collection", dict(return_value=MagicMock())),
    ("app.database.get_bookmarks_collection", dict(return_value=MagicMock())),
]


from contextlib import asynccontextmanager
from httpx import ASGITransport, AsyncClient


@asynccontextmanager
async def _quota_client(user=None, quota_doc=None):
    """Async context manager: app client with DB mocked and optional auth override."""
    for key in list(sys.modules.keys()):
        if key.startswith("app"):
            del sys.modules[key]

    quota_col = MagicMock()
    quota_col.find_one = AsyncMock(return_value=quota_doc)

    patches = [patch(target, **kwargs) for target, kwargs in _BASE_DB_PATCHES]
    patches.append(patch("app.database.get_user_quotas_collection", return_value=quota_col))

    for p in patches:
        p.start()

    try:
        from app.auth.config import current_active_user
        from app.main import app

        if user is not None:
            app.dependency_overrides[current_active_user] = lambda: user

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

        app.dependency_overrides.clear()
    finally:
        for p in reversed(patches):
            p.stop()


@pytest.mark.asyncio
async def test_quota_status_no_usage():
    """Fresh user with no usage today — used=0, remaining=3."""
    async with _quota_client(user=_make_user("free"), quota_doc=None) as c:
        resp = await c.get("/api/v2/chat/quota")

    assert resp.status_code == 200
    data = resp.json()
    assert data["tier"] == "free"
    assert data["limit"] == 3
    assert data["used"] == 0
    assert data["remaining"] == 3
    assert "resets_at" in data


@pytest.mark.asyncio
async def test_quota_status_after_requests():
    """User has made 2 requests — used=2, remaining=1."""
    async with _quota_client(user=_make_user("free"), quota_doc={"request_count": 2}) as c:
        resp = await c.get("/api/v2/chat/quota")

    assert resp.status_code == 200
    data = resp.json()
    assert data["used"] == 2
    assert data["remaining"] == 1


@pytest.mark.asyncio
async def test_quota_status_unlimited():
    """Unlimited tier — limit=-1, remaining=-1."""
    async with _quota_client(user=_make_user("unlimited"), quota_doc=None) as c:
        resp = await c.get("/api/v2/chat/quota")

    assert resp.status_code == 200
    data = resp.json()
    assert data["tier"] == "unlimited"
    assert data["limit"] == -1
    assert data["remaining"] == -1


@pytest.mark.asyncio
async def test_quota_status_unauthenticated():
    """No auth cookie — 401."""
    async with _quota_client(user=None) as c:
        resp = await c.get("/api/v2/chat/quota")

    assert resp.status_code == 401
