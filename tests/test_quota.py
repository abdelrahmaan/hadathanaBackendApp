import uuid
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from app.auth.models import User
from app.chatbot.quota import check_quota
import app.chatbot.quota as _quota_module

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
