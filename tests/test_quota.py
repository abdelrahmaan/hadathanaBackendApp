import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from app.auth.models import User
from app.chatbot.quota import check_quota


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


@pytest.mark.asyncio
async def test_check_quota_allows_first_request():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(return_value={"request_count": 1})

    with (
        patch("app.chatbot.quota.database.get_client", return_value=MagicMock()),
        patch("app.chatbot.quota.database.get_db", return_value=MagicMock()),
        patch("app.chatbot.quota.database.get_user_quotas_collection", return_value=collection),
    ):
        await check_quota(_make_user("free"))


@pytest.mark.asyncio
async def test_check_quota_allows_request_at_limit():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(return_value={"request_count": 3})

    with (
        patch("app.chatbot.quota.database.get_client", return_value=MagicMock()),
        patch("app.chatbot.quota.database.get_db", return_value=MagicMock()),
        patch("app.chatbot.quota.database.get_user_quotas_collection", return_value=collection),
    ):
        await check_quota(_make_user("free"))


@pytest.mark.asyncio
async def test_check_quota_rejects_request_over_limit():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(return_value={"request_count": 4})

    with (
        patch("app.chatbot.quota.database.get_client", return_value=MagicMock()),
        patch("app.chatbot.quota.database.get_db", return_value=MagicMock()),
        patch("app.chatbot.quota.database.get_user_quotas_collection", return_value=collection),
    ):
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

    with (
        patch("app.chatbot.quota.database.get_client", return_value=MagicMock()),
        patch("app.chatbot.quota.database.get_db", return_value=MagicMock()),
        patch("app.chatbot.quota.database.get_user_quotas_collection", return_value=collection),
    ):
        await check_quota(_make_user("unlimited"))

    collection.find_one_and_update.assert_not_called()
