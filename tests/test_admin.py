"""Unit tests for GET /api/v2/admin/stats."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import make_mock_collection


def make_superuser():
    user = MagicMock()
    user.is_active = True
    user.is_superuser = True
    return user


def make_regular_user():
    user = MagicMock()
    user.is_active = True
    user.is_superuser = False
    return user


@pytest_asyncio.fixture
async def admin_client():
    """AsyncClient with all collections mocked — no live DB or Qdrant needed."""
    mock_col = make_mock_collection()
    mock_db = MagicMock()
    mock_client_obj = MagicMock()

    # MongoDB aggregation returns empty cursor by default
    mock_col.aggregate.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    mock_col.distinct = AsyncMock(return_value=[])

    with (
        patch("app.database.connect", new_callable=AsyncMock),
        patch("app.database.disconnect", new_callable=AsyncMock),
        patch("app.database.validate_connection", new_callable=AsyncMock),
        patch("app.database.get_client", return_value=mock_client_obj),
        patch("app.database.get_db", return_value=mock_db),
        patch("app.database.get_hadiths_collection", return_value=mock_col),
        patch("app.database.get_narrators_collection", return_value=mock_col),
        patch("app.database.get_podia_hadiths_collection", return_value=mock_col),
        patch("app.database.get_podia_narrators_collection", return_value=mock_col),
        patch("app.database.get_auth_users_collection", return_value=mock_col),
        patch("app.database.get_bookmarks_collection", return_value=mock_col),
        patch("app.database.get_user_quotas_collection", return_value=mock_col),
        patch("app.database.get_chat_sessions_collection", return_value=mock_col),
        patch("app.chatbot.qdrant.get_qdrant_client", return_value=None),
    ):
        from app.main import app
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c


@pytest.mark.asyncio
async def test_admin_stats_requires_auth(admin_client):
    """Unauthenticated request returns 401."""
    response = await admin_client.get("/api/v2/admin/stats")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_admin_stats_forbids_regular_user(admin_client):
    """Regular (non-superuser) user gets 403."""
    from app.auth.config import current_active_user
    from app.main import app

    app.dependency_overrides[current_active_user] = lambda: make_regular_user()
    try:
        response = await admin_client.get("/api/v2/admin/stats")
        assert response.status_code == 403
    finally:
        app.dependency_overrides.pop(current_active_user, None)


@pytest.mark.asyncio
async def test_admin_stats_returns_200_for_superuser(admin_client):
    """Superuser gets 200 with correct response shape."""
    from app.auth.config import current_active_user
    from app.main import app

    app.dependency_overrides[current_active_user] = lambda: make_superuser()
    try:
        response = await admin_client.get("/api/v2/admin/stats")
        assert response.status_code == 200
        data = response.json()
        assert "system" in data
        assert "users" in data
        assert "chatbot" in data
        assert "data" in data
        # system section
        assert data["system"]["mongodb"] in ("connected", "disconnected")
        assert data["system"]["qdrant"] in ("connected", "disconnected")
        assert isinstance(data["system"]["chatbot_enabled"], bool)
        # users section
        assert isinstance(data["users"]["total"], int)
        assert "by_tier" in data["users"]
        assert isinstance(data["users"]["quota_used_today"], int)
        assert isinstance(data["users"]["users_at_limit_today"], int)
        # chatbot section
        assert isinstance(data["chatbot"]["total_sessions"], int)
        assert isinstance(data["chatbot"]["avg_messages_per_session"], float)
        # data section
        assert isinstance(data["data"]["podia_hadiths"], int)
        assert isinstance(data["data"]["qdrant_points"], int)
    finally:
        app.dependency_overrides.pop(current_active_user, None)


@pytest.mark.asyncio
async def test_admin_stats_shape_with_data(admin_client):
    """Response counts match mocked collection data."""
    from app.auth.config import current_active_user
    from app.main import app

    # Patch podia hadiths collection at the database module level
    mock_podia = make_mock_collection()
    mock_podia.count_documents = AsyncMock(return_value=7076)
    mock_podia.aggregate.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    mock_podia.distinct = AsyncMock(return_value=["الصلاة", "الصوم"])

    app.dependency_overrides[current_active_user] = lambda: make_superuser()
    with patch("app.database.get_podia_hadiths_collection", return_value=mock_podia):
        try:
            response = await admin_client.get("/api/v2/admin/stats")
            assert response.status_code == 200
            data = response.json()
            assert data["data"]["podia_hadiths"] == 7076
            assert data["data"]["topics"] == 2
        finally:
            app.dependency_overrides.pop(current_active_user, None)
