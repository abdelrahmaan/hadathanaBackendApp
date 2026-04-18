"""Tests for POST /auth/reset-password.

An invalid or expired token must return 400.
"""

import sys
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


@pytest_asyncio.fixture
async def reset_pw_client():
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    user_col = MagicMock()
    user_col.find_one = AsyncMock(return_value=None)
    user_col.insert_one = AsyncMock(return_value=MagicMock(inserted_id="mock_id"))
    user_col.find.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    user_col.count_documents = AsyncMock(return_value=0)
    user_col.delete_one = AsyncMock(return_value=MagicMock(deleted_count=0))
    other_col = _make_other_collection()

    with patch("app.database.connect", new_callable=AsyncMock), \
         patch("app.database.disconnect", new_callable=AsyncMock), \
         patch("app.database.validate_connection", new_callable=AsyncMock), \
         patch("app.database.get_client", return_value=MagicMock()), \
         patch("app.database.get_db", return_value=MagicMock()), \
         patch("app.database.get_hadiths_collection", return_value=other_col), \
         patch("app.database.get_narrators_collection", return_value=other_col), \
         patch("app.database.get_podia_hadiths_collection", return_value=other_col), \
         patch("app.database.get_podia_narrators_collection", return_value=other_col), \
         patch("app.database.get_auth_users_collection", return_value=user_col), \
         patch("app.database.get_bookmarks_collection", return_value=other_col):

        from app.auth.database import MotorUserDatabase, get_user_db
        from app.main import app

        async def override_get_user_db():
            yield MotorUserDatabase(user_col)

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_reset_password_invalid_token_returns_400(reset_pw_client):
    """POST /auth/reset-password with a bogus token returns 400."""
    response = await reset_pw_client.post(
        "/auth/reset-password",
        json={"token": "invalid.token.value", "password": "newpassword123"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_reset_password_missing_token_returns_422(reset_pw_client):
    """POST /auth/reset-password with missing token field returns 422."""
    response = await reset_pw_client.post(
        "/auth/reset-password",
        json={"password": "newpassword123"},
    )
    assert response.status_code == 422
