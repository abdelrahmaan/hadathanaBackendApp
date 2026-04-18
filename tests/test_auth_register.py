"""Tests for POST /auth/register — success and validation cases."""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


def _make_user_collection(find_one_return=None):
    """Return a mock Motor collection for auth_users."""
    col = MagicMock()
    col.find_one = AsyncMock(return_value=find_one_return)
    col.insert_one = AsyncMock(return_value=MagicMock(inserted_id="mock_id"))
    col.find.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    col.count_documents = AsyncMock(return_value=0)
    col.delete_one = AsyncMock(return_value=MagicMock(deleted_count=1))
    return col


def _make_other_collection():
    col = MagicMock()
    col.find.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    col.count_documents = AsyncMock(return_value=0)
    col.find_one = AsyncMock(return_value=None)
    col.insert_one = AsyncMock(return_value=MagicMock(inserted_id="mock_id"))
    col.delete_one = AsyncMock(return_value=MagicMock(deleted_count=1))
    return col


@pytest_asyncio.fixture
async def register_client():
    """Client with get_user_db overridden to use a mock collection."""
    # Remove cached app module so dependency_overrides take effect cleanly.
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    user_col = _make_user_collection(find_one_return=None)  # email not taken
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

        # Override get_user_db to yield a MotorUserDatabase backed by our mock collection.
        async def override_get_user_db():
            yield MotorUserDatabase(user_col)

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_register_success(register_client):
    """POST /auth/register with valid credentials returns 201 and no hashed_password."""
    response = await register_client.post(
        "/auth/register",
        json={"email": "test@example.com", "password": "strongpassword123"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["email"] == "test@example.com"
    assert "hashed_password" not in data
    assert "id" in data


@pytest.mark.asyncio
async def test_register_invalid_email(register_client):
    """POST /auth/register with a malformed email returns 422 (validation error)."""
    response = await register_client.post(
        "/auth/register",
        json={"email": "not-an-email", "password": "strongpassword123"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_register_missing_password(register_client):
    """POST /auth/register with missing password field returns 422."""
    response = await register_client.post(
        "/auth/register",
        json={"email": "test@example.com"},
    )
    assert response.status_code == 422
