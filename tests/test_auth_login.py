"""Tests for POST /auth/login — unknown user and wrong password return 400.

fastapi-users uses OAuth2PasswordRequestForm, so the form fields are
`username` (mapped to email) and `password`.
"""

import sys
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


def _make_user_collection(find_one_return=None):
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


def _patches(user_col, other_col):
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
        patch("app.database.get_bookmarks_collection", return_value=other_col),
    ]


@pytest_asyncio.fixture
async def login_client_unknown_user():
    """Client where get_user_db returns None for any find_one (user not found)."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    user_col = _make_user_collection(find_one_return=None)
    other_col = _make_other_collection()

    stack = _patches(user_col, other_col)
    for p in stack:
        p.start()

    try:
        from app.auth.database import MotorUserDatabase, get_user_db
        from app.main import app

        async def override_get_user_db():
            yield MotorUserDatabase(user_col)

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

        app.dependency_overrides.clear()
    finally:
        for p in reversed(stack):
            p.stop()


@pytest_asyncio.fixture
async def login_client_known_user():
    """Client where get_user_db returns a valid user with a correct hashed password."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    from pwdlib import PasswordHash
    from pwdlib.hashers.argon2 import Argon2Hasher

    password_hash = PasswordHash([Argon2Hasher()])
    hashed = password_hash.hash("Str0ngPassword!")

    user_doc = {
        "id": str(uuid.uuid4()),
        "email": "known@example.com",
        "hashed_password": hashed,
        "is_active": True,
        "is_superuser": False,
        "is_verified": False,
    }

    user_col = _make_user_collection(find_one_return=user_doc)
    # update_one needed when fastapi-users updates last_login or similar fields
    user_col.update_one = AsyncMock(return_value=MagicMock())
    other_col = _make_other_collection()

    stack = _patches(user_col, other_col)
    for p in stack:
        p.start()

    try:
        from app.auth.database import MotorUserDatabase, get_user_db
        from app.main import app

        async def override_get_user_db():
            yield MotorUserDatabase(user_col)

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c

        app.dependency_overrides.clear()
    finally:
        for p in reversed(stack):
            p.stop()


@pytest.mark.asyncio
async def test_login_success_sets_cookie(login_client_known_user):
    """Successful login returns 200 and sets the access_token cookie."""
    response = await login_client_known_user.post(
        "/auth/login",
        data={"username": "known@example.com", "password": "Str0ngPassword!"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    # fastapi-users CookieTransport returns 204 No Content on successful login
    assert response.status_code == 204
    assert "access_token" in response.cookies


@pytest.mark.asyncio
async def test_login_unknown_email_returns_400(login_client_unknown_user):
    """Login with an email that doesn't exist returns 400 BAD_CREDENTIALS."""
    response = await login_client_unknown_user.post(
        "/auth/login",
        data={"username": "nobody@example.com", "password": "anypassword"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_login_missing_fields_returns_422(login_client_unknown_user):
    """Login with missing username/password returns 422."""
    response = await login_client_unknown_user.post(
        "/auth/login",
        data={"username": "nobody@example.com"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 422
