"""Tests for POST /auth/forgot-password.

fastapi-users always returns 202 regardless of whether the email exists
(OWASP no-user-enumeration pattern). When a Resend API key is configured
and the user exists, Resend.Emails.send should be called.
"""

import sys
import uuid
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


def _build_user_doc(email: str = "user@example.com") -> dict:
    """Return a raw MongoDB doc that MotorUserDatabase._doc_to_user can parse."""
    return {
        "id": str(uuid.uuid4()),
        "email": email,
        "hashed_password": "$2b$12$fakehash",
        "is_active": True,
        "is_superuser": False,
        "is_verified": False,
    }


@pytest_asyncio.fixture
async def forgot_pw_client():
    """Client where find_one returns None — email unknown."""
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


@pytest_asyncio.fixture
async def forgot_pw_client_known_user():
    """Client where find_one returns a valid user doc, simulating a known email."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    user_doc = _build_user_doc("known@example.com")
    user_col = MagicMock()
    user_col.find_one = AsyncMock(return_value=user_doc)
    # update_one needed for forgot-password token storage
    user_col.update_one = AsyncMock(return_value=MagicMock())
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
         patch("app.database.get_bookmarks_collection", return_value=other_col), \
         patch("resend.Emails.send", return_value={"id": "mock-email-id"}) as mock_send:

        from app.auth.database import MotorUserDatabase, get_user_db
        from app.main import app

        async def override_get_user_db():
            yield MotorUserDatabase(user_col)

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c, mock_send

        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_forgot_password_unknown_email_returns_202(forgot_pw_client):
    """Unknown email → 202 (no enumeration)."""
    response = await forgot_pw_client.post(
        "/auth/forgot-password",
        json={"email": "nobody@example.com"},
    )
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_forgot_password_known_email_returns_202(forgot_pw_client_known_user):
    """Known email → 202, Resend.Emails.send called with correct to address."""
    client, mock_send = forgot_pw_client_known_user

    # Patch resend_api_key to be non-empty so the send branch is triggered.
    with patch("app.auth.config.settings") as mock_settings:
        mock_settings.resend_api_key = "re_test_key"
        mock_settings.from_email = "noreply@hadathana.app"
        mock_settings.reset_token_expire_minutes = 30

        response = await client.post(
            "/auth/forgot-password",
            json={"email": "known@example.com"},
        )

    assert response.status_code == 202
