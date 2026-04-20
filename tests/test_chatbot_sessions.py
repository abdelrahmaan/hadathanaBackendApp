"""Tests for authenticated chatbot session endpoints."""

from __future__ import annotations

import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from langchain_core.messages import AIMessageChunk


class _AsyncIter:
    """Minimal async iterator for mocked Mongo cursors."""

    def __init__(self, items=None):
        self._items = iter(items or [])

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


def _make_other_collection():
    col = MagicMock()
    col.find.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    col.count_documents = AsyncMock(return_value=0)
    col.find_one = AsyncMock(return_value=None)
    col.insert_one = AsyncMock(return_value=MagicMock(inserted_id="mock_id"))
    col.delete_one = AsyncMock(return_value=MagicMock(deleted_count=1))
    return col


def _make_sessions_collection(find_one_return=None, deleted_count=1, find_docs=None):
    """Return a mock sessions collection with list/get/delete helpers wired."""
    col = MagicMock()
    col.find_one = AsyncMock(return_value=find_one_return)
    col.update_one = AsyncMock(return_value=MagicMock(matched_count=1, modified_count=1))
    col.delete_one = AsyncMock(return_value=MagicMock(deleted_count=deleted_count))
    col.count_documents = AsyncMock(return_value=len(find_docs or []))

    cursor = _AsyncIter(find_docs)
    skip_mock = MagicMock()
    skip_mock.limit.return_value = cursor
    sort_mock = MagicMock()
    sort_mock.skip.return_value = skip_mock
    col.find.return_value.sort.return_value = sort_mock
    return col


def _make_mock_user(user_id=None):
    from app.auth.models import User

    uid = user_id or uuid.uuid4()
    return User(
        id=uid,
        email="user@example.com",
        hashed_password="$2b$12$fakehash",
        is_active=True,
        is_superuser=False,
        is_verified=False,
    )


def _make_mock_agent(tokens: list[str]) -> MagicMock:
    async def _stream(*args, **kwargs):
        for token in tokens:
            yield (AIMessageChunk(content=token), {"langgraph_node": "agent"})

    agent = MagicMock()
    agent.astream = MagicMock(side_effect=_stream)
    return agent


@asynccontextmanager
async def _client_context(
    *,
    sessions_col=None,
    user=None,
    agent_tokens=None,
    citation_docs=None,
    title="عنوان الجلسة",
):
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            sys.modules.pop(mod, None)

    other_col = _make_other_collection()
    sessions_col = sessions_col or _make_sessions_collection()
    mock_db = MagicMock()
    mock_db.__getitem__ = lambda _, key: (
        sessions_col if key in ("chat_sessions_dev", "chat_sessions_prod") else other_col
    )

    patches = [
        patch("app.database.connect", new_callable=AsyncMock),
        patch("app.database.disconnect", new_callable=AsyncMock),
        patch("app.database.validate_connection", new_callable=AsyncMock),
        patch("app.database.get_client", return_value=MagicMock()),
        patch("app.database.get_db", return_value=mock_db),
        patch("app.database.get_hadiths_collection", return_value=other_col),
        patch("app.database.get_narrators_collection", return_value=other_col),
        patch("app.database.get_podia_hadiths_collection", return_value=other_col),
        patch("app.database.get_podia_narrators_collection", return_value=other_col),
        patch("app.database.get_auth_users_collection", return_value=other_col),
        patch("app.database.get_bookmarks_collection", return_value=other_col),
        patch("app.database.get_user_quotas_collection", return_value=MagicMock(
            find_one_and_update=AsyncMock(return_value={"request_count": 1})
        )),
        patch("app.chatbot.qdrant.connect_qdrant", new_callable=AsyncMock),
        patch("app.chatbot.qdrant.disconnect_qdrant", new_callable=AsyncMock),
        patch("app.chatbot.agent.build_agent"),
        patch(
            "app.chatbot.router.get_agent",
            return_value=_make_mock_agent(agent_tokens or ["هذا جواب مختصر.\nREFS:[1]"]),
        ),
        patch("app.chatbot.router._generate_title", new_callable=AsyncMock, return_value=title),
        patch("app.chatbot.router.get_last_docs", return_value=citation_docs or []),
    ]

    for p in patches:
        p.start()

    try:
        from app.auth.config import current_active_user
        from app.main import app

        if user is not None:
            app.dependency_overrides[current_active_user] = lambda: user

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c, sessions_col

        app.dependency_overrides.clear()
    finally:
        for p in reversed(patches):
            p.stop()


@pytest.mark.asyncio
async def test_chat_requires_auth():
    """POST /api/v2/chat without auth → 401."""
    async with _client_context() as (client, _):
        response = await client.post("/api/v2/chat", json={"question": "ما هو الحديث؟"})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_chat_creates_session_with_user_id():
    """New sessions are persisted with the authenticated user ID."""
    user = _make_mock_user()

    async with _client_context(user=user) as (client, sessions_col):
        response = await client.post("/api/v2/chat", json={"question": "ما معنى الإخلاص؟"})

    assert response.status_code == 200
    assert sessions_col.update_one.await_count >= 1
    saved_doc = sessions_col.update_one.await_args_list[0].args[1]["$set"]
    assert saved_doc["user_id"] == str(user.id)


@pytest.mark.asyncio
async def test_chat_session_ownership():
    """User B continuing User A's session must get 403."""
    owner = _make_mock_user()
    stranger = _make_mock_user()
    sessions_col = _make_sessions_collection(
        find_one_return={
            "session_id": "session-owned-by-a",
            "user_id": str(owner.id),
            "title": "جلسة خاصة",
            "created_at": datetime.now(timezone.utc),
            "messages": [],
        }
    )

    async with _client_context(user=stranger, sessions_col=sessions_col) as (client, _):
        response = await client.post(
            "/api/v2/chat",
            json={"question": "أكمل الحوار", "session_id": "session-owned-by-a"},
        )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_sessions_empty():
    """A new user with no sessions gets an empty list."""
    async with _client_context(user=_make_mock_user()) as (client, _):
        response = await client.get("/api/v2/chat/sessions")

    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_list_sessions_returns_metadata():
    """List endpoint returns metadata only, ordered by created_at."""
    user = _make_mock_user()
    sessions_col = _make_sessions_collection(
        find_docs=[
            {
                "session_id": "sess-1",
                "user_id": str(user.id),
                "title": "حديث النية",
                "created_at": datetime.now(timezone.utc),
                "messages": [{"role": "user"}, {"role": "assistant"}],
            }
        ]
    )

    async with _client_context(user=user, sessions_col=sessions_col) as (client, _):
        response = await client.get("/api/v2/chat/sessions")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["session_id"] == "sess-1"
    assert data[0]["title"] == "حديث النية"
    assert data[0]["message_count"] == 2
    assert "messages" not in data[0]


@pytest.mark.asyncio
async def test_get_session_full():
    """Owner can fetch the full session including citations."""
    user = _make_mock_user()
    sessions_col = _make_sessions_collection(
        find_one_return={
            "session_id": "sess-1",
            "user_id": str(user.id),
            "title": "حديث الهجرة",
            "created_at": datetime.now(timezone.utc),
            "messages": [
                {
                    "role": "user",
                    "content": "ما معنى الهجرة؟",
                    "citations": [],
                    "timestamp": datetime.now(timezone.utc),
                },
                {
                    "role": "assistant",
                    "content": "الهجرة انتقال القلب والعمل.",
                    "citations": [
                        {
                            "resource_id": "1",
                            "text_span": "إنما الأعمال بالنيات",
                            "confidence": 0.91,
                            "title": "حديث النية",
                            "hadith_url": "https://example.com/hadith/1",
                        }
                    ],
                    "timestamp": datetime.now(timezone.utc),
                },
            ],
        }
    )

    async with _client_context(user=user, sessions_col=sessions_col) as (client, _):
        response = await client.get("/api/v2/chat/sessions/sess-1")

    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == "sess-1"
    assert data["user_id"] == str(user.id)
    assert len(data["messages"]) == 2
    assert data["messages"][1]["citations"][0]["resource_id"] == "1"


@pytest.mark.asyncio
async def test_get_session_wrong_user():
    """Non-owners get 403 when fetching a session."""
    owner = _make_mock_user()
    stranger = _make_mock_user()
    sessions_col = _make_sessions_collection(
        find_one_return={
            "session_id": "sess-1",
            "user_id": str(owner.id),
            "title": "جلسة المالك",
            "created_at": datetime.now(timezone.utc),
            "messages": [],
        }
    )

    async with _client_context(user=stranger, sessions_col=sessions_col) as (client, _):
        response = await client.get("/api/v2/chat/sessions/sess-1")

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_delete_session():
    """Owner can delete a session, and it is gone on the next fetch."""
    user = _make_mock_user()
    session_doc = {
        "session_id": "sess-delete",
        "user_id": str(user.id),
        "title": "جلسة للحذف",
        "created_at": datetime.now(timezone.utc),
        "messages": [],
    }
    sessions_col = _make_sessions_collection(deleted_count=1)
    sessions_col.find_one = AsyncMock(side_effect=[session_doc, None])

    async with _client_context(user=user, sessions_col=sessions_col) as (client, _):
        delete_response = await client.delete("/api/v2/chat/sessions/sess-delete")
        get_response = await client.get("/api/v2/chat/sessions/sess-delete")

    assert delete_response.status_code == 204
    assert get_response.status_code == 404


@pytest.mark.asyncio
async def test_delete_session_wrong_user():
    """Non-owners get 403 when deleting a session."""
    owner = _make_mock_user()
    stranger = _make_mock_user()
    sessions_col = _make_sessions_collection(
        find_one_return={
            "session_id": "sess-delete",
            "user_id": str(owner.id),
            "title": "جلسة خاصة",
            "created_at": datetime.now(timezone.utc),
            "messages": [],
        }
    )

    async with _client_context(user=stranger, sessions_col=sessions_col) as (client, _):
        response = await client.delete("/api/v2/chat/sessions/sess-delete")

    assert response.status_code == 403
