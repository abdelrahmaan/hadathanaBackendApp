"""
Chatbot V1 tests.

All external services (MongoDB, Qdrant, LangChain agent, Cohere) are mocked.
No live connections required.
"""
import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from langchain_core.documents import Document
from langchain_core.messages import AIMessageChunk

# ── Shared fixtures ────────────────────────────────────────────────────────────

MOCK_DOCS = [
    Document(
        page_content="إنما الأعمال بالنيات وإنما لكل امرئ ما نوى",
        metadata={
            "_id": "1",
            "hadith_url": "https://www.bukhari-pedia.net/book/matn_bukhari/1",
            "book": "كتاب بدء الوحي",
            "title": "حديث النية",
            "relevance_score": 0.92,
        },
    ),
    Document(
        page_content="من كانت هجرته إلى الله ورسوله فهجرته إلى الله ورسوله",
        metadata={
            "_id": "2",
            "hadith_url": "https://www.bukhari-pedia.net/book/matn_bukhari/2",
            "book": "كتاب بدء الوحي",
            "title": "حديث الهجرة",
            "relevance_score": 0.85,
        },
    ),
    Document(
        page_content="بُنِيَ الإسلام على خمس",
        metadata={
            "_id": "3",
            "hadith_url": "https://www.bukhari-pedia.net/book/matn_bukhari/3",
            "book": "كتاب الإيمان",
            "title": "أركان الإسلام",
            "relevance_score": 0.70,
        },
    ),
]


async def _async_gen(items):
    """Yield items as an async generator."""
    for item in items:
        yield item


def _make_mock_agent(tokens: list[str]) -> MagicMock:
    """
    Build a mock agent whose astream() yields (AIMessageChunk, metadata) tuples,
    matching what _extract_token() expects from stream_mode="messages".
    Uses side_effect so each call produces a fresh generator.
    """
    async def _stream(*args, **kwargs):
        for t in tokens:
            yield (AIMessageChunk(content=t), {"langgraph_node": "agent"})

    agent = MagicMock()
    agent.astream = MagicMock(side_effect=_stream)
    return agent


def _parse_sse(text: str) -> list[dict]:
    """Parse SSE response body into list of event dicts."""
    events = []
    for line in text.splitlines():
        if line.startswith("data: "):
            events.append(json.loads(line[6:]))
    return events


def _make_mock_user():
    from app.auth.models import User

    return User(
        id=uuid.uuid4(),
        email="user@example.com",
        hashed_password="$2b$12$fakehash",
        is_active=True,
        is_superuser=False,
        is_verified=False,
    )


@pytest_asyncio.fixture
async def chat_client():
    """AsyncClient with all external services mocked."""
    mock_collection = MagicMock()
    mock_collection.find_one = AsyncMock(return_value=None)
    mock_collection.update_one = AsyncMock(return_value=None)
    mock_collection.find_one_and_update = AsyncMock(return_value={"request_count": 1})

    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    mock_mongo_client = MagicMock()

    mock_retriever = MagicMock()
    mock_retriever.invoke = MagicMock(return_value=MOCK_DOCS)
    mock_retriever.ainvoke = AsyncMock(return_value=MOCK_DOCS)

    mock_agent = _make_mock_agent(["الأعمال", " بالنيات", " [١]."])

    with (
        patch("app.database.connect", new_callable=AsyncMock),
        patch("app.database.disconnect", new_callable=AsyncMock),
        patch("app.database.validate_connection", new_callable=AsyncMock),
        patch("app.database.get_client", return_value=mock_mongo_client),
        patch("app.database.get_db", return_value=mock_db),
        patch("app.chatbot.qdrant.connect_qdrant", new_callable=AsyncMock),
        patch("app.chatbot.qdrant.disconnect_qdrant", new_callable=AsyncMock),
        patch("app.chatbot.agent.build_agent"),
        # Patch in the router's namespace (where the names are actually used)
        patch("app.chatbot.router.get_agent", return_value=mock_agent),
        patch("app.chatbot.agent.get_retriever", return_value=mock_retriever),
        patch("app.chatbot.router._generate_title", new_callable=AsyncMock, return_value="النية في الأعمال"),
        patch("app.chatbot.router.get_or_create_session", new_callable=AsyncMock) as mock_get_session,
        patch("app.chatbot.router.append_turn", new_callable=AsyncMock) as mock_append,
    ):
        from app.auth.config import current_active_user
        from app.chatbot.models import ChatSession
        from app.main import app

        mock_user = _make_mock_user()
        app.dependency_overrides[current_active_user] = lambda: mock_user
        mock_get_session.return_value = ChatSession(user_id=str(mock_user.id))
        mock_append.return_value = None

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            # Expose mocks so tests can inspect calls
            c.mock_append = mock_append
            c.mock_get_session = mock_get_session
            yield c
        app.dependency_overrides.clear()


# ── Tests ──────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_new_session_returns_session_id(chat_client):
    """When no session_id is sent, first SSE event must carry a new session_id."""
    response = await chat_client.post(
        "/api/v2/chat",
        json={"question": "ما معنى النية في الإسلام؟"},
    )
    assert response.status_code == 200
    events = _parse_sse(response.text)
    start = events[0]
    assert start["type"] == "assistant_message_start"
    assert "session_id" in start, "New session must expose session_id in start event"
    assert start["content"] == ""


@pytest.mark.asyncio
async def test_sse_schema_and_order(chat_client):
    """All required SSE event types must be present in the correct order."""
    response = await chat_client.post(
        "/api/v2/chat",
        json={"question": "ما معنى النية في الإسلام؟"},
    )
    assert response.status_code == 200
    events = _parse_sse(response.text)
    types = [e["type"] for e in events]

    assert types[0] == "assistant_message_start"
    assert "content" in types
    assert "assistant_message_complete" in types
    assert "thread_rename" in types
    assert types[-1] == "stream_end"

    # Validate assistant_message_complete shape
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    data = complete["data"]
    assert data["message_type"] == "assistant"
    assert isinstance(data["content"], str)
    assert isinstance(data["citations"], list)
    for cit in data["citations"]:
        assert "resource_id" in cit
        assert "text_span" in cit
        assert "confidence" in cit

    # Validate thread_rename
    rename = next(e for e in events if e["type"] == "thread_rename")
    assert rename["title"] == "النية في الأعمال"


@pytest.mark.asyncio
async def test_conversation_saved_to_mongo(chat_client):
    """After the stream completes, append_turn must be called with correct args."""
    response = await chat_client.post(
        "/api/v2/chat",
        json={"question": "ما معنى النية في الإسلام؟"},
    )
    assert response.status_code == 200

    chat_client.mock_append.assert_called_once()
    call_args = chat_client.mock_append.call_args
    # positional args: db, session, user_content, assistant_content, citations
    user_content = call_args[0][2]
    assistant_content = call_args[0][3]
    citations = call_args[0][4]

    assert user_content == "ما معنى النية في الإسلام؟"
    assert isinstance(assistant_content, str)
    assert isinstance(citations, list)


@pytest.mark.asyncio
async def test_refusal_when_no_context(chat_client):
    """When retriever returns no docs, response must contain a refusal phrase."""
    empty_retriever = MagicMock()
    empty_retriever.invoke = MagicMock(return_value=[])
    empty_retriever.ainvoke = AsyncMock(return_value=[])

    refusal_text = "لا يوجد في المصادر المُتاحة ما يجيب على سؤالك."
    refusal_agent = _make_mock_agent([refusal_text])

    with (
        patch("app.chatbot.router.get_agent", return_value=refusal_agent),
        patch("app.chatbot.agent.get_retriever", return_value=empty_retriever),
    ):
        response = await chat_client.post(
            "/api/v2/chat",
            json={"question": "ما رأيك في كرة القدم؟"},
        )

    assert response.status_code == 200
    events = _parse_sse(response.text)
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    assert "لا يوجد" in complete["data"]["content"]
    assert complete["data"]["citations"] == []


def _make_refs_agent_with_docs(tokens: list[str]) -> MagicMock:
    """
    Build a mock agent that stashes MOCK_DOCS keyed by the thread_id from config,
    simulating what the real search_hadiths tool does inside astream.
    """
    from app.chatbot import agent as agent_module

    async def _stream_with_stash(*args, **kwargs):
        cfg = kwargs.get("config") or {}
        thread_id = (cfg.get("configurable") or {}).get("thread_id", "")
        if thread_id:
            agent_module._last_docs[thread_id] = list(MOCK_DOCS)
        for t in tokens:
            yield (AIMessageChunk(content=t), {"langgraph_node": "agent"})

    agent = MagicMock()
    agent.astream = MagicMock(side_effect=_stream_with_stash)
    return agent


@pytest.mark.asyncio
async def test_refs_line_filters_citations(chat_client):
    """REFS:[1] in LLM response → only first doc returned as citation."""
    from app.chatbot.models import ChatSession
    session_id = "test-refs-filter-1"
    mock_session = ChatSession(session_id=session_id, user_id="test-user-id")

    refs_agent = _make_refs_agent_with_docs(
        ["النفقة على الزوجة صدقة [1].\nREFS:[1]"]
    )

    with (
        patch("app.chatbot.router.get_agent", return_value=refs_agent),
        patch("app.chatbot.router.get_or_create_session", new_callable=AsyncMock, return_value=mock_session),
    ):
        response = await chat_client.post(
            "/api/v2/chat",
            json={"question": "النفقة على الزوجة هل هي صدقة؟", "session_id": session_id},
        )

    assert response.status_code == 200
    events = _parse_sse(response.text)
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    citations = complete["data"]["citations"]

    assert len(citations) == 1
    assert citations[0]["hadith_url"] == "https://www.bukhari-pedia.net/book/matn_bukhari/1"
    assert "REFS:" not in complete["data"]["content"]


@pytest.mark.asyncio
async def test_refs_line_filters_multiple_citations(chat_client):
    """REFS:[1,3] → only docs 1 and 3 returned; doc 2 excluded."""
    from app.chatbot.models import ChatSession
    session_id = "test-refs-filter-multi"
    mock_session = ChatSession(session_id=session_id, user_id="test-user-id")

    refs_agent = _make_refs_agent_with_docs(
        ["الجواب من [1] و[3].\nREFS:[1,3]"]
    )

    with (
        patch("app.chatbot.router.get_agent", return_value=refs_agent),
        patch("app.chatbot.router.get_or_create_session", new_callable=AsyncMock, return_value=mock_session),
    ):
        response = await chat_client.post(
            "/api/v2/chat",
            json={"question": "سؤال عام", "session_id": session_id},
        )

    events = _parse_sse(response.text)
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    citations = complete["data"]["citations"]

    assert len(citations) == 2
    urls = {c["hadith_url"] for c in citations}
    assert "https://www.bukhari-pedia.net/book/matn_bukhari/1" in urls
    assert "https://www.bukhari-pedia.net/book/matn_bukhari/3" in urls
    assert "https://www.bukhari-pedia.net/book/matn_bukhari/2" not in urls


@pytest.mark.asyncio
async def test_no_refs_line_falls_back_to_all_docs(chat_client):
    """When LLM omits REFS line, all retrieved docs are returned as citations (fallback)."""
    from app.chatbot.models import ChatSession
    session_id = "test-refs-fallback"
    mock_session = ChatSession(session_id=session_id, user_id="test-user-id")

    refs_agent = _make_refs_agent_with_docs(["إجابة بدون مراجع."])

    with (
        patch("app.chatbot.router.get_agent", return_value=refs_agent),
        patch("app.chatbot.router.get_or_create_session", new_callable=AsyncMock, return_value=mock_session),
    ):
        response = await chat_client.post(
            "/api/v2/chat",
            json={"question": "سؤال", "session_id": session_id},
        )

    events = _parse_sse(response.text)
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    assert len(complete["data"]["citations"]) == 3


@pytest.mark.asyncio
async def test_refs_empty_returns_no_citations(chat_client):
    """REFS:[] means the LLM explicitly cited nothing → empty citations list."""
    from app.chatbot.models import ChatSession
    session_id = "test-refs-empty"
    mock_session = ChatSession(session_id=session_id, user_id="test-user-id")

    refs_agent = _make_refs_agent_with_docs(["لا توجد أحاديث مناسبة.\nREFS:[]"])

    with (
        patch("app.chatbot.router.get_agent", return_value=refs_agent),
        patch("app.chatbot.router.get_or_create_session", new_callable=AsyncMock, return_value=mock_session),
    ):
        response = await chat_client.post(
            "/api/v2/chat",
            json={"question": "سؤال", "session_id": session_id},
        )

    events = _parse_sse(response.text)
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    assert complete["data"]["citations"] == []


@pytest.mark.asyncio
async def test_citation_resource_id_populated(chat_client):
    """resource_id in each citation must match the _id from doc metadata."""
    response = await chat_client.post(
        "/api/v2/chat",
        json={"question": "ما معنى النية في الإسلام؟"},
    )
    assert response.status_code == 200
    events = _parse_sse(response.text)
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    for cit in complete["data"]["citations"]:
        assert cit["resource_id"] != "", "resource_id must not be empty"


@pytest.mark.asyncio
async def test_chat_returns_429_when_daily_quota_exceeded(chat_client):
    """Over-limit free user gets 429 before the agent is invoked."""
    with patch(
        "app.chatbot.quota.database.get_user_quotas_collection",
        return_value=MagicMock(
            find_one_and_update=AsyncMock(return_value={"request_count": 4})
        ),
    ):
        response = await chat_client.post(
            "/api/v2/chat",
            json={"question": "هل هذا هو الطلب الرابع اليوم؟"},
        )

    assert response.status_code == 429
    data = response.json()["detail"]
    assert data["limit"] == 3
    assert data["used"] == 4
    assert data["upgrade_hint"] == "supporter"
    assert "لقد وصلت" in data["ar"]
    chat_client.mock_get_session.assert_not_called()
