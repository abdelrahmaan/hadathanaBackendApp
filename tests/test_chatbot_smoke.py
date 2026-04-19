"""
Chatbot smoke tests — hit a live running API.

Requires the relevant stack to be up before running:
  make dev   → test against http://localhost:8001
  make prod  → test against http://localhost:8000

Run via Makefile:
  make test-chatbot-dev    # APP_ENV=dev  → port 8001
  make test-chatbot-prod   # APP_ENV=prod → port 8000
"""
import json
import os
import pytest
import httpx

# Resolve base URL from APP_ENV (mirrors app/config.py logic)
_ENV = os.getenv("APP_ENV", "dev")
BASE_URL = "http://localhost:8001" if _ENV == "dev" else "http://localhost:8000"
CHAT_URL = f"{BASE_URL}/api/v2/chat"
HEALTH_URL = f"{BASE_URL}/health"


def _parse_sse(text: str) -> list[dict]:
    events = []
    for line in text.splitlines():
        if line.startswith("data: "):
            events.append(json.loads(line[6:]))
    return events


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def require_api_up():
    """Fail fast with a clear message if the target API is not running."""
    try:
        r = httpx.get(HEALTH_URL, timeout=5)
        r.raise_for_status()
    except Exception:
        pytest.skip(
            f"API not reachable at {BASE_URL} — run `make {'dev' if _ENV == 'dev' else 'prod'}` first"
        )


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_health():
    """Health endpoint returns ok and mongodb connected."""
    r = httpx.get(HEALTH_URL, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body.get("mongodb") == "connected"


def test_chat_sse_event_sequence():
    """POST /api/v2/chat returns valid SSE with all required event types in order."""
    with httpx.Client(timeout=60) as client:
        with client.stream(
            "POST", CHAT_URL,
            json={"question": "ما حكم النية في الصلاة؟"},
            headers={"Content-Type": "application/json"},
        ) as r:
            assert r.status_code == 200
            assert "text/event-stream" in r.headers.get("content-type", "")
            raw = r.read().decode()

    events = _parse_sse(raw)
    types = [e["type"] for e in events]

    assert types[0] == "assistant_message_start", "First event must be assistant_message_start"
    assert "content" in types, "Must have at least one content token"
    assert "assistant_message_complete" in types
    assert "thread_rename" in types
    assert types[-1] == "stream_end"


def test_new_session_returns_session_id():
    """First request (no session_id) must return a session_id in the start event."""
    with httpx.Client(timeout=60) as client:
        with client.stream(
            "POST", CHAT_URL,
            json={"question": "مرحبا"},
        ) as r:
            raw = r.read().decode()

    events = _parse_sse(raw)
    start = events[0]
    assert start["type"] == "assistant_message_start"
    assert "session_id" in start, "New session must include session_id in start event"
    assert start["session_id"]


def test_multi_turn_session():
    """Second request with session_id must not return a new session_id."""
    with httpx.Client(timeout=60) as client:
        # First turn — get session_id
        with client.stream("POST", CHAT_URL, json={"question": "مرحبا"}) as r:
            raw = r.read().decode()
        session_id = _parse_sse(raw)[0]["session_id"]

        # Second turn — resume session
        with client.stream(
            "POST", CHAT_URL,
            json={"question": "شكراً", "session_id": session_id},
        ) as r:
            raw2 = r.read().decode()

    events2 = _parse_sse(raw2)
    start2 = events2[0]
    assert start2["type"] == "assistant_message_start"
    assert "session_id" not in start2, "Resumed session must NOT re-send session_id"


def test_citations_are_filtered():
    """
    For an Islamic question the LLM should call search_hadiths and return
    citations — and citations must be a subset of what was retrieved (REFS filtering).
    We can't know the exact count but we verify the shape and that count <= 5.
    """
    with httpx.Client(timeout=60) as client:
        with client.stream(
            "POST", CHAT_URL,
            json={"question": "ما فضل الصدقة على الزوجة؟"},
        ) as r:
            raw = r.read().decode()

    events = _parse_sse(raw)
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    citations = complete["data"]["citations"]

    assert isinstance(citations, list)
    assert len(citations) <= 5, "Citations must not exceed retriever top-k (5)"
    for c in citations:
        assert "resource_id" in c
        assert "text_span" in c
        assert "confidence" in c
        assert "hadith_url" in c
        assert "title" in c


def test_refs_not_in_content():
    """The REFS:[...] line must never appear in the streamed content."""
    with httpx.Client(timeout=60) as client:
        with client.stream(
            "POST", CHAT_URL,
            json={"question": "ما هي أركان الإسلام؟"},
        ) as r:
            raw = r.read().decode()

    events = _parse_sse(raw)
    content_tokens = "".join(
        e["content"] for e in events if e.get("type") == "content"
    )
    complete = next(e for e in events if e["type"] == "assistant_message_complete")

    assert "REFS:" not in content_tokens, "REFS line leaked into streamed content tokens"
    assert "REFS:" not in complete["data"]["content"], "REFS line leaked into complete content"


def test_greeting_returns_no_citations():
    """Greetings bypass search_hadiths — citations list must be empty."""
    with httpx.Client(timeout=60) as client:
        with client.stream(
            "POST", CHAT_URL,
            json={"question": "مرحبا، كيف حالك؟"},
        ) as r:
            raw = r.read().decode()

    events = _parse_sse(raw)
    complete = next(e for e in events if e["type"] == "assistant_message_complete")
    assert complete["data"]["citations"] == [], "Greeting should produce no citations"
