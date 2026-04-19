# Chatbot User Sessions — Design Spec

**Date:** 2026-04-18
**Branch:** feat-chatbot_v1

---

## Context

The chatbot endpoint (`POST /api/v2/chat`) currently creates anonymous sessions with no link to authenticated users. The auth system (fastapi-users v15, JWT + HttpOnly cookie) is fully implemented and used by the bookmarks feature. This spec covers linking chatbot sessions to authenticated users so that each user owns their conversation history, can list/resume sessions across devices, and can delete sessions they no longer want.

---

## Data Model

### `ChatSession` — updated (`app/chatbot/models.py`)

```python
class ChatSession(BaseModel):
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str                          # UUID string from auth User.id
    title: str = ""                       # populated by thread_rename SSE event
    created_at: datetime = Field(default_factory=datetime.utcnow)
    messages: list[SessionMessage] = []
```

### `ChatSessionMeta` — new response model (`app/chatbot/models.py`)

```python
class ChatSessionMeta(BaseModel):
    session_id: str
    title: str
    created_at: datetime
    message_count: int
```

Used by the list endpoint — no messages included.

### MongoDB Index — new (`mongo_migration/create_indexes.py`)

Compound index on both `chat_sessions_dev` and `chat_sessions_prod`:

```python
{ "user_id": 1, "created_at": -1 }
```

Powers `GET /api/v2/chat/sessions` efficiently.

---

## API Endpoints

### Modified

**`POST /api/v2/chat`**
- Now requires authentication: `user: User = Depends(current_active_user)`
- Passes `str(user.id)` to `get_or_create_session()`
- New sessions get `user_id` set automatically
- Existing sessions: ownership verified — `403` if `session.user_id != str(user.id)`
- Title persisted: when `thread_rename` SSE event fires, calls `update_session_title(db, session_id, title)`
- Returns `401` if unauthenticated

### New

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/api/v2/chat/sessions` | Required | List user's sessions — `list[ChatSessionMeta]`, supports `skip`/`limit` |
| `GET` | `/api/v2/chat/sessions/{session_id}` | Required | Full session with all messages + citations |
| `DELETE` | `/api/v2/chat/sessions/{session_id}` | Required | Delete session — ownership check, returns `204` |

All new endpoints use `Depends(current_active_user)` and filter by `user_id`.

---

## Session Logic

### `app/chatbot/session.py`

**`get_or_create_session(db, session_id, user_id)`** — updated signature:
- New session (`session_id=None` or not found): creates `ChatSession` with `user_id`
- Existing session: loads from DB, raises `HTTPException(403)` if `session.user_id != user_id`

**`update_session_title(db, session_id, title)`** — new function:
- `$set: {"title": title}` on the session document
- Called from router after `thread_rename` event

**`append_turn()`** — unchanged.

### Router flow (`app/chatbot/router.py`)

```
POST /api/v2/chat
  → current_active_user (401 if missing)
  → get_or_create_session(db, request.session_id, str(user.id))
  → stream agent response (unchanged)
  → on thread_rename event → update_session_title(db, session_id, title)
  → append_turn() after stream ends (unchanged)
```

---

## Tests (`tests/test_chatbot_sessions.py`)

Written first (TDD), confirmed red before implementation.

| Test | Assertion |
|------|-----------|
| `test_chat_requires_auth` | `POST /api/v2/chat` → `401` with no cookie |
| `test_chat_creates_session_with_user_id` | New session has `user_id` matching authenticated user |
| `test_chat_session_ownership` | User B continuing User A's session → `403` |
| `test_list_sessions_empty` | New user → `[]` |
| `test_list_sessions_returns_metadata` | Returns `ChatSessionMeta` fields only, no messages |
| `test_get_session_full` | Returns full messages + citations |
| `test_get_session_wrong_user` | `403` if session belongs to another user |
| `test_delete_session` | Session removed; subsequent `GET` → `404` |
| `test_delete_session_wrong_user` | `403` if session belongs to another user |

Pattern: `httpx` + `pytest` with mocked MongoDB collections (matches existing test style).

---

## Files to Modify

| File | Change |
|------|--------|
| `app/chatbot/models.py` | Add `user_id`, `title` to `ChatSession`; add `ChatSessionMeta` |
| `app/chatbot/session.py` | Update `get_or_create_session` signature; add `update_session_title` |
| `app/chatbot/router.py` | Add auth dependency; add 3 new endpoints; persist title |
| `mongo_migration/create_indexes.py` | Add compound index on `user_id + created_at` |
| `tests/test_chatbot_sessions.py` | New test file (TDD) |
| `tasks.md` | Mark task in_progress → completed |

---

## Out of Scope

- Session rename by user (PATCH endpoint) — can be added later
- Anonymous chatting — deliberately removed; auth is required
- Migrating existing anonymous sessions — none exist in prod worth keeping
