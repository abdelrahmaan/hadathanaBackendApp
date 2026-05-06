# LangSmith Observability — Design Spec

**Date:** 2026-05-05
**Branch:** feat-enhance_chatbot

---

## Context

LangSmith tracing infrastructure is partially wired but not fully functional:

- Env vars are set at startup (`LANGCHAIN_TRACING_V2`, `LANGCHAIN_API_KEY`, `LANGCHAIN_PROJECT`) — these work but use the old naming convention
- `langsmith` is imported in `agent.py` but not pinned in `requirements.txt`
- `wrap_gemini()` is called but there is a syntax bug (missing `)` on line 118 of `agent.py`)
- `@traceable` decorates the `search_hadiths` tool — good, but the full request lifecycle (user question → retrieval → LLM → title generation) is not grouped under a single parent trace
- No `session_id` or `user_id` is attached to traces, making it impossible to correlate LangSmith traces with MongoDB sessions

The goal is to complete the observability setup so that every chat request shows as a single end-to-end trace in LangSmith with full context: who made the request, which session it belongs to, and the latency breakdown across retrieval, main LLM, and title generation.

---

## Architecture

The tracing hierarchy per chat request will be:

```
chat_request (chain, @traceable on router chat() handler)
├── hadiths_search (tool, @traceable — already present)
│   └── LangChain auto-traced: Qdrant retrieval + Cohere rerank
├── LangChain auto-traced: main LLM (wrapped via wrap_gemini)
└── generate_title (chain, @traceable — to be added)
    └── LangChain auto-traced: title LLM call
```

Metadata attached to the root `chat_request` run: `session_id`, `user_id`, `app_env`.

---

## Changes

### 1. `requirements.txt`

Add `langsmith>=0.2.0` under the Chatbot section (currently imported but not pinned).

### 2. `app/main.py` — env var names

Update lines 20–22 to use the modern LangSmith SDK names:
- `LANGSMITH_TRACING=true` (replaces `LANGCHAIN_TRACING_V2`)
- `LANGSMITH_API_KEY=<key>` (replaces `LANGCHAIN_API_KEY`)
- `LANGSMITH_PROJECT=<project>` (replaces `LANGCHAIN_PROJECT`)

Both old and new names work, but the new names are canonical in the current SDK.

### 3. `app/chatbot/agent.py`

**Fix syntax bug:** Missing `)` closing `init_chat_model(` call (line 118 — `wrap_gemini(init_chat_model(...)` needs a closing paren before being passed to `wrap_gemini`).

**Add `@traceable` to `generate_title()`:**
```python
@traceable(name="generate_title", run_type="chain")
async def generate_title(question: str) -> str:
    ...
```

**Remove `@traceable(name="Alrawi")` from `build_agent()`** — `build_agent()` runs once at startup, not per request; tracing it adds noise without value.

### 4. `app/chatbot/router.py`

**Wrap the `chat` endpoint's `event_stream()` generator with `@traceable`** and pass `session_id` + `user_id` as metadata via `tracing_context`:

```python
from langsmith.run_helpers import tracing_context
from langsmith import traceable

# Inside the chat() handler, before the agent.astream() call:
with tracing_context(
    project_name=settings.get_langsmith_project(),
    metadata={"session_id": session.session_id, "user_id": str(user.id), "app_env": settings.app_env},
    tags=[settings.app_env],
):
    async for chunk in agent.astream(...):
        ...
```

This groups the entire streaming request — including the tool call and title generation — under one parent trace in LangSmith. The `tracing_context` block wraps only the `astream()` loop and the `generate_title()` await.

---

## Files to Modify

| File | Change |
|---|---|
| `requirements.txt` | Add `langsmith>=0.2.0` |
| `app/main.py` | Update env var names (LANGSMITH_TRACING, LANGSMITH_API_KEY, LANGSMITH_PROJECT) |
| `app/chatbot/agent.py` | Fix syntax bug; add `@traceable` to `generate_title`; remove `@traceable` from `build_agent` |
| `app/chatbot/router.py` | Wrap `agent.astream()` + `generate_title()` in `tracing_context` with session metadata |

---

## Verification

1. Set `LANGSMITH_API_KEY_DEV=<key>` in `.env`
2. Start dev stack: `make dev`
3. Send a chat request to `POST /api/v2/chat`
4. Open LangSmith → project `hadathana_dev`
5. Confirm: one root run named `chat_request` (or whatever the outer traceable is named) with metadata `session_id` + `user_id`
6. Expand the run — should see `hadiths_search` as a child tool run and `generate_title` as a child chain run
7. LLM calls (OpenRouter/Gemini) should appear as nested LLM runs under the agent node
