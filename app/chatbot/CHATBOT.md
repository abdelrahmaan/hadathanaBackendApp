# Chatbot — Architecture & Design

## Overview

Hadathna's chatbot is an **Agentic RAG** system: instead of a fixed retrieve→generate pipeline, a LangChain agent decides when and how to search, then generates a grounded Arabic answer citing only what it found.

- **Backend**: FastAPI SSE endpoint (`POST /api/v2/chat`)
- **LLM**: Any OpenRouter model, configured via `CHATBOT_MODEL` env var (default: `google/gemini-3-flash-preview`)
- **Vector store**: Qdrant (self-hosted Docker), hybrid dense + BM25
- **Embeddings**: Cohere `embed-v4.0` (1536-dim)
- **Reranker**: Cohere `rerank-multilingual-v3.0`
- **Session storage**: MongoDB (`chat_sessions_dev` / `chat_sessions_prod`)

---

## Directory Layout

```
app/chatbot/
  __init__.py
  config.py       # Compile-time constants (model names, dims, collection name)
  models.py       # Pydantic models: ChatRequest, Citation, SessionMessage, ChatSession
  prompts.py      # System prompts — single source of truth, never inline
  qdrant.py       # QdrantClient lifecycle (module-level singleton, mirrors database.py)
  indexer.py      # Populate/sync Qdrant from MongoDB (idempotent, BM25 computed locally)
  retriever.py    # build_hadiths_retriever() — hybrid search + Cohere rerank + logging wrappers
  agent.py        # build_agent() — create_agent() + @tool search_hadiths
  session.py      # get_or_create_session() / append_turn() / update_session_title() — MongoDB persistence
  router.py       # POST /api/v2/chat — SSE streaming, pipeline logging
  CHATBOT.md      # This file
```

---

## Agent Flow

```
User Question
      │
      ▼
┌─────────────────────────────────────────┐
│        MongoDB History Seeding          │
│  session.messages[-6:] loaded from DB   │
│  (last 3 QA pairs — durable across      │
│   server restarts, no InMemorySaver)    │
└──────────────────┬──────────────────────┘
                   │
                   ▼  history + current question
┌─────────────────────────────────────────┐
│           SYSTEM PROMPT                 │
│  (ARABIC_SYSTEM_PROMPT from prompts.py) │
│  • Answer ONLY from tool results        │
│  • Always call search_hadiths first     │
│  • Cite inline with [1][2]...           │
│  • Refuse if tool returns nothing       │
└──────────────────┬──────────────────────┘
                   │
                   ▼
         ┌─────────────────┐
         │   LLM decides   │◄─────────────────────────┐
         │ (via OpenRouter) │                          │
         └────────┬────────┘                          │
                  │                                   │
         ┌────────▼────────┐              ┌───────────┴──────────┐
         │  Call tool?     │──── YES ────►│   search_hadiths()   │
         │                 │              │  (LLM-refined query)  │
         └────────┬────────┘              └──────────┬───────────┘
                  │ NO (answer ready)                │
                  │                    ┌─────────────▼────────────┐
                  │                    │   Qdrant Hybrid Search   │
                  │                    │   dense (embed-v4.0)     │
                  │                    │   + BM25 sparse          │
                  │                    │   → 20 candidates        │
                  │                    └─────────────┬────────────┘
                  │                                  │
                  │                    ┌─────────────▼────────────┐
                  │                    │   Cohere Reranker        │
                  │                    │   rerank-multilingual    │
                  │                    │   20 → top 5 with scores │
                  │                    └─────────────┬────────────┘
                  │                                  │
                  │                    ┌─────────────▼────────────┐
                  │                    │  Formatted string to LLM │
                  │                    │  [1] (book)\nmatn\n_id   │
                  │                    └─────────────┬────────────┘
                  │                                  │
                  │◄─────────────────────────────────┘
                  │         tool result fed back to LLM
                  │
         ┌────────▼────────┐
         │  LLM generates  │  ← streams tokens via SSE to client
         │  Arabic answer  │    references [1][2]... from tool
         └────────┬────────┘
                  │
                  ▼
         ┌────────────────────────────────┐
         │  Citations from stashed docs   │
         │  get_last_docs(thread_id)      │
         │  docs captured during tool     │
         │  call — no second retrieval    │
         └────────┬───────────────────────┘
                  │
                  ▼
    SSE: assistant_message_complete  (full text + citations[])
    SSE: thread_rename               (short Arabic title)
    SSE: stream_end
          │
          ▼
    MongoDB: append_turn() → chat_sessions_dev / chat_sessions_prod
```

---

## API Reference

### `POST /api/v2/chat`

Requires valid JWT session cookie (`fastapiusersauth`). Returns a `text/event-stream` response.

**Request body** (JSON):

```json
{
  "question": "ما حكم الصلاة في وقتها؟",
  "session_id": null,
  "topic": null,
  "book": null
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `question` | string | yes | User's question in Arabic or English |
| `session_id` | string (UUID) | no | Omit or `null` to start a new session; pass the UUID from a previous `assistant_message_start` event to resume |
| `topic` | string | no | Optional Qdrant payload filter — restricts retrieval to hadiths with this topic tag |
| `book` | string | no | Optional Qdrant payload filter — restricts retrieval to hadiths from this book name |

**Error responses** (before any SSE is sent):

| Code | Condition |
|------|-----------|
| `401` | Not authenticated |
| `403` | `session_id` belongs to a different user |
| `429` | Daily quota exceeded (see quota section below) |

**SSE event stream** — events emitted in this exact order:

| # | Event type | Payload | Notes |
|---|-----------|---------|-------|
| 1 | `assistant_message_start` | `{ "type": "assistant_message_start", "content": "", "session_id": "<uuid>" }` | Always first; `session_id` is included only for new sessions |
| 2 | `content` | `{ "type": "content", "content": "إنما..." }` | One event per token chunk — append to display buffer |
| 3 | `assistant_message_complete` | `{ "type": "assistant_message_complete", "data": { "message_type": "assistant", "content": "...", "citations": [...] } }` | Full answer text + filtered citations |
| 4 | `thread_rename` | `{ "type": "thread_rename", "title": "أول حديث في البخاري" }` | Short Arabic conversation title (5–7 words), auto-generated |
| 5 | `stream_end` | `{ "type": "stream_end" }` | Stream closed — client should stop reading |
| — | `error` | `{ "type": "error", "content": "حدث خطأ..." }` | Emitted on exception; Arabic error message |

**Citation object** (inside `assistant_message_complete`):

```json
{
  "resource_id": "8896",
  "text_span": "الأم أحق الناس بحسن الصحبة...",
  "confidence": 0.974,
  "title": "الأم أحق الناس بحسن الصحبة",
  "hadith_url": "https://www.bukhari-pedia.net/book/matn_bukhari/8896"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `resource_id` | string | `hadith_indices[0]` — maps to `GET /api/v2/hadiths/{resource_id}`. **Use this for internal navigation.** |
| `text_span` | string | First 200 chars of `matn_text_plain` |
| `confidence` | float | Cohere reranker score, 0.0–1.0 |
| `title` | string | Short Arabic headline from the hadith `title` field |
| `hadith_url` | string | External bukhari-pedia.net URL — for display only, **not** for internal routing |

**Frontend integration:**
```js
// CORRECT — internal navigation
<a href={`/hadith/${citation.resource_id}`}>{citation.title}</a>

// WRONG — opens external site
<a href={citation.hadith_url}>...</a>
```

---

### `GET /api/v2/chat/sessions`

Requires auth. Returns session metadata (no message history).

**Query params**: `skip` (default `0`), `limit` (default `20`, max `100`)

**Response** `200`:
```json
[
  {
    "session_id": "3f2e1a00-...",
    "title": "أول حديث في البخاري",
    "created_at": "2024-01-01T10:00:00Z",
    "message_count": 4
  }
]
```

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | string (UUID) | Pass to `POST /api/v2/chat` to resume |
| `title` | string | Auto-generated Arabic title (empty string until first `thread_rename`) |
| `created_at` | datetime | Session creation time (UTC) |
| `message_count` | int | Total turns (user + assistant combined) |

---

### `GET /api/v2/chat/sessions/{session_id}`

Requires auth. Returns the full session including all messages and citations.

**Response** `200`:
```json
{
  "session_id": "3f2e1a00-...",
  "user_id": "a1b2c3d4-...",
  "title": "أول حديث في البخاري",
  "created_at": "2024-01-01T10:00:00Z",
  "messages": [
    {
      "role": "user",
      "content": "ما هو أول حديث في صحيح البخاري؟",
      "citations": [],
      "timestamp": "2024-01-01T10:00:01Z"
    },
    {
      "role": "assistant",
      "content": "أول حديث هو حديث النية...",
      "citations": [
        {
          "resource_id": "1",
          "text_span": "إنما الأعمال بالنيات...",
          "confidence": 0.97,
          "title": "حديث النية",
          "hadith_url": "https://..."
        }
      ],
      "timestamp": "2024-01-01T10:00:05Z"
    }
  ]
}
```

Returns `403` if the session belongs to a different user, `404` if not found.

---

### `DELETE /api/v2/chat/sessions/{session_id}`

Requires auth. No request body. **Response** `204`.

Returns `403` if the session belongs to a different user.

---

### `GET /api/v2/chat/quota`

Requires auth. Returns the authenticated user's quota usage for today. No side effects — pure read.

**Response** `200`:
```json
{
  "tier": "free",
  "limit": 3,
  "used": 2,
  "remaining": 1,
  "resets_at": "2026-04-28T00:00:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `tier` | string | User's plan: `"free"`, `"supporter"`, or `"unlimited"` |
| `limit` | int | Daily request cap; `-1` = no limit |
| `used` | int | Requests made today; `0` if none yet |
| `remaining` | int | `limit - used`; `-1` if unlimited |
| `resets_at` | datetime | UTC midnight tonight — when today's counter expires |

**Notes:**
- `used` reflects the count **after** any `POST /api/v2/chat` calls today (counters are incremented atomically before streaming starts)
- Call this once on page load and after each chat turn to keep the UI indicator fresh
- Returns `401` if not authenticated

---

## SSE Event Sequence

Every request emits exactly these events in order:

| # | Event | When | Key fields |
|---|-------|------|------------|
| 1 | `assistant_message_start` | Always first | `session_id` (new sessions only) |
| 2 | `content` | Repeats per token | `content` — append to display |
| 3 | `assistant_message_complete` | After full generation | `content`, `citations[]` |
| 4 | `thread_rename` | After complete | `title` (5–7 Arabic words) |
| 5 | `stream_end` | Always last | — |
| — | `error` | On exception | `content` — Arabic error message |

---

## Pipeline Logging

Each request logs the full pipeline. Watch with:

```bash
make dev-logs | grep -E "pipeline_|tool_|retrieval_|reranking_"
```

Log events in order per question:

| Event | Logger | Key fields |
|-------|--------|------------|
| `pipeline_user_question` | router | `session_id`, `question` |
| `pipeline_llm_start` | router | `session_id` |
| `tool_search_hadiths` | agent | `query` (LLM-refined) |
| `retrieval_candidates` | retriever | `query`, `count` (always 20) |
| `reranking_done` | retriever | `returned` (5), `scores[]`, `urls[]` |
| `tool_results_ready` | agent | `query`, `count` |
| `pipeline_llm_done` | router | `response_chars` |
| `pipeline_citations_start` | router | — |
| `pipeline_citations_done` | router | `citations` count (from stashed docs, no extra retrieval) |
| `pipeline_done` | router | `title` |

A `--------------------------------------------------` separator is printed before each new question.

**Reading the reranker scores:**

| Score | Meaning |
|-------|---------|
| `> 0.7` | Very relevant — LLM-refined query matched well |
| `0.3–0.7` | Moderate relevance |
| `< 0.3` | Weak match — consider query rewriting or prompt tuning |

Citation scores come from the same tool call retrieval (LLM-refined query) — no second retrieval pass. Scores are therefore high and consistent with the tool results shown to the LLM.

---

## Qdrant Collection: `hadiths_matn`

| Field | Value |
|-------|-------|
| Points | 7,075 |
| Dense vector | `dense` — 1536-dim, Cohere `embed-v4.0`, cosine |
| Sparse vector | `text-sparse` — BM25 via `Qdrant/bm25` FastEmbed |
| Retrieval mode | `HYBRID` (dense + sparse fused) |
| Fetch k | 20 candidates |
| Rerank top_n | 5 final |

**Payload structure per point:**

```json
{
  "matn_text_plain": "«إنما الأعمال بالنيات...»",
  "metadata": {
    "hadith_url": "https://www.bukhari-pedia.net/book/matn_bukhari/3",
    "book": "كيف كان بدء الوحي...",
    "chapter": "",
    "matn_text": "«إِنَّما الأعمالُ...»",
    "topics": ["النية", "الأعمال"],
    "rawi_ids": [1, 5, 12]
  }
}
```

MongoDB is the source of truth. Re-sync Qdrant after any data update:
```bash
docker exec hadathana-api-dev python scripts/sync_qdrant.py \
  --uri mongodb://mongo:27017/ --db HadithDataDev --qdrant-url http://qdrant:6333 --force
```

---

## Session Storage

Sessions are stored in MongoDB, collection selected by `APP_ENV`:

| Environment | Collection |
|------------|------------|
| `dev` | `chat_sessions_dev` |
| `prod` | `chat_sessions_prod` |

`POST /api/v2/chat` requires authentication (JWT cookie). Sessions are **user-scoped** — resuming another user's `session_id` returns `403`.

Schema (`ChatSession`):

```python
session_id: str          # UUID, generated server-side on first message
user_id: str             # UUID string from auth User.id — set on creation
title: str               # short Arabic title, auto-generated after first turn
created_at: datetime
messages: [
  { role: "user" | "assistant", content: str, citations: [], timestamp: datetime }
]
```

`session_id` is returned in `assistant_message_start` (new sessions only). Client must store and re-send it on every follow-up message.

### Session management endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v2/chat/sessions` | List user's sessions — returns `[ChatSessionMeta]` (no messages), supports `skip`/`limit` |
| `GET` | `/api/v2/chat/sessions/{session_id}` | Full session with all messages and citations |
| `DELETE` | `/api/v2/chat/sessions/{session_id}` | Delete session — `403` if owned by another user |

`ChatSessionMeta`:
```python
session_id: str
title: str
created_at: datetime
message_count: int
```

MongoDB indexes on both session collections:
- `{ user_id: 1, created_at: -1 }` — powers the list endpoint
- `{ session_id: 1 }` unique — fast point lookups

---

## Key Design Decisions

**Why Agentic RAG over chain-based RAG?**
The agent decides *when* to search and *how* to phrase the query — it naturally reformulates "من راوي حديث النية؟" into "حديث النية إنما الأعمال بالنيات راوي" before hitting Qdrant, which produces much better retrieval scores.

**Why stash docs in `_last_docs` instead of post-stream re-retrieval?**
The tool stashes retrieved `Document` objects in a module-level dict keyed by `thread_id`. After streaming completes, the router calls `get_last_docs(thread_id)` to retrieve them — zero extra network calls, ~500ms saved per request. Citation scores are also higher because they use the LLM-refined query, not the raw user question.

**Why `embed-v4.0` not `embed-multilingual-v3.0`?**
The stored `matn_embedding` vectors in MongoDB were generated with `embed-v4.0` (1536-dim). The embedding model at query time must match the stored vectors exactly. `embed-multilingual-v3.0` is 1024-dim and would cause a dimension mismatch error.

**Why MongoDB history seeding instead of `InMemorySaver`?**
The agent is stateless — no checkpointer. On every request, `router.py` loads `session.messages[-6:]` (last 3 QA pairs) from MongoDB and prepends them to the `agent.astream()` input as explicit messages. This makes memory durable across server restarts and container redeploys: the agent always sees the prior conversation regardless of whether the process was restarted. An `InMemorySaver` would silently lose history on every restart, which is unacceptable in prod. The empty-list slice `[][-6:]` returns `[]`, so new sessions work identically with zero prior context.

**Why `init_chat_model` (not `create_agent`) for title generation?**
`create_agent` is reserved for the main orchestrator only. Title generation is a one-shot utility call — no tools, no conversation state, no streaming — so it uses a plain `init_chat_model` singleton (`_title_model` in `agent.py`). This singleton is initialised once in `build_agent()` and reused across requests, avoiding per-request model instantiation overhead. Generation is launched as `asyncio.create_task()` at the start of streaming so it runs concurrently with LLM token output and adds zero perceived latency.

**Why Rule 0 (greeting guard) in the system prompt?**
Without it, the LLM treats every input as potentially Islamic and calls `search_hadiths` on greetings ("مرحبا", "hi"), returning irrelevant citations. Rule 0 explicitly carves out small-talk: respond naturally, no tool call. This is simpler and zero-cost compared to a pre-classification LLM call.

**Why internal `_id` not `hadith_url` for citations?**
`_id` is the project's canonical identifier (Qdrant point id = `hadith_indices[0]`). It maps directly to `GET /api/v2/hadiths/{id}`. The `hadith_url` is an external bukhari-pedia.net URL — useful as a deep link but not as an internal reference.
