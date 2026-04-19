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

### Citation object

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
| `resource_id` | string | `hadith_indices[0]` — the internal hadith index. **Use this** to build frontend links. |
| `text_span` | string | First 200 chars of `matn_text_plain` |
| `confidence` | float | Cohere reranker score, 0.0–1.0 |
| `title` | string | Short Arabic headline from the hadith `title` field |
| `hadith_url` | string | External bukhari-pedia.net source URL — **do NOT use for internal navigation** |

**Frontend integration rule:**

```
// CORRECT — internal navigation
<a href={`/hadith/${citation.resource_id}`}>{citation.title}</a>

// WRONG — opens external site
<a href={citation.hadith_url}>...</a>
```

`resource_id` maps to `GET /api/v2/hadiths/{id}` for the full hadith details.

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

**Why `InMemorySaver` for session memory?**
`create_agent` is called with a module-level `InMemorySaver` checkpointer. Each request passes `thread_id = session.session_id` in the LangChain config — the agent accumulates conversation history per thread automatically. `InMemorySaver` is in-process; swap to `langgraph-checkpoint-redis` for multi-replica prod without changing any agent code.

**Why Rule 0 (greeting guard) in the system prompt?**
Without it, the LLM treats every input as potentially Islamic and calls `search_hadiths` on greetings ("مرحبا", "hi"), returning irrelevant citations. Rule 0 explicitly carves out small-talk: respond naturally, no tool call. This is simpler and zero-cost compared to a pre-classification LLM call.

**Why internal `_id` not `hadith_url` for citations?**
`_id` is the project's canonical identifier (Qdrant point id = `hadith_indices[0]`). It maps directly to `GET /api/v2/hadiths/{id}`. The `hadith_url` is an external bukhari-pedia.net URL — useful as a deep link but not as an internal reference.
