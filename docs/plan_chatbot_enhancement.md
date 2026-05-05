# Chatbot Enhancement Plan

## Context

The chatbot is a FastAPI SSE-streaming RAG agent built with LangChain `create_agent()`, Qdrant hybrid search, and Cohere reranking. After a full audit, several high-severity issues and improvement opportunities were identified spanning memory, retrieval quality, answer quality, and best-practice alignment.

---

## Current State: What Works Well

- Hybrid retrieval (dense Cohere embed-v4.0 + BM25 sparse) is correctly set up
- Cohere multilingual reranker (`rerank-multilingual-v3.0`) is properly wired
- `create_agent()` is called correctly with `checkpointer=InMemorySaver()`
- SSE streaming event sequence is well-structured
- Citation extraction via REFS line is a clever, working pattern
- Quota enforcement, ownership checks, and error handling are solid
- Logging at every pipeline stage is thorough

---

## Issues Found (Prioritized)

### 🔴 CRITICAL — Memory Is Broken

**Root cause**: The `InMemorySaver` checkpointer IS passed to `create_agent()` and keyed by `thread_id = session.session_id`. This means LangGraph internally accumulates message state across turns for the same thread.

**BUT** — `router.py:118` only sends the current turn:
```python
{"messages": [{"role": "user", "content": request.question}]}
```

Because the checkpointer already has state for this `thread_id`, LangGraph **appends** this new message to the existing stored state. So memory technically accumulates in `InMemorySaver` — but **it resets on every server restart**, meaning:
- In dev (with `--reload`): memory resets on every code change
- In prod: memory resets on every container restart / deploy

MongoDB sessions (`session.messages`) are written after each turn and loaded on each request, but **never fed back into the agent**. The two systems are disconnected.

**Impact**: Users think the chatbot remembers context (same session_id returned to them), but after any server restart the memory is gone silently.

### 🟡 HIGH — `topic` and `book` Filters Not Implemented

`ChatRequest` accepts `topic: str | None` and `book: str | None` but these are **never passed** to the retriever or search tool. The search always queries the full corpus regardless.

### 🟡 HIGH — Retriever Called Synchronously (Blocking Event Loop)

`_retriever.invoke(query)` in `search_hadiths` (agent.py:55) is a **synchronous** call inside an async context. This blocks the async event loop during retrieval (~200-500ms Cohere API call).

### 🟠 MEDIUM — Relevance Threshold Too Low

`RELEVANCE_SCORE_THRESHOLD = 0.1` passes nearly everything. Cohere reranker scores below 0.3 are typically noise. This means low-quality docs are shown to the LLM and may produce tangential answers.

### 🟠 MEDIUM — Title Generation Adds Latency After Stream

`_generate_title()` in router.py:197 makes a **separate LLM API call** after streaming ends. This adds ~500ms-1s to perceived completion time, and re-instantiates `init_chat_model()` on every request (expensive).

### 🟠 MEDIUM — `init_chat_model()` Created Per Request in `_generate_title`

`_generate_title` creates a new `init_chat_model()` instance on every call (router.py:61-67). This bypasses connection pooling.

### 🟠 MEDIUM — No Query Expansion / Reformulation

For follow-up questions like "ما حكمه؟" (What is its ruling?), the tool receives a context-free query with no reference to the prior topic. Since the InMemorySaver state has the prior context, the LLM _should_ produce a better query — but this isn't guaranteed. Adding explicit query reformulation would help.

### 🟡 HIGH — No Exception Handling in the Retriever Tool

`_retriever.invoke(query)` can throw (network error, Qdrant down, Cohere API failure). There is no try/except — unhandled exceptions propagate to the agent and produce opaque errors.

### 🔵 LOW — `_last_docs` dict can grow unbounded

`_last_docs: dict[str, list]` uses `pop()` which is correct — but if a stream errors before `get_last_docs()` is called, the entry is never cleaned up. Over time this is a memory leak.

---

## Enhancement Plan

### 1. Fix Memory: Seed Agent from MongoDB on Every Request

**Files**: `app/chatbot/router.py`

**What**: When calling `agent.astream()`, prepend MongoDB session history as messages so the LangGraph checkpointer is seeded with prior context, even after a server restart.

```python
# Build full message list from session history
history_messages = [
    {"role": msg.role, "content": msg.content}
    for msg in session.messages
]
input_messages = history_messages + [{"role": "user", "content": request.question}]

async for chunk in agent.astream(
    {"messages": input_messages},
    stream_mode="messages",
    config={"configurable": {"thread_id": session.session_id}},
):
```

**Tradeoff**: Long sessions will consume more tokens. Mitigate by capping history (last N turns) or summarizing. Start with last 10 turns.

**Critical detail**: With `InMemorySaver`, if the thread already has stored state, appending messages to it will cause duplicates. We need to either:
- Option A: Clear the checkpointer state for this thread before seeding (complex)
- Option B: Drop `InMemorySaver` and rely solely on MongoDB history (simpler, more reliable)

**Recommended: Option B** — remove `checkpointer` from `create_agent()`, always reconstruct full history from MongoDB. This makes memory durable across restarts with zero added complexity.

**File changes**:
- `app/chatbot/agent.py`: Remove `_checkpointer = InMemorySaver()` and `checkpointer=_checkpointer` from `create_agent()`
- `app/chatbot/router.py:117-121`: Build `input_messages` from `session.messages[-10:]` + current question

### 2. Fix Async: Use `ainvoke` in the Tool

**Files**: `app/chatbot/agent.py`

**What**: Replace `_retriever.invoke(query)` with `await _retriever.ainvoke(query)`. The `search_hadiths` tool definition must be `async def`.

```python
@tool
async def search_hadiths(query: str, config: RunnableConfig) -> str:
    ...
    docs = await _retriever.ainvoke(query)
```

Both `_LoggingRetriever._aget_relevant_documents()` and `_LoggingReranker.acompress_documents()` are already implemented — we just need to call them.

### 3. Implement Topic/Book Filtering

**Files**: `app/chatbot/agent.py`, `app/chatbot/router.py`

**What**: Pass `topic` and `book` from `ChatRequest` through to the tool, then apply Qdrant payload filters.

**Approach**: Store `topic`/`book` from the request in the agent config's `configurable` dict so the tool can read them without changing the tool signature visible to the LLM.

```python
# router.py
config={"configurable": {
    "thread_id": session.session_id,
    "topic": request.topic,
    "book": request.book,
}}

# agent.py tool
topic = (config.get("configurable") or {}).get("topic")
book = (config.get("configurable") or {}).get("book")
```

Then pass as Qdrant filter conditions via `search_kwargs`:
```python
filters = []
if topic:
    filters.append(FieldCondition(key="topics", match=MatchAny(any=[topic])))
if book:
    filters.append(FieldCondition(key="book_plain", match=MatchValue(value=book)))
```

### 4. Raise Relevance Threshold

**Files**: `app/chatbot/config.py`

Change `RELEVANCE_SCORE_THRESHOLD = 0.1` → `0.25`. At 0.1, Cohere reranker scores near noise floor. 0.25 is a reasonable floor for Arabic hadith retrieval (monitor via existing logs).

### 5. Fix Title Generation: Module-Level Model + Parallel Execution

**Files**: `app/chatbot/router.py`, `app/chatbot/agent.py`

**What**: 
- Move title model to a module-level singleton in `agent.py` (same pattern as main agent)
- Run title generation in parallel with streaming (not after), using `asyncio.create_task()`

```python
# Start title generation as background task when streaming begins
title_task = asyncio.create_task(_generate_title(request.question))
# ... stream tokens ...
title = await title_task  # awaited after stream, usually already done
```

### 6. Add Error Handling in Tool

**Files**: `app/chatbot/agent.py`

Wrap `_retriever.ainvoke(query)` in try/except:
```python
try:
    docs = await _retriever.ainvoke(query)
except Exception as e:
    logger.error("retrieval_error", extra={"event": "retrieval_error", "error": str(e)})
    return "حدث خطأ أثناء البحث. يرجى المحاولة مرة أخرى."
```

### 7. Improve System Prompt for Multi-Turn Context

**Files**: `app/chatbot/prompts.py`

Add a rule about follow-up questions using prior context:
```
8. When the user asks a follow-up question (e.g. "ما حكمه؟", "اشرح أكثر"), 
   reformulate your search query to include the topic from prior context 
   before calling search_hadiths.
```

---

## Files to Modify

| File | Change |
|------|--------|
| `app/chatbot/agent.py` | Remove InMemorySaver, make tool async, add error handling, fix title model singleton |
| `app/chatbot/router.py` | Seed agent from session history, add topic/book to config, parallel title generation |
| `app/chatbot/config.py` | Raise RELEVANCE_SCORE_THRESHOLD to 0.25 |
| `app/chatbot/prompts.py` | Add rule 8 for follow-up query reformulation |

---

## What We Will NOT Change

- Retriever architecture (hybrid + rerank is correct and working)
- FETCH_K=20, RERANK_TOP_N=5 (reasonable, adjust only if metrics show gaps)
- SSE event sequence (well-designed, frontend depends on it)
- Citation REFS extraction pattern (clever and working)
- Session CRUD endpoints (correct)
- MongoDB as session store (stays — becomes the single source of truth for memory)

---

## Verification

1. `pytest tests/ -v` — all existing tests must pass
2. Manual: Start a session, ask a question, restart the server, ask a follow-up → agent should still know prior context
3. Manual: Send a request with `topic="الطهارة"` → verify only that topic is retrieved
4. Check logs: `retrieval_candidates` count = 20, `reranking_done` scores should mostly be > 0.25 post-fix
5. Timing: Title generation should complete before or concurrent with stream end (not add latency)
