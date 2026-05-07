# LangSmith Observability Implementation Plan — ✅ COMPLETED 2026-05-06

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete LangSmith tracing so every chat request appears as a single end-to-end trace with `session_id`, `user_id`, and nested child spans for retrieval, LLM, and title generation.

**Final architecture** (evolved past the original spec — see "Final Architecture" section at the bottom):
1. `async with trace("Hadathana_agent", ...) as run:` — creates a **visible** named parent run in the LangSmith UI (the original `tracing_context()` is invisible — sets env only).
2. `with tracing_context(parent=run):` wraps `agent.astream()` — bridges the parent into LangChain's tracer for LangGraph child runs.
3. `langsmith_extra={"parent": run}` is passed to `generate_title(...)` — `asyncio.create_task` runs in a fresh context, so the parent must be threaded through explicitly.

**Tech Stack:** `langsmith>=0.2.0`, `langsmith.trace`, `langsmith.tracing_context`, `langsmith.traceable`

---

## File Map

| File | Action | Change |
|---|---|---|
| `requirements.txt` | Modify | Add `langsmith>=0.2.0` |
| `app/main.py` | Modify | Update env var names to modern LANGSMITH_* names |
| `app/chatbot/agent.py` | Modify | Fix syntax bug; add `@traceable` to `generate_title`; remove from `build_agent` |
| `app/chatbot/router.py` | Modify | Import `tracing_context`; wrap astream loop + title await in `tracing_context` block |

---

### Task 1: Pin `langsmith` in requirements.txt

**Files:**
- Modify: `requirements.txt`

`langsmith` is imported in `agent.py` but not pinned — a fresh Docker build could silently install an incompatible version.

- [x] **Step 1: Add the pin**

Open `requirements.txt`. After line 44 (`langchain-community>=0.3.0`), add:

```
langsmith>=0.2.0
```

The Chatbot section (lines 41–46) should now read:

```
# Chatbot (app/chatbot/)
langchain>=0.3.15
langchain-qdrant>=0.2.0
langchain-community>=0.3.0
langsmith>=0.2.0
qdrant-client>=1.12.0
fastembed>=0.4.0
```

- [x] **Step 2: Verify the package resolves**

```bash
/home/abdo_kamar/Projects/.venv/bin/pip show langsmith
```

Expected: a version line like `Version: 0.2.x` or higher. If not installed yet:

```bash
/home/abdo_kamar/Projects/.venv/bin/pip install "langsmith>=0.2.0"
```

- [x] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: pin langsmith>=0.2.0 in requirements"
```

---

### Task 2: Update env var names in `app/main.py`

**Files:**
- Modify: `app/main.py:19-22`

The current code sets `LANGCHAIN_TRACING_V2`, `LANGCHAIN_API_KEY`, `LANGCHAIN_PROJECT`. The modern LangSmith SDK reads `LANGSMITH_TRACING`, `LANGSMITH_API_KEY`, `LANGSMITH_PROJECT`. Both sets work for now, but the old names are being deprecated.

- [x] **Step 1: Update the env var block**

In `app/main.py`, replace lines 18–22:

```python
# Wire LangSmith tracing — must happen before any LangChain import
if settings.get_langsmith_api_key():
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = settings.get_langsmith_api_key()
    os.environ["LANGCHAIN_PROJECT"] = settings.get_langsmith_project()
```

with:

```python
# Wire LangSmith tracing — must happen before any LangChain import
if settings.get_langsmith_api_key():
    os.environ["LANGSMITH_TRACING"] = "true"
    os.environ["LANGSMITH_API_KEY"] = settings.get_langsmith_api_key()
    os.environ["LANGSMITH_PROJECT"] = settings.get_langsmith_project()
```

- [x] **Step 2: Verify the app still starts**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "from app.main import app; print('ok')"
```

Expected: `ok` (no import errors).

- [x] **Step 3: Commit**

```bash
git add app/main.py
git commit -m "chore: use canonical LANGSMITH_* env var names"
```

---

### Task 3: Fix agent.py — syntax bug, `@traceable` on `generate_title`, remove from `build_agent`

**Files:**
- Modify: `app/chatbot/agent.py`

Three changes in one file, committed together:

1. **Syntax bug** (line 118): `wrap_gemini(init_chat_model(` is missing the closing `)` for `init_chat_model` before `wrap_gemini` closes.
2. **Add `@traceable`** to `generate_title()` so it appears as a named child span per request.
3. **Remove `@traceable(name="Alrawi")`** from `build_agent()` — startup function, not a per-request trace.

- [x] **Step 1: Apply all three changes to `app/chatbot/agent.py`**

**Change A** — remove `@traceable(name="Alrawi")` from `build_agent` (line 58):

```python
# Before:
@traceable(name="Alrawi")
def build_agent() -> None:

# After:
def build_agent() -> None:
```

**Change B** — add `@traceable` to `generate_title` (currently at line 39, before the `async def`):

```python
# Before:
async def generate_title(question: str) -> str:
    """Generate a short Arabic title for a chat thread from the user's first question.

    Uses a module-level plain chat model singleton built in build_agent() — kept
    separate from the main agent (one create_agent per orchestrator only).
    Safe to call as a background task — failures fall back to the truncated question.
    """

# After:
@traceable(name="generate_title", run_type="chain")
async def generate_title(question: str) -> str:
    """Generate a short Arabic title for a chat thread from the user's first question.

    Uses a module-level plain chat model singleton built in build_agent() — kept
    separate from the main agent (one create_agent per orchestrator only).
    Safe to call as a background task — failures fall back to the truncated question.
    """
```

**Change C** — fix syntax bug at lines 111–118. The current code is:

```python
    model = wrap_gemini(init_chat_model(
        settings.chatbot_model,
        model_provider="openai",
        base_url="https://openrouter.ai/api/v1",
        api_key=settings.openrouter_api_key,
        max_tokens=1000,
        streaming=True,
    )
```

Fix it to:

```python
    model = wrap_gemini(init_chat_model(
        settings.chatbot_model,
        model_provider="openai",
        base_url="https://openrouter.ai/api/v1",
        api_key=settings.openrouter_api_key,
        max_tokens=1000,
        streaming=True,
    ))
```

(Added one closing `)` on line 118 to properly close `wrap_gemini(`.)

- [x] **Step 2: Verify no syntax errors**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m py_compile app/chatbot/agent.py && echo "syntax ok"
```

Expected: `syntax ok`

- [x] **Step 3: Run the test suite**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/ -v
```

Expected: all previously passing tests still pass (this task has no new tests — syntax fix + decorators are not logic changes).

- [x] **Step 4: Commit**

```bash
git add app/chatbot/agent.py
git commit -m "fix: correct wrap_gemini syntax; add @traceable to generate_title; remove from build_agent"
```

---

### Task 4: Add `tracing_context` to `router.py`

**Files:**
- Modify: `app/chatbot/router.py`

This is the key change: wrap the `agent.astream()` loop and the `title_task` await inside a `tracing_context` block so they share a parent trace with `session_id`, `user_id`, and `app_env` attached as metadata.

`tracing_context` is a synchronous context manager that works inside `async def` functions — it sets thread-local state that LangSmith reads when creating child runs.

- [x] **Step 1: Add imports at the top of `router.py`**

After the existing imports (around line 24, after `from app.config import settings`), add:

```python
from langsmith.run_helpers import tracing_context
```

- [x] **Step 2: Wrap the streaming block in `event_stream()`**

Find the `try:` block inside `event_stream()` (currently at line 99). Wrap the contents of that `try` block — the `agent.astream()` loop and the post-stream processing — inside a `tracing_context`. The title_task await (line 192) should also be inside the context so `generate_title` appears as a child span.

Replace the block from line 99 to line 196 (the `try/except` + post-stream logic up to and including `yield _sse({"type": "stream_end"})`):

```python
        try:
            with tracing_context(
                project_name=settings.get_langsmith_project(),
                metadata={
                    "session_id": session.session_id,
                    "user_id": str(user.id),
                    "app_env": settings.app_env,
                },
                tags=[settings.app_env],
            ):
                logger.info(
                    "pipeline_llm_start",
                    extra={"event": "pipeline_llm_start", "session_id": session.session_id},
                )
                # Build history: last 3 QA pairs (up to 6 messages) from MongoDB
                history = [
                    {"role": msg.role, "content": msg.content}
                    for msg in session.messages[-6:]
                ]
                input_messages = history + [{"role": "user", "content": request.question}]

                async for chunk in agent.astream(
                    {"messages": input_messages},
                    stream_mode="messages",
                    config={"configurable": {"thread_id": session.session_id}},
                ):
                    token = _extract_token(chunk)
                    if not token:
                        continue
                    assembled.append(token)
                    tail = (tail + token)[-120:]  # keep last 120 chars to detect REFS line
                    # Suppress tokens once the REFS line has started — it's metadata, not content
                    if "\nREFS:" not in tail:
                        yield _sse({"type": "content", "content": token})

                full_content = "".join(assembled)

                # Extract REFS line written by the LLM (e.g. "REFS:[1,3]") and strip it from content.
                # refs_found=True means the LLM wrote REFS (even if empty). refs_found=False means no line
                # at all — we fall back to showing all docs so citations are never silently lost.
                refs_match = re.search(r'\nREFS:\[([^\]]*)\]\s*$', full_content)
                referenced: set[int] = set()
                refs_found: bool = refs_match is not None
                if refs_match:
                    raw = refs_match.group(1).strip()
                    if raw:
                        referenced = {int(x.strip()) for x in raw.split(",") if x.strip().isdigit()}
                    full_content = full_content[:refs_match.start()].rstrip()

                logger.info(
                    "pipeline_llm_done",
                    extra={
                        "event": "pipeline_llm_done",
                        "session_id": session.session_id,
                        "response_chars": len(full_content),
                        "refs": sorted(referenced),
                    },
                )

                # Use docs already retrieved during the tool call — no second retrieval needed.
                logger.info(
                    "pipeline_citations_start",
                    extra={"event": "pipeline_citations_start", "session_id": session.session_id},
                )
                citation_docs = get_last_docs(session.session_id)
                citations = [
                    Citation(
                        resource_id=str(d.metadata.get("_id", "")),
                        text_span=(d.page_content or "")[:200],
                        confidence=float(d.metadata.get("relevance_score", 0.0)),
                        title=d.metadata.get("title", ""),
                        hadith_url=d.metadata.get("hadith_url", ""),
                    )
                    for i, d in enumerate(citation_docs, start=1)
                    # Filter to only cited hadiths. Two fallback cases:
                    # - no REFS line at all (refs_found=False): show all docs (graceful degradation)
                    # - REFS:[] (refs_found=True, referenced={}): show nothing (LLM cited none)
                    if not refs_found or i in referenced
                ]
                logger.info(
                    "pipeline_citations_done",
                    extra={"event": "pipeline_citations_done", "session_id": session.session_id, "citations": len(citations)},
                )

                yield _sse({
                    "type": "assistant_message_complete",
                    "data": {
                        "message_type": "assistant",
                        "content": full_content,
                        "citations": [c.model_dump() for c in citations],
                    },
                })

                title = await title_task
                logger.info(
                    "pipeline_done",
                    extra={"event": "pipeline_done", "session_id": session.session_id, "title": title},
                )
                yield _sse({"type": "thread_rename", "title": title})
                yield _sse({"type": "stream_end"})

        except Exception as e:
            logger.error(
                "chat_stream_error",
                extra={"event": "chat_stream_error", "session_id": session.session_id, "error": str(e)},
            )
            title_task.cancel()
            yield _sse({"type": "error", "content": "حدث خطأ أثناء المعالجة."})
            yield _sse({"type": "stream_end"})
            return
```

Note: the `except` block moves outside the `with tracing_context` block so errors are caught at the `try` level, not inside the context manager.

- [x] **Step 3: Verify no syntax errors**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m py_compile app/chatbot/router.py && echo "syntax ok"
```

Expected: `syntax ok`

- [x] **Step 4: Run the test suite**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/ -v
```

Expected: all tests pass. The `tracing_context` import is safe when `langsmith` is installed; it is a no-op when `LANGSMITH_TRACING` is not set.

- [x] **Step 5: Commit**

```bash
git add app/chatbot/router.py
git commit -m "feat: wrap chat stream in tracing_context with session_id/user_id metadata"
```

---

### Task 5: Verify end-to-end in LangSmith

This is a manual verification task — no code changes.

- [x] **Step 1: Confirm `LANGSMITH_API_KEY_DEV` is set in `.env`**

Open `.env` and check that `LANGSMITH_API_KEY_DEV` has a real key (not empty). If you don't have a key, get one from https://smith.langchain.com → Settings → API Keys.

- [x] **Step 2: Start the dev stack**

```bash
make dev
```

Wait for `agent_built` log line to appear in `make dev-logs`.

- [x] **Step 3: Send a chat request**

```bash
# Replace <TOKEN> with a valid JWT from /auth/login
curl -s -X POST http://localhost:8001/api/v2/chat \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <TOKEN>" \
  -d '{"question": "ما هو أول حديث في صحيح البخاري؟"}' \
  --no-buffer
```

Expected: SSE events stream back, ending with `stream_end`.

- [x] **Step 4: Open LangSmith and inspect the trace**

1. Go to https://smith.langchain.com → project `hadathana_dev`
2. Find the most recent run
3. Confirm:
   - Root run has metadata: `session_id`, `user_id`, `app_env`
   - Tag: `dev`
   - Child span: `hadiths_search` (run_type=tool)
   - Child span: `generate_title` (run_type=chain)
   - LLM call nested under the agent node

- [x] **Step 5: Commit verification note (optional)**

If all checks pass, no code change needed. The plan is complete.

---

## Final Architecture (as shipped — supersedes Task 4 above)

The original Task 4 used `with tracing_context(project_name=..., metadata=..., tags=...)` alone. That worked, but produced **no visible parent run** in the LangSmith UI — just sibling root runs with shared metadata. After three iterations we landed on the following pattern, which produces a single visible parent named `Hadathana_agent` with all child runs nested below it.

### What changed vs. the original Task 4

| Original | Shipped |
|---|---|
| `with tracing_context(project_name=..., metadata=...)` (no visible parent) | `async with trace("Hadathana_agent", run_type="chain", project_name=..., metadata=..., tags=..., inputs=...) as run:` (visible named parent) |
| Title generation: relied on contextvars | Title generation: explicit `langsmith_extra={"parent": run}` (contextvars are reset by `asyncio.create_task`) |
| LangGraph nesting: relied on contextvars | LangGraph nesting: `with tracing_context(parent=run):` wraps `agent.astream(...)` (LangChain's tracer reads the contextvar set by `tracing_context`, not by `trace()`) |
| Title computed *after* stream ends | `title_task = asyncio.create_task(...)` started early, awaited after stream ends — runs concurrently |

### Final code shape ([app/chatbot/router.py](app/chatbot/router.py))

```python
from langsmith import trace, tracing_context

async with trace(
    "Hadathana_agent",
    run_type="chain",
    project_name=settings.get_langsmith_project(),
    metadata={
        "session_id": session.session_id,
        "user_id": str(user.id),
        "app_env": settings.app_env,
    },
    tags=[settings.app_env],
    inputs={"question": request.question},
) as run:
    # Parallel @traceable child — pass parent explicitly because create_task
    # schedules the coroutine in a fresh context (contextvar gets reset)
    title_task = asyncio.create_task(
        generate_title(request.question, langsmith_extra={"parent": run})
    )

    # LangChain/LangGraph child — bridge the parent into LangChain's tracer
    # via the contextvar that tracing_context sets
    with tracing_context(parent=run):
        async for chunk in agent.astream(
            {"messages": input_messages},
            stream_mode="messages",
            config={"configurable": {"thread_id": session.session_id}},
        ):
            ...

    try:
        title = await title_task
    except asyncio.CancelledError:
        title = request.question[:50]
    run.end(outputs={"title": title, "response_chars": len(full_content)})
```

### Resulting waterfall (verified in LangSmith UI)

```
Hadathana_agent
├── generate_title (parallel)
│   └── ChatOpenAI
└── LangGraph
    ├── model → ChatOpenAI
    └── tools → search_hadiths
        └── ContextualCompressionRetriever
            ├── _LoggingRetriever
            └── VectorStoreRetriever
```

### Why two different bridges are needed

| Child | Bridge | Reason |
|---|---|---|
| `@traceable` async fn under `asyncio.create_task` | `langsmith_extra={"parent": run}` | `create_task` runs in a fresh contextvar context — the parent set by `trace()` is not inherited |
| LangChain Runnable (`.astream` / `.ainvoke`) | `with tracing_context(parent=run):` | LangChain's `LangChainTracer` reads the contextvar that `tracing_context` sets — `trace()` alone does NOT set it in the calling coroutine |

### Reference

LangSmith docs — [Combining decorated code with LangGraph and LangChain](https://docs.smith.langchain.com/observability/how_to_guides/nest_traces#combining-decorated-code-with-langgraph-and-langchain).

### Anti-patterns we tried and abandoned

1. **Manually seeding `LangChainTracer.run_map` and `order_map`, passing a custom `AsyncCallbackManager` in `config["callbacks"]`** — fights the framework. `tracing_context` is the official bridge.
2. **Relying on `asyncio.create_task` to inherit the LangSmith parent automatically** — broken: contextvars are reset on `create_task`. Use `langsmith_extra={"parent": run}` (or `copy_context().run(...)`).
3. **Using `tracing_context(parent=run)` to wrap the `create_task` call** — `tracing_context` is sync; the contextvar is set in the caller's frame, not in the spawned task. Use `langsmith_extra` instead.
