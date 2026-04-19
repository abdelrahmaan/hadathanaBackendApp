# Plan: Filter Out Low-Relevance Citations Before They Reach the LLM

## Context

The REFS mechanism we built earlier works — but the LLM over-cites. Example: user asks "هل النفقة على الزوجة صدقة؟" and the LLM receives 5 hadiths from the reranker. It cites all 5 (`REFS:[1,2,3,4,5]`) even though hadiths 4 and 5 are only tangentially related (about عمر بن الخطاب and صدقات النبي). The REFS line faithfully passes all 5 through.

**Root cause:** The LLM never sees the relevance scores. The Cohere reranker already computes a `relevance_score` (0.0–1.0) per doc, but the `search_hadiths` tool strips it before formatting results for the LLM. So the LLM has no signal to distinguish a 0.9 hit from a 0.3 hit.

**Fix (two layers):**
1. **Pre-filter** — Drop docs scoring below a threshold before the LLM sees them (removes noise)
2. **Show scores** — Include the relevance score in the tool output so the LLM can make informed citation decisions on borderline cases

---

## Changes (3 files, ~15 lines)

### 1. `app/chatbot/config.py` — Add threshold constant

```python
RELEVANCE_SCORE_THRESHOLD = 0.3  # drop docs below this before showing to LLM
```

Line 10, after `FETCH_K`. Using 0.3 as a safe floor — Cohere reranker scores cluster bimodally (relevant > 0.6, noise < 0.2). The 0.3 floor only removes clearly irrelevant results. Can tune later using existing reranker logs.

### 2. `app/chatbot/agent.py` — Filter docs + show scores to LLM

Lines 54-68 in `search_hadiths` tool:

**A.** After `docs = _retriever.invoke(query)`, filter:
```python
from app.chatbot.config import RELEVANCE_SCORE_THRESHOLD

docs = [d for d in docs if d.metadata.get("relevance_score", 0.0) >= RELEVANCE_SCORE_THRESHOLD]
```

**B.** Stash the *filtered* list (so router citation indices stay in sync):
```python
if thread_id:
    _last_docs[thread_id] = docs
```

**C.** Include score in the formatted output:
```python
f"[{i + 1}] (relevance: {d.metadata.get('relevance_score', 0.0):.2f}) "
f"{d.metadata.get('title', '') or d.metadata.get('book', '')}\n{d.page_content}"
```

**D.** Log how many docs were filtered:
```python
logger.info("tool_results_ready", extra={..., "count": len(docs), "filtered_out": pre_filter_count - len(docs)})
```

### 3. `app/chatbot/prompts.py` — Instruct LLM to use scores

Update Rules 2 and 4:

**Rule 2:** `"Answer ONLY from the results returned by the tool — never from prior knowledge. Each passage has a relevance score (0.0–1.0). Only cite passages that directly answer the user's question — do not cite tangentially related passages."`

**Rule 4:** `"Each passage is numbered [1], [2], etc. with a relevance score. When referencing a passage, cite its number. Do NOT cite a passage just because it shares a keyword with the question — it must provide direct evidence for your answer."`

---

## No changes needed

| File | Why |
|---|---|
| `router.py` | REFS parsing + citation filtering unchanged. Indices stay in sync because we filter *before* stashing in `_last_docs` |
| `retriever.py` | Reranker already provides `relevance_score` in metadata — no change needed |
| `config.py` constants | `RERANK_TOP_N = 5` stays — we still fetch 5 from reranker, just filter before LLM |

---

## How it works end-to-end

```
Query → Qdrant hybrid (top 20) → Cohere rerank (top 5 with scores)
  → Pre-filter: drop docs < 0.3 (e.g. 5 → 3)
  → Format with scores: "[1] (relevance: 0.92) أجر النفقة...\n..."
  → LLM sees 3 passages with scores, cites only relevant ones
  → REFS:[1,2] → Router filters citations to [1,2]
```

Before: LLM sees 5 docs (no scores) → cites all 5 → user sees 5 sources, 2 irrelevant
After: LLM sees 3 docs (with scores) → cites 2 → user sees 2 relevant sources

---

## Verification

1. `make test-chatbot` — existing unit tests pass (mock docs have `relevance_score` in metadata)
2. `make test-chatbot-dev` — smoke tests against live dev API
3. Manual: `curl -N -X POST http://localhost:8001/api/v2/chat -d '{"question":"هل النفقة على الزوجة هل هي صدقة؟"}'`
   - Check `assistant_message_complete` → citations should be 2-3, not 5
   - Check content tokens don't show `(relevance: ...)` to user (it's only in the tool output to the LLM, stripped by the agent layer)
4. Check logs for `filtered_out` count to verify threshold is working
