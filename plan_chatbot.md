# Plan: Smart Hadith Chatbot (V1 → V2 → V3)

**Decisions locked in:**
- Vector store: **Qdrant** (self-hosted Docker, hybrid dense+BM25)
- Reranker: **Cohere rerank-multilingual-v3.0** (Arabic-aware)
- LLM: **OpenRouter** (model-agnostic, you already have the key)
- Framework: **LangChain** `create_agent` + `@tool` (current API)
- Streaming: **SSE** from day one
- Code location: `app/chatbot/`
- Stateless V1/V2, tool-use agent V3

---

## Existing assets — reuse, do not re-embed

| Asset | Location | What it contains |
|---|---|---|
| Matn embeddings | `processed_podia_books.matn_embedding` (Mongo) | 7,075 × 1536-dim Cohere vectors |
| Topics | `processed_podia_books.topics` (Mongo) | 7,075 × Arabic topic arrays |
| Narrator bios | `processed_podia_narrator_biographies` (Mongo) | 1,780 narrators with `tarajim[]` |
| Normalization | [app/normalization.py](app/normalization.py) | `normalize_for_search()` — Arabic text |
| Existing routers | [app/routers/](app/routers/) | hadiths_podia, narrators_podia, search_podia |

---

## Stack

```
Qdrant (Docker :6333)           ← vector + hybrid index (derived from Mongo)
MongoDB (Docker :27017)         ← source of truth (unchanged)
FastAPI app/chatbot/            ← new chatbot package inside existing app
LangChain create_agent          ← agent orchestration (V1: simple RAG tool, V3: multi-tool)
Cohere embed-multilingual-v3.0  ← query embedding (same model as stored vectors)
Cohere rerank-multilingual-v3.0 ← reranker after retrieval
OpenRouter                      ← LLM (model: qwen/qwen3-235b-a22b or similar Arabic-capable)
SSE                             ← streaming responses
```

**Why Cohere reranker even in V1:**
Hybrid retrieval (dense + BM25) gives a good top-20. Reranker collapses it to the best top-5
for the LLM context window. Cohere's multilingual reranker is explicitly trained on Arabic and
is the strongest open-API option here. Latency cost: ~300ms — worth it for precision on
religious content where a wrong hadith is worse than no hadith.

---

## Directory layout

```
app/
  chatbot/
    __init__.py
    config.py          # chatbot-specific settings (extends app/config.py)
    qdrant.py          # QdrantClient lifecycle (mirrors app/database.py)
    indexer.py         # build/sync Qdrant collections from Mongo (called by scripts)
    retriever.py       # LangChain retriever wrapping Qdrant hybrid + Cohere rerank
    agent.py           # create_agent() with @tool definitions
    prompts.py         # Arabic system prompts (single source of truth)
    router.py          # FastAPI: POST /api/v2/chat (SSE)
    models.py          # Pydantic: ChatRequest, ChatResponse, HadithHit, NarratorHit

scripts/
  sync_qdrant.py       # CLI: populate/resync Qdrant from Mongo (both collections)

tests/
  test_chatbot_v1.py
  test_chatbot_v2.py
  test_chatbot_v3.py
```

No files at root. No scattered service files.

---

## V1 — Matn Q&A

### Flow

```
POST /api/v2/chat  {question, topic?, book?, k?}
  1. embed question     → Cohere embed-multilingual-v3.0 (input_type="search_query")
  2. hybrid search      → Qdrant collection `hadiths_matn`
                           dense: 1536-dim cosine
                           sparse: BM25 (FastEmbedSparse "Qdrant/bm25")
                           filter: topics[] if provided, book if provided
                           prefetch top-20
  3. rerank             → Cohere rerank-multilingual-v3.0 → top-5
  4. agent              → LangChain create_agent, single tool `search_hadiths`
                           model: OpenRouter (qwen3 or gemini-2.0-flash — Arabic-capable, cheap)
                           system prompt: Arabic "answer only from context, cite [1]–[5], refuse if absent"
  5. SSE stream         → token-by-token + final citations JSON chunk
```

Steps 1–3 are inside the `search_hadiths` tool. The agent calls it once, gets context,
then streams the answer. No multi-turn in V1 — each request is stateless.

### Qdrant collection `hadiths_matn`

```python
# One point per hadith
{
  "id": <int hadith_index>,
  "vector": {
      "dense": [...],          # matn_embedding from Mongo (1536-dim)
      "sparse": <BM25 sparse>  # computed by FastEmbedSparse on matn_text_plain
  },
  "payload": {
      "hadith_url": "...",
      "hadith_indices": [...],
      "book": "...",
      "chapter": "...",
      "matn_text": "...",        # with tashkeel, for display
      "matn_text_plain": "...",  # for BM25 and snippet
      "topics": [...],           # indexed array filter
      "rawi_ids": [...]          # indexed, used in V2 filter
  }
}
```

### Key code patterns (confirmed against current LangChain docs)

```python
# app/chatbot/retriever.py
from langchain_qdrant import QdrantVectorStore, RetrievalMode, FastEmbedSparse
from langchain_cohere import CohereRerank
from langchain.retrievers import ContextualCompressionRetriever
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

def build_hadiths_retriever(client: QdrantClient, cohere_embeddings, k_fetch=20, k_final=5):
    vector_store = QdrantVectorStore(
        client=client,
        collection_name="hadiths_matn",
        embedding=cohere_embeddings,
        sparse_embedding=FastEmbedSparse(model_name="Qdrant/bm25"),
        retrieval_mode=RetrievalMode.HYBRID,
        vector_name="dense",
        sparse_vector_name="sparse",
    )
    base = vector_store.as_retriever(search_kwargs={"k": k_fetch})
    reranker = CohereRerank(model="rerank-multilingual-v3.0", top_n=k_final)
    return ContextualCompressionRetriever(base_compressor=reranker, base_retriever=base)


# app/chatbot/agent.py
from langchain.agents import create_agent
from langchain.tools import tool
from langchain.chat_models import init_chat_model

def build_agent(retriever, system_prompt: str):
    @tool(response_format="content_and_artifact")
    def search_hadiths(query: str):
        """Search Bukhari hadiths by meaning. Use for any question about hadith content."""
        docs = retriever.invoke(query)
        text = "\n\n".join(
            f"[{i+1}] {d.page_content}\nSource: {d.metadata.get('hadith_url')}"
            for i, d in enumerate(docs)
        )
        return text, docs

    model = init_chat_model(
        "openrouter/qwen/qwen3-235b-a22b",   # swap model in config, not here
        base_url="https://openrouter.ai/api/v1",
        api_key=settings.openrouter_api_key,
    )
    return create_agent(model, [search_hadiths], system_prompt=system_prompt)
```

```python
# app/chatbot/router.py  — SSE streaming
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from .agent import build_agent
import json

router = APIRouter(prefix="/api/v2/chat", tags=["chatbot"])

@router.post("")
async def chat(req: ChatRequest):
    agent = get_agent()   # singleton from lifespan
    async def stream():
        async for chunk in agent.astream(
            {"messages": [{"role": "user", "content": req.question}]}
        ):
            if token := chunk.get("token"):
                yield f"data: {json.dumps({'token': token})}\n\n"
        yield "data: [DONE]\n\n"
    return StreamingResponse(stream(), media_type="text/event-stream")
```

### New infra

**docker-compose.yml** — uncomment the qdrant stub, add:
```yaml
qdrant:
  image: qdrant/qdrant:v1.12.0
  volumes: ["qdrant_data:/qdrant/storage"]
  ports: ["6333:6333"]   # dev only; prod: remove port, internal only

qdrant-init:
  build: .
  command: python scripts/sync_qdrant.py
  depends_on:
    qdrant: {condition: service_healthy}
    mongo-init: {condition: service_completed_successfully}
  restart: "no"
```

`sync_qdrant.py` reads `processed_podia_books` from Mongo, reuses `matn_embedding` (no
re-embedding), upserts into Qdrant. Idempotent. Runs in <60s for 7k docs.

### New dependencies

```
# requirements.txt additions
langchain>=0.3.0
langchain-qdrant>=0.2.0
langchain-cohere>=0.4.0
langchain-community>=0.3.0
qdrant-client>=1.12.0
fastembed>=0.4.0          # for BM25 sparse vectors (FastEmbedSparse)
```

OpenRouter is accessed via `langchain-openai` (already present) with a custom `base_url`.
No new package needed.

### New env vars

```bash
# .env.example additions
OPENROUTER_API_KEY=sk-or-...      # already present (used by tag_topics_jsonl.py)
COHERE_API_KEY=...                # already present (used by embed_matn_jsonl.py)
QDRANT_URL=http://qdrant:6333     # docker service name
QDRANT_API_KEY=                   # empty for self-hosted, set for Qdrant Cloud
CHATBOT_MODEL=qwen/qwen3-235b-a22b  # swappable without code change
```

### Verification

```bash
# Qdrant populated
curl http://localhost:6333/collections/hadiths_matn | jq .result.points_count
# → 7075

# Chat works, streams Arabic
curl -X POST http://localhost:8001/api/v2/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"ما حكم الصلاة في وقتها؟"}' --no-buffer

# Topic filter
curl -X POST http://localhost:8001/api/v2/chat \
  -d '{"question":"كيف أتوضأ؟","topic":"الطهارة"}'

# Refusal (must not fabricate)
curl -X POST http://localhost:8001/api/v2/chat \
  -d '{"question":"ما رأيك في كرة القدم؟"}'
```

---

## V2 — Narrator chatbot (biographies)

### What's new

1. **New Qdrant collection `narrators_bio`** — one point per narrator (1,780)
   - New embeddings needed: `scripts/embed_narrators.py` concatenates
     `full_name_plain + rank_plain + tarajim[].tarjama_plain` per narrator,
     embeds with Cohere embed-multilingual-v3.0, writes `narrator_embeddings.jsonl`
   - `sync_qdrant.py` extended to also populate `narrators_bio`
   - Payload: `rawi_id`, `full_name`, `full_name_plain`, `rank`, `url`

2. **Second tool** `search_narrators` added to the agent — same pattern as `search_hadiths`

3. **Agent queries both collections** — no intent classifier. At 1,780 + 7,075 points both
   searches cost <5ms total. The LLM synthesizes from whichever hits are relevant.

4. **Cross-corpus tool** `hadiths_by_narrator(rawi_id)` — first tool that bridges corpora:
   Mongo lookup by `rawi_id`, returns hadith summaries. The agent calls this after
   `search_narrators` resolves a name to a `rawi_id`.

5. **Updated system prompt** — cites `[hadith:url]` and `[narrator:rawi_id]` separately.

### Why separate collections (not one tagged collection)

Dense + BM25 scores are not comparable across hadith matns (long, ~50 words) vs narrator
biographies (long structured text). Mixing them in one collection means the reranker would
compare scores with different baselines. Two collections, two retrievers, one reranker pass
each — clean.

### New files

| File | Action |
|---|---|
| `scripts/embed_narrators.py` | New — Cohere embeddings for narrator bios, resumable |
| `scripts/sync_qdrant.py` | Extended — adds narrator upsert after hadith upsert |
| `app/chatbot/retriever.py` | Add `build_narrators_retriever()` |
| `app/chatbot/agent.py` | Add `search_narrators` and `hadiths_by_narrator` tools |
| `app/chatbot/prompts.py` | Updated dual-source Arabic system prompt |

---

## V3 — Relational narrator chatbot (Neo4j)

### Goal

Questions that require graph traversal:
- "ما أقصر سلسلة رواة بين الحميدي والبخاري؟"
- "من هم الرواة المشتركون بين حديث 1 وحديث 5؟"
- "كل تلاميذ ابن عيينة من الدرجة الثانية"

### What changes

1. **Neo4j as a compose service** — formalize the current manual `docker start neo4j-hadith`:
   ```yaml
   neo4j:
     image: neo4j:5.20-community
     volumes: ["neo4j_data:/data"]
     environment:
       NEO4J_AUTH: "neo4j/${NEO4J_PASSWORD}"
     ports: ["7474:7474", "7687:7687"]   # dev only

   neo4j-init:
     command: python mongo_migration/processed_bukhari_podia/build_graph.py
     depends_on: [neo4j, mongo-init]
     restart: "no"
   ```

2. **`app/chatbot/graph.py`** — typed, parametrized Cypher helpers only.
   **The LLM never writes Cypher.** Hard rule.
   ```python
   async def shortest_chain(driver, name_a: str, name_b: str) -> list[dict]: ...
   async def k_hop_students(driver, rawi_id: int, depth: int) -> list[dict]: ...
   async def common_narrators(driver, hadith_idx_a: int, hadith_idx_b: int) -> list[dict]: ...
   ```

3. **Three new tools** added to the agent:
   - `narrator_chain_between(name_a, name_b)` → calls `shortest_chain`
   - `narrator_students(rawi_id, depth)` → calls `k_hop_students`
   - `common_narrators_between_hadiths(idx_a, idx_b)` → calls `common_narrators`

4. **Model upgrade for V3 only** — tool orchestration with 6 tools and Arabic reasoning:
   switch `CHATBOT_MODEL` to `google/gemini-2.0-flash` or `anthropic/claude-sonnet-4-6`
   via OpenRouter. V1/V2 stay on cheap model.

5. **Bounded tool loop** — `max_iterations=4` in `create_agent`. Hard cap, no exceptions.

### New files

| File | Action |
|---|---|
| `app/chatbot/graph.py` | New — async Neo4j helpers (parametrized Cypher only) |
| `app/chatbot/agent.py` | Add 3 graph tools, set max_iterations=4 |
| `app/chatbot/qdrant.py` | Add Neo4j driver lifecycle hook |
| `tests/test_chatbot_v3.py` | Tool-dispatch tests with mock Neo4j |

---

## Honest criticisms of this plan

1. **`create_agent` is a new API** — `langchain.agents.create_agent` appears in the current
   docs but is not yet in a stable release as of this writing. It may rename or change
   signature. Mitigation: pin `langchain>=0.3.15`, write a thin wrapper so agent construction
   is in one place (`app/chatbot/agent.py`), easy to swap.

2. **FastEmbedSparse requires `fastembed`** — this pulls a ~300MB model download on first run.
   The `qdrant-init` container must have internet access or a pre-baked model cache.
   Mitigation: add `fastembed` model download to the Dockerfile build step.

3. **Cohere reranker adds ~300ms latency per request.** For SSE streaming this is felt as
   a pause before the first token. Consider running retrieval + rerank in the background while
   streaming a "Searching..." indicator. Or accept the latency — it's bounded and predictable.

4. **OpenRouter model availability is not guaranteed.** If `qwen/qwen3-235b-a22b` goes down,
   the whole chatbot is down. Mitigation: `CHATBOT_MODEL` in env — operators can swap model
   without code deploy. Add a `/api/v2/chat/health` that pings OpenRouter.

5. **Qdrant drift.** If Mongo data is updated and `sync_qdrant.py` isn't rerun, the chatbot
   answers from stale vectors. Mitigation: document in CLAUDE.md "Data update workflow" as a
   mandatory step; add a version/checksum check to `sync_qdrant.py`.

6. **No Arabic eval set yet.** We have 7k hadiths but no ground-truth Q&A pairs to measure
   retrieval@5. Ship V1, then build a 30-question eval set from the topic list before V2.

---

## Files to create/modify — complete list

### V1

| File | Create/Modify | Notes |
|---|---|---|
| `app/chatbot/__init__.py` | Create | Empty |
| `app/chatbot/config.py` | Create | `QDRANT_URL`, `CHATBOT_MODEL`, etc. |
| `app/chatbot/qdrant.py` | Create | QdrantClient lifecycle |
| `app/chatbot/indexer.py` | Create | Build Qdrant collections from Mongo |
| `app/chatbot/retriever.py` | Create | Hybrid + rerank retriever builders |
| `app/chatbot/agent.py` | Create | `create_agent` + `search_hadiths` tool |
| `app/chatbot/prompts.py` | Create | Arabic system prompt strings |
| `app/chatbot/router.py` | Create | `POST /api/v2/chat`, SSE |
| `app/chatbot/models.py` | Create | `ChatRequest`, `ChatResponse` |
| `app/main.py` | Modify | Register chatbot router + lifespan hooks |
| `app/config.py` | Modify | Add chatbot env vars |
| `scripts/sync_qdrant.py` | Create | CLI sync script |
| `docker-compose.yml` | Modify | Add qdrant + qdrant-init services |
| `docker-compose.prod.yml` | Modify | Add qdrant (no exposed port) |
| `requirements.txt` | Modify | Add langchain-qdrant, fastembed |
| `.env.example` | Modify | Add QDRANT_URL, CHATBOT_MODEL |
| `tests/test_chatbot_v1.py` | Create | Unit + integration tests |
| `tasks.md` | Modify | New section v1.4 chatbot |
| `README.md` | Modify | Chat endpoint docs |
| `CLAUDE.md` | Modify | Qdrant in architecture, sync step in data workflow |

### V2 additions

| File | Create/Modify |
|---|---|
| `scripts/embed_narrators.py` | Create |
| `app/chatbot/retriever.py` | Modify — add narrator retriever |
| `app/chatbot/agent.py` | Modify — add 2 narrator tools |
| `app/chatbot/prompts.py` | Modify — dual-source prompt |
| `tests/test_chatbot_v2.py` | Create |

### V3 additions

| File | Create/Modify |
|---|---|
| `app/chatbot/graph.py` | Create |
| `app/chatbot/agent.py` | Modify — add 3 graph tools, max_iterations=4 |
| `docker-compose.yml` | Modify — add neo4j + neo4j-init |
| `tests/test_chatbot_v3.py` | Create |

---

## Rollout order

1. Qdrant infra + `sync_qdrant.py` — verify 7075 points
2. `app/chatbot/` skeleton (qdrant.py, config.py, models.py)
3. Retriever (hybrid + rerank) — unit test with mock data
4. Agent + router — SSE endpoint live on dev `:8001`
5. Manual eval on 10 Arabic questions, tune system prompt
6. Tests pass, update docs, promote to prod
7. V2: embed narrators, second collection, two tools
8. V2: eval on narrator questions, update docs
9. V3: Neo4j compose service, graph.py, three tools, upgrade model
10. V3: eval on graph questions, update docs
