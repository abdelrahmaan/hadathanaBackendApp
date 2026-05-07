# Hadathna - Project Tasks

> **For Claude**: Before starting any task, mark the relevant item as `in_progress`. After finishing, update status, add a summary, and list touched files. Both updates are mandatory — see CLAUDE.md Workflow Rules.

---

## v0.1 - Data Acquisition & Scraping (DONE)

### Shamela.ws - Firecrawl Pipeline
- [x] Scrape Sahih Al-Bukhari (book/1681): **7,230 hadiths**
- [x] Scrape narrator profiles: **1,527 narrators** (kunya, nasab, death dates, jarh wa ta'dil)
- [x] Retry failed pages and track failures
- [x] 2-phase narrator ID resolution: exact match + context rules → **87.8% coverage**

### Bukhari-Pedia - Playwright Pipeline
- [x] Scrape bukhari-pedia.net: **7,008 hadiths** with structured narrator chains
- [x] Stage 0: Clean narrators (remove tashkeel, footnotes, punctuation)
- [x] Stage 1: Match narrator names → podia_rawi_id (5-level matching) → **98.5% coverage** (44,055/44,733)
- [x] Stage 2: Match Podia narrators → tarajm.com IDs → **97.5% coverage** (992/1,017)
- [x] Stage 3: Enrich Bukhari hadiths with tarajm candidates

### Tarajm.com - BFS Crawler
- [x] Automated BFS-based people discovery crawler
- [x] Persistent state tracking with auto-resume
- [x] CSV output with structured person data (name, summary, biography, teachers/students)

---

## v0.2 - Narrator Disambiguation (DONE)

### Chain Extraction
- [x] GPT-4o extraction with Pydantic structured outputs (7,563 hadiths)
- [x] Matn segmentation and chain structure validation

### Disambiguation Engine
- [x] Extract 1,555 ambiguous name-student pairs
- [x] Rule engine with ~200 rules from علم الرجال
  - [x] Unambiguous dictionary (260+ entries)
  - [x] Student-based context rules (14 ambiguous names)
  - [x] Pronoun resolution (117+ father-son pairs)
  - [x] 1,529/1,555 pairs resolved (98.3%)
- [x] Bug fixes: removed historically impossible fallbacks, fixed rule conflicts

### Name Normalization
- [x] 3-step resolution pipeline (context → static → identity)
- [x] Current coverage: **75.0%** (33,560/44,733 mentions)
  - Unambiguous: 13,894 (31.1%)
  - Context-disambiguated: 2,718 (6.1%)
  - Pronoun-resolved: 781 (1.7%)
  - Static mapping: 16,167 (36.1%)
  - Unmapped: 11,173 (25.0%)
- [x] Generated deterministic narrator IDs: `NAR_<12-char-SHA256>`

---

## v0.3 - Data Preprocessing & MongoDB Migration (DONE)

### Shamela Pipeline
- [x] `preprocess_pages.py` → `hadith_pages.jsonl` (page-level metadata)
- [x] `preprocess_hadiths.py` → `preprocessed_bukhari.jsonl` (hadiths with chains)
- [x] Narrator profiles → `narrators.jsonl` (with jarh wa ta'dil)

### Podia Pipeline
- [x] `preprocess.py` → `bukhari_podia_hadiths.jsonl` (7,008 hadiths)
- [x] Deduplicated narrators → `bukhari_podia_narrators.jsonl`
- [x] Narrator biographies → `narrators_tarajem.jsonl`
- [x] Text normalization: 3-level hierarchy (raw → clean → plain)

### MongoDB Upload
- [x] Batch upsert (500 docs/batch) to 6+ collections
- [x] Shamela collections: `raw_shamela_books`, `raw_shamela_narrators`, `raw_shamela_hadith_pages`, `analytics_narrator_stats_shamela`
- [x] Podia collections: `processed_podia_books`, `raw_podia_books`, `processed_podia_narrators`, `processed_podia_narrator_biographies`, `analytics_narrator_stats_podia`
- [x] Index creation for optimized queries (unique, text, compound indexes)
- [x] Renamed all collections to new schema (prefixed by source: `raw_shamela_*`, `raw_podia_*`, `analytics_*`)

---

## v0.4 - FastAPI Backend & Statistics (DONE)

### Shamela Endpoints
- [x] `GET /api/v1/hadiths` - List with pagination, regex search (hadith_plain), filter by narrator_id, chain_type
- [x] `GET /api/v1/hadiths/{hadith_id}` - Single hadith detail
- [x] `GET /api/v1/narrators` - List with search (name_plain, kunya, nasab)
- [x] `GET /api/v1/narrators/{narrator_id}` - Single narrator detail
- [x] `GET /api/v1/narrators/{narrator_id}/stats` - Teacher/student relationships with frequency

### Podia Endpoints (v2)
- [x] `GET /api/v2/hadiths` - List with search (hadith_text_plain, rawi_id, book)
- [x] `GET /api/v2/hadiths/{hadith_index}` - Single hadith detail
- [x] `GET /api/v2/narrators` - List with search (full_name_plain, rank)
- [x] `GET /api/v2/narrators/{rawi_id}` - Single narrator detail
- [x] `GET /api/v2/narrators/{rawi_id}/tarajem` - Narrator biography
- [x] `GET /api/v2/narrators/{rawi_id}/stats` - Teacher/student stats

### Infrastructure
- [x] Async MongoDB via Motor with connection lifecycle management
- [x] Pydantic v2 models for request/response validation
- [x] CORS middleware (configurable origins, GET-only)
- [x] Health check endpoint (`GET /health`)
- [x] Environment-based configuration (pydantic-settings)

### Narrator Statistics
- [x] Compute teacher/student relationships from chain positions
- [x] Frequency-based ranking (sort by co-occurrence count)
- [x] Idempotent computation (safe to re-run)
- [x] Stats for both Shamela and Podia pipelines

### Docker
- [x] Dockerfile (Python 3.11-slim, uvicorn)
- [x] .dockerignore (excludes data files, venvs, .git)
- [x] Environment variable management via .env

---

## v0.5 - Frontend v1 (DONE)

- [x] Next.js with App Router, TypeScript, Tailwind CSS
- [x] Arabic font support (Noto Naskh Arabic) + RTL
- [x] Search page with filters
- [x] Hadith detail page with chain list view
- [x] Pure SVG graph visualization (no external lib)
- [x] Narrator side panel

---

## v1.0 - Data Quality & Coverage Improvement

- [ ] Auto-add ~2,303 full canonical names to dictionary → push coverage from 75% to ~95%
- [ ] Resolve remaining 186 ambiguous pairs (923 mentions) with teacher-context lookup
- [ ] Fix عبة → شعبة data extraction error (6 pairs, 19 mentions)
- [ ] Entity clustering for aliases (e.g., الزهري = ابن شهاب = محمد بن مسلم)
- [ ] Re-run MongoDB migration with improved data
- [ ] Validate data consistency across Shamela and Podia sources

---

## v1.1 - Data Enrichment

- [x] Generate structured Arabic summaries for narrator biographies
  - Script: `scripts/generate_narrator_summaries.py`
  - Adds `summary` field to `processed_podia_narrator_biographies` (1,780 narrators)
  - LLM: `google/gemini-3-flash-preview` via OpenRouter (kunya, era, location, notes)
  - Structured fields computed from `narrator_info` + `analytics_narrator_stats_podia`
  - Output: `mongo_migration/processed_bukhari_podia/narrator_summaries.jsonl`
- [ ] Enrich narrators with biographical data (birth/death dates, places, tabaqah)
- [ ] Add scholarly assessments (jarh wa ta'dil from multiple sources)
- [ ] Cross-reference teacher/student relationships with tarajm data
- [ ] Validate enrichment coverage and data consistency

---

## v1.2 - Neo4j Graph Database

- [ ] Ingest normalized data into Neo4j with V3 schema
- [ ] Nodes: Narrator (with variants), Hadith, Segment
- [ ] Relationships: NARRATED, NARRATED_HADITH, HAS_SEGMENT
- [ ] Batch processing with progress tracking
- [ ] Data validation (no duplicate nodes, valid references)
- [ ] Graph query API endpoints in FastAPI

---

## v1.3 - Semantic Search

- [ ] Evaluate Arabic embedding models (OpenAI, multilingual-e5, Arabic-specific)
- [ ] Generate embeddings for hadith matn segments
- [ ] Set up vector storage (MongoDB Atlas Vector Search or dedicated vector DB)
- [ ] Implement hybrid search (keyword + semantic)
- [ ] Add `GET /api/v2/hadiths/search/semantic?q=...` endpoint (returns results ranked by cosine similarity)
- [ ] Integrate with FastAPI search endpoints

---

## v1.4 - Arabic Text Normalization & Advanced Search

### Arabic Query Normalization (backend-only)
- [x] Implement `normalize_arabic_query(text)` utility: strip tashkeel, normalize alef variants (أ إ آ أ → ا), normalize ة → ه, normalize ى → ي, normalize hamzas (ؤ ئ → و ي), remove tatweel (ـ)
- [x] Apply normalization at query time on all search endpoints (`hadith_plain`, `name_plain`, `full_name_plain`)
- [x] Test: "أبو هريرة" and "ابوهريره" must return the same results
- [x] Affects: `app/routers/hadiths_shamela.py`, `app/routers/narrators_shamela.py`, `app/routers/hadiths_podia.py`, `app/routers/narrators_podia.py`

### Advanced / Fuzzy Search
- [ ] Evaluate options: MongoDB Atlas Search (Lucene-based), n-gram index, normalized token matching
- [ ] Implement chosen approach for partial/fuzzy Arabic text matching
- [ ] Replace or augment current `$regex` queries with the new search strategy
- [ ] Affects all v1 & v2 hadith + narrator text search params

### Hadith Topic Classification (offline pipeline)
- [x] Define topic taxonomy (47 Arabic topic tags, e.g., الصلاة، الصوم، العقيدة والتوحيد، الأخلاق والآداب)
- [x] Implement LLM-based classifier for hadith `matn_text` (Gemini Flash via OpenRouter) — `scripts/tag_topics_jsonl.py`
- [x] Run classification as offline pipeline step; store `topics: [...]` array in `processed_podia_books` — 7,075/7,076 hadiths tagged
- [x] Add `GET /api/v2/hadiths?topic=صيام` filter endpoint
- [x] Add `GET /api/v2/topics` endpoint (all topics with counts)

### Semantic Search (complements v1.3)
- [ ] Prerequisite: v1.0 data quality (narrator coverage 75% → 95%) must be done first
- [ ] Use `matn_text` from `processed_podia_books` as the embedding input (Shamela lacks matn segmentation)
- [ ] Add rate limiting / response caching before exposing the endpoint (prerequisite from v2.0)

---

## v2.0 - Production Release

### API Enhancements
- [x] Add API authentication and rate limiting (per-user daily quota: free=3/day, supporter=10/day, unlimited; 429 on breach)
- [ ] Add write endpoints (POST/PUT/DELETE) for data management
- [ ] Auto-generated API documentation (Swagger/ReDoc polish)
- [ ] Response caching for frequently accessed data
- [ ] API versioning strategy

### Frontend v2
- [ ] Update frontend to consume new MongoDB-backed API
- [ ] Add Podia data views (narrator biographies, tarajem)
- [ ] Improve graph visualization with narrator statistics
- [ ] Mobile optimization

### Testing & CI/CD
- [x] Data presence smoke tests — `tests/test_data_presence.py` covering Shamela hadiths/narrators, Podia hadiths/narrators, topics, stats, pagination, 404s, and health collections. Run with `APP_ENV=dev pytest tests/test_data_presence.py -v`.
- [x] Unit tests for API endpoints (pytest + httpx)
- [ ] Integration tests with MongoDB test database
- [x] CI/CD pipeline (GitHub Actions): run tests + linting on every PR, block merge to `main` if checks fail — `.github/workflows/ci.yml` (lint job: ruff; test job: pytest)
- [x] Automated linting and type checking (ruff in CI)
- [x] Branch protection (GitHub Ruleset on `main`): PRs required, status checks enforced
- [x] Auto-deploy on merge to `main` — `.github/workflows/deploy.yml` SSHes into VPS and runs `make prod`
- [x] Remove manual `tmux` restart workflow — deployment is now zero-touch via GitHub Actions CD
- [x] Health check after deploy — `make prod` retries `/health` for up to 120s before reporting success/failure
- [x] Release tagging — `make tag VERSION=v1.x.x` creates annotated git tag

### Deployment & Monitoring
- [ ] Deploy backend to cloud (Railway / Render / AWS)
- [ ] Deploy frontend to Vercel
- [ ] Error tracking (Sentry or similar)
- [x] Prometheus metrics (Phase 1) — request rate, latency, error rate via `/metrics`
- [x] Grafana dashboards (Phase 2) — API Overview dashboard with 11 panels
- [ ] Loki + Promtail log aggregation (Phase 3)
- [x] LangSmith LLM observability (Phase 4) — every chat request traced as `Hadathana_agent` parent run with `session_id`/`user_id`/`app_env` metadata; `generate_title` nested via `langsmith_extra={"parent": run}`; LangGraph nested via `tracing_context(parent=run)`. Touched: `app/main.py`, `app/chatbot/router.py`, `app/chatbot/agent.py`, `requirements.txt`. Plan: `docs/superpowers/plans/2026-05-05-langsmith-observability.md`. Docs: `README.md` § LangSmith Tracing, `CLAUDE.md` § Tracing & observability.
- [ ] Alerting (Phase 6)
- [x] Logging infrastructure

---

---

## v1.5 - Smart Chatbot

### V1 — Matn Q&A (in_progress)
- [x] Qdrant hybrid search infra (dense Cohere embed-multilingual-v3.0 + BM25 sparse via FastEmbedSparse)
- [x] Cohere rerank-multilingual-v3.0 as contextual compression layer
- [x] LangChain `create_tool_calling_agent` + `AgentExecutor` with `@tool search_hadiths`
- [x] OpenRouter LLM (model configurable via `CHATBOT_MODEL` env var)
- [x] `POST /api/v2/chat` SSE endpoint — exact event format with `assistant_message_start`, `content`, `assistant_message_complete` (with citations), `thread_rename`, `stream_end`
- [x] Authenticated session management: `POST /api/v2/chat` requires auth, persists `user_id` ownership, stores history in `chat_sessions`, and exposes `GET/DELETE /api/v2/chat/sessions...`
- [x] `scripts/sync_qdrant.py` — CLI to populate Qdrant from Mongo (idempotent)
- [x] `qdrant` + `qdrant-init` Docker Compose services added to dev + prod stacks
- [x] FastEmbed BM25 model pre-baked into Docker image
- [x] CORS updated to allow POST
- [x] 4 unit tests in `tests/test_chatbot_v1.py`
- [x] Dedicated session API tests in `tests/test_chatbot_sessions.py` covering auth, ownership, list/get/delete, and delete-then-404 flow
- [x] Per-user daily quota system — `free: 3/day`, `supporter: 10/day`, `unlimited: -1` — tier stored on user, counter in `user_quotas` collection, 429 with Arabic message + upgrade hint on limit
- [ ] Manual eval: test 10+ Arabic questions, tune system prompt
- [ ] Promote to prod after eval passes

### feat: per-user daily quota system (2026-04-20)
**Status**: done
**Summary**: Implemented tier-based daily request limits for the chatbot. Free users get 3 requests/day, supporter users get 10/day, unlimited users bypass quota entirely. Limits are env-var configurable. On limit breach, returns HTTP 429 with Arabic message + upgrade hint. Counter stored in `user_quotas` MongoDB collection with atomic `$inc` (no Redis needed). TTL index auto-cleans old counters after 2 days.
**Touched files**:
- `app/config.py` — added `quota_free_daily`, `quota_supporter_daily`, `quota_unlimited_daily`, `get_daily_limit()`
- `app/auth/models.py` — added `tier: str = "free"` to `User` and `UserRead`
- `app/auth/database.py` — `create()` sets `tier="free"` default
- `app/database.py` — added `get_user_quotas_collection()`, `get_quota_expiry()`, `ensure_indexes()`, `user_quotas` in `EXPECTED_COLLECTIONS`
- `app/chatbot/quota.py` — new: `check_quota` FastAPI dependency
- `app/chatbot/router.py` — wired `check_quota` into `POST /api/v2/chat`
- `.env.example` — added `QUOTA_FREE_DAILY`, `QUOTA_SUPPORTER_DAILY`, `QUOTA_UNLIMITED_DAILY`
- `tests/test_quota.py` — new: 4 unit tests (first/at-limit/over-limit/unlimited)
- `tests/test_chatbot_v1.py` — added 429 route-level test
- `tests/conftest.py` — updated mocks for quota collection

### feat: complete chatbot user sessions spec (2026-04-18)
**Status**: done
**Summary**: Linked chatbot sessions to authenticated users. `ChatSession` gains `user_id` + `title` fields. `POST /api/v2/chat` now requires auth and enforces session ownership (403 on mismatch). Title is persisted after `thread_rename` event. Added `GET /api/v2/chat/sessions`, `GET /api/v2/chat/sessions/{id}`, `DELETE /api/v2/chat/sessions/{id}` endpoints. MongoDB indexes added for both session collections. 9 new tests cover auth gate, ownership, list, detail, and delete flows.
**Touched files**:
- `app/chatbot/models.py` — added `user_id`, `title` to `ChatSession`; added `ChatSessionMeta`
- `app/chatbot/session.py` — `get_or_create_session` now takes `user_id` + ownership check; new `update_session_title`
- `app/chatbot/router.py` — auth gate on `POST /chat`; title persistence; 3 new session endpoints
- `mongo_migration/create_indexes.py` — `user_id+created_at` and `session_id` unique indexes for both session collections
- `tests/test_chatbot_sessions.py` — 9 tests covering all session flows

### V2 — Narrator biographies (pending)
- [ ] `scripts/embed_narrators.py` — Cohere embeddings for narrator bios
- [ ] Second Qdrant collection `narrators_bio` (1,780 points)
- [ ] Add `search_narrators` + `hadiths_by_narrator` tools to agent
- [ ] Updated dual-source system prompt

### V3 — Graph queries (pending)
- [ ] Neo4j as Docker Compose service
- [ ] `app/chatbot/graph.py` — parametrized Cypher helpers (no free-form LLM Cypher)
- [ ] 3 graph tools: `narrator_chain_between`, `narrator_students`, `common_narrators_between_hadiths`
- [ ] Upgrade to Claude Sonnet 4.6 via OpenRouter for tool-use orchestration

### Touched files (V1 enhancements — greeting guard, memory, citation fix)
- `app/chatbot/prompts.py` — Rule 0: greetings bypass tool call
- `app/chatbot/agent.py` — `InMemorySaver` checkpointer; `_last_docs` dict to stash tool docs; `config: RunnableConfig` param on `search_hadiths` to extract `thread_id`; `get_last_docs()` helper
- `app/chatbot/router.py` — `config={"configurable": {"thread_id": session.session_id}}` passed to `astream`; replaced post-stream retriever re-invoke with `get_last_docs()`
- `app/chatbot/CHATBOT.md` — updated agent flow diagram, pipeline logging table, design decisions

### Touched files (V1)
- `requirements.txt` — added langchain, langchain-qdrant, langchain-community, qdrant-client, fastembed
- `app/config.py` — added cohere_api_key, openrouter_api_key, qdrant_url, chatbot_model
- `app/database.py` — added chat_sessions to EXPECTED_COLLECTIONS, get_chat_sessions_collection()
- `app/main.py` — wired chatbot lifespan hooks + router, fixed CORS to allow POST
- `app/chatbot/` — new package: __init__, config, models, prompts (English), qdrant, indexer, retriever, agent, session, router
  - `session.py` writes to `chat_sessions_dev` (dev) or `chat_sessions_prod` (prod) based on `APP_ENV`
  - `prompts.py` written in English per convention
  - No `load_dotenv` needed — pydantic-settings reads `.env` automatically via `model_config`
- `scripts/sync_qdrant.py` — new CLI sync script
- `docker-compose.yml` — added qdrant + qdrant-init services, qdrant_data volume
- `docker-compose.override.yml` — dev config: qdrant port 6333 exposed, qdrant-init command, QDRANT_URL env
- `docker-compose.prod.yml` — prod config: qdrant internal-only, qdrant-init command, QDRANT_URL env
- `Dockerfile` — added scripts/ copy + fastembed model pre-download
- `tests/test_chatbot_v1.py` — 4 tests (session_id, SSE schema, Mongo save, refusal)
- `.env.example` — added QDRANT_URL, CHATBOT_MODEL
- `CLAUDE.md` — updated architecture + data workflow
- `README.md` — added chatbot endpoint docs

---

## Chatbot & AI Enhancements

> Improvements identified from live testing and pipeline analysis. Ordered by impact/effort ratio.

### High Impact — Low Effort

- [x] **Fix double retrieval** — tool stashes docs in `_last_docs[thread_id]`; router uses `get_last_docs()` after stream. Saves ~500ms, citation scores now use LLM-refined query (higher).
  - Files: `app/chatbot/router.py`, `app/chatbot/agent.py`

- [x] **Stateful multi-turn memory** — Initially backed by `InMemorySaver` checkpointer, but that reset on every server restart. **Superseded** by MongoDB-seeded history: each request now reconstructs the last 3 QA pairs from `session.messages` and prepends them to `agent.astream()` input. `InMemorySaver` removed entirely. Memory now durable across restarts.
  - Files: `app/chatbot/agent.py`, `app/chatbot/router.py`

- [x] **Greeting guard (Rule 0)** — added Rule 0 to system prompt: greetings and small-talk bypass `search_hadiths` entirely. LLM responds naturally with no citations.
  - Files: `app/chatbot/prompts.py`

- [x] **Citation filtering (REFS line)** — LLM appends `REFS:[1,3]` at end of response. Router strips it before emitting content events and filters `citation_docs` to only those indices. Fallback: if no REFS line, all docs shown; if `REFS:[]`, zero citations. Also fixed `resource_id` (was always `""`) by adding `_id` to Qdrant payload in `indexer.py`. Qdrant re-sync required for `resource_id` to populate on existing data.
  - Files: `app/chatbot/prompts.py`, `app/chatbot/router.py`, `app/chatbot/indexer.py`, `tests/test_chatbot_v1.py`

- [x] **Relevance score filtering** — Pre-filter retrieved docs below `RELEVANCE_SCORE_THRESHOLD` (0.3) before passing to LLM. Include reranker scores in tool output so LLM can judge borderline passages. Updated prompt Rules 2 and 4 to instruct score-aware citation behavior.
  - Files: `app/chatbot/config.py`, `app/chatbot/agent.py`, `app/chatbot/prompts.py`

- [x] **Memory durability + async retriever + parallel title generation + error handling** — replaced `InMemorySaver` (lost on every server restart) with explicit history seeding from MongoDB (`session.messages[-6:]` = last 3 QA pairs) so memory survives restarts; switched `search_hadiths` to `async def` + `await _retriever.ainvoke()` to stop blocking the event loop; wrapped retrieval in try/except returning a graceful Arabic fallback message; moved title generation to a module-level `_title_model` singleton (no more per-request `init_chat_model()`) and kicked it off via `asyncio.create_task()` in parallel with the main stream so the title is usually ready by the time streaming completes; reserved `create_agent()` for the main orchestrator only.
  - Files: `app/chatbot/agent.py`, `app/chatbot/router.py`, `tests/test_chatbot_v1.py`, `tests/test_chatbot_sessions.py`
  - Plan: `docs/plan_chatbot_enhancement.md`

- [ ] **Guardrail middleware** — wrap `create_agent` with `ToolCallLimitMiddleware(max_tool_calls=3)` to prevent runaway tool loops on ambiguous questions.
  - Files: `app/chatbot/agent.py`

### Medium Impact — Medium Effort

- [ ] **Explicit query rewriting** — add a lightweight pre-step that rewrites the user question into a clean Arabic search query before hitting Qdrant. The agent already does this implicitly (seen in logs), but making it explicit improves consistency for low-score cases (e.g. "ما فضل قراءة القرآن؟" scored 0.19).
  - Files: `app/chatbot/agent.py` or new `app/chatbot/rewriter.py`

- [ ] **`search_narrators` tool (V2)** — second Qdrant collection `narrators_bio` for narrator biographies (`processed_podia_narrator_biographies`, 1,780 docs). Agent picks the right tool based on question type (hadith content vs. narrator info).
  - Files: `app/chatbot/agent.py`, `app/chatbot/retriever.py`, `scripts/embed_narrators.py` (new)

- [x] **Swap `InMemorySaver` → durable storage** — Solved by removing `InMemorySaver` entirely and seeding the agent from MongoDB-stored `session.messages` on every request. No Redis needed; MongoDB is already the session-of-truth.
  - Files: `app/chatbot/agent.py`, `app/chatbot/router.py`

### Lower Priority

- [ ] **Graph tools (V3)** — Neo4j Cypher helpers exposed as agent tools: `narrator_chain_between`, `narrator_students`, `common_narrators_between_hadiths`. LLM never writes raw Cypher — all queries are parametrized Python functions.
  - Files: `app/chatbot/graph.py` (new), `app/chatbot/agent.py`

- [ ] **Manual eval suite** — test 20+ Arabic questions covering: direct matn lookup, narrator questions, topic questions, off-topic (should refuse), multi-turn follow-up. Document reranker scores per question type.
  - Files: `tests/eval_chatbot.md` (new)

- [ ] **Confidence score display** — reranker `confidence` (0.0–1.0) is already in every citation object. Frontend should display it as a relevance indicator (high/medium/low) on citation cards.
  - Frontend only — no backend change needed.

---

## Progress Summary

| Version | Description | Status |
|---------|-------------|--------|
| v0.1 | Data Acquisition & Scraping | Done |
| v0.2 | Narrator Disambiguation | Done |
| v0.3 | MongoDB Migration | Done |
| v0.4 | FastAPI Backend & Statistics | Done |
| v0.5 | Frontend v1 | Done |
| v1.0 | Data Quality & Coverage | Not started |
| v1.1 | Data Enrichment | Not started |
| v1.2 | Neo4j Graph Database | Not started |
| v1.3 | Semantic Search | Not started |
| v1.4 | Arabic Normalization & Advanced Search | In progress (topic classification done) |
| v2.0 | Production Release | Not started |

**Current version**: v0.5 (all foundational work complete)
**Next milestone**: v1.0 (boost narrator coverage to ~95%)

---

## Chore Log

### docs: document prod MongoDB port 27018 + Compass connection (2026-05-04)
**Status**: done
**Summary**: Updated docs to reflect that prod MongoDB exposes port `27018` on the host (not "internal only" as previously documented). Added a Compass connection block in `CLAUDE.md` covering both dev (`27017`) and prod (`27018`) URIs, with a note recommending SSH tunneling over public exposure for remote access.
**Touched files**:
- `README.md` (Environments table + Services and Ports table)
- `CLAUDE.md` (Environment allocation table + new "Connect from MongoDB Compass" section)
- `tasks.md`

### test: admin dashboard endpoints on dev (2026-05-03)
**Status**: done
**Summary**: Verified the admin dashboard endpoints end to end against the live dev stack on `http://localhost:8001`. Confirmed `GET /api/v2/admin/stats` returns `401` when unauthenticated, `403` for a regular authenticated user, and `200` for a superuser. Confirmed `GET /api/v2/admin/users` returns `200` with paginated data for a superuser, and `PATCH /api/v2/admin/users/{user_id}/tier` returns `200` and updates the tier correctly. Ran `tests/test_admin.py -v` afterward and all 10 tests passed. Dev `/health` was reachable during verification; its `degraded` status was due to an empty `chat_sessions_prod` collection rather than an admin-route failure.
**Touched files**:
- `README.md`
- `tasks.md`

### feat: admin dashboard backend — GET /api/v2/admin/stats (2026-05-03)
**Status**: done
**Summary**: Added superuser-only stats endpoint aggregating system health, user/quota breakdown, chatbot activity counts, and data collection sizes into a single JSON response. 3 new files, main.py updated, 4 tests.
**Touched files**:
- `app/models/admin.py` (new)
- `app/routers/admin.py` (new)
- `app/main.py` (added admin router)
- `tests/test_admin.py` (new — 4 tests)
- `tasks.md`

### test: auth and bookmarks test suite — Tasks 10–14 (2026-04-18)
**Status**: done
**Summary**: Wrote full test coverage for auth and bookmarks endpoints. All 40 tests pass; ruff reports no violations.
- Task 10: Updated `tests/conftest.py` with `make_mock_collection()` helper and patches for `get_auth_users_collection` and `get_bookmarks_collection`. Verified `test_auth_register.py` covers 201 success, 422 invalid email, 422 missing password.
- Task 11: Verified `test_auth_login.py` covers 400 unknown email and 422 missing fields.
- Task 12: Verified `test_auth_forgot_password.py` covers 202 for unknown and known email (with Resend mock). Verified `test_auth_reset_password.py` covers 400 invalid token and 422 missing token field.
- Task 13: Verified `test_bookmarks.py` covers 401 unauthenticated, 200 authenticated list, 201 add, 409 duplicate, 204 delete.
- Task 14: Ran `pytest tests/ -v` (40/40 passed) and `ruff check app/ tests/` (clean). Fixed one unused `importlib` import in `test_auth_register.py`.
**Key finding**: All test files were already present but `test_auth_register.py` had a stray `import importlib` (unused). Removed it to pass ruff.
**Touched files**:
- `tests/conftest.py`
- `tests/test_auth_register.py` (removed unused import)
- `tasks.md`

### feat: auth and bookmarks backend — Tasks 2–9 (2026-04-18)
**Status**: done
**Summary**: Implemented full authentication and bookmarks backend using fastapi-users v15.
- Task 2: Extended `Settings` with jwt_secret, token lifetimes, resend_api_key, from_email; updated .env.example.
- Task 3: Added `get_auth_users_collection()` and `get_bookmarks_collection()` to database.py.
- Task 4: Created `app/auth/` package with `User` (internal DB model with hashed_password), `UserRead`, `UserCreate`, `UserUpdate` Pydantic schemas.
- Task 5: Implemented `MotorUserDatabase` — custom `BaseUserDatabase` subclass for Motor async MongoDB (MongoDBUserDatabase was removed in v13+).
- Task 6: Configured `UserManager` (UUIDIDMixin, email hooks via Resend), HttpOnly cookie transport, HS256 JWT strategy, `auth_backend`, `current_active_user` dependency.
- Task 7: Added authenticated bookmarks router (`GET/POST/DELETE /api/v1/bookmarks`).
- Task 8: Updated main.py — mounted auth routes (/auth/login, /auth/register, /auth/forgot-password, /auth/verify), bookmarks router, slowapi rate limiter, updated CORS to allow POST/DELETE/OPTIONS.
- Task 9: Added MongoDB indexes for auth_users (unique email+id), TTL indexes for reset/refresh tokens, compound unique index for user_bookmarks.
**Key deviation**: fastapi-users v15 uses a separate `User` model (with hashed_password) as the `UP` type, distinct from `UserRead` (the public API schema with no hashed_password). The plan's `BaseUserManager[UserRead, UUID]` generic was incorrect — corrected to `BaseUserManager[User, UUID]`.
**Verification**: `from app.main import app` import OK; all 26 existing tests pass; ruff clean.
**Touched files**:
- `app/config.py`
- `.env.example`
- `app/database.py`
- `app/auth/__init__.py` (new)
- `app/auth/models.py` (new)
- `app/auth/database.py` (new)
- `app/auth/config.py` (new)
- `app/routers/bookmarks.py` (new)
- `app/main.py`
- `mongo_migration/create_indexes.py`
- `tasks.md`

### docs: update README and CLAUDE.md for CI/CD, Grafana, Prometheus, Qdrant (2026-04-15)
**Status**: done
**Summary**: Documented all new infrastructure added since last docs update. README now has a CI/CD section (GitHub Actions workflow, lint + test jobs, how to run locally) and a Monitoring section (Prometheus, Grafana with dashboard panel list, Qdrant). CLAUDE.md updated with full port allocation table for all services, linting commands, CI/CD pipeline docs, and a Monitoring section covering Prometheus configs, Grafana provisioning paths, and Qdrant collection details. Updated Testing & CI/CD checklist in tasks.md to reflect completed items.
**Touched files**:
- `README.md` (added Services and Ports table, CI/CD section, Monitoring section)
- `CLAUDE.md` (updated environment allocation table, added CI/CD and Monitoring sections under Common Commands)
- `tasks.md`

### chore: apply hadith titles to HadithDataDev (2026-04-11)
**Status**: done
**Summary**: Applied `hadith_titles.jsonl` (7,075 entries) to `processed_podia_books` via PyMongo `bulk_write($set)`. Each document now has a `title` field (short Arabic headline ≤150 chars). Also added `hadith_titles.jsonl` to the `ENRICHMENTS` list in `bootstrap_local_db.py` so future bootstrap runs apply titles automatically.
**Touched files**:
- `scripts/bootstrap_local_db.py` (added titles to ENRICHMENTS)
- `tasks.md`

### chore: README overhaul + data recovery guide (2026-04-10)
**Status**: done
**Summary**: Rewrote README with clearer structure. Added explicit dev/prod command sections (`make dev`, `make prod`, etc.), a "No Data? Recovery Guide" section covering how to diagnose + fix corrupted/missing collections (drop → re-import JSONL → apply enrichments via `$set` → rebuild indexes). Fixed the topics import incident — `mongoimport --mode=merge` replaced full hadith documents with slim `{hadith_url, topics}` records (7075 docs corrupted). Fixed by dropping collection, re-importing full JSONL, then applying topics via PyMongo `bulk_write($set)`. Added warning to CLAUDE.md prohibiting `mongoimport --mode=upsert/merge` for partial field updates.
**Touched files**:
- `README.md`
- `CLAUDE.md`
- `tasks.md`

### chore: import topics into HadithDataDev (2026-04-10)
**Status**: done
**Summary**: Topics were missing from `HadithDataDev` — `/api/v2/topics` returned empty results. First attempt used `mongoimport --mode=merge` which corrupted 7075 documents (replaced them with slim `{hadith_url, topics}` records). Recovery: dropped collection, re-imported from `bukhari_podia_hadiths.jsonl`, then applied topics via PyMongo `bulk_write($set)` which safely merges a single field without touching other data. Result: 7076 hadiths fully restored + 7075 with topics.
**Touched files**:
- `tasks.md`

### chore: chatbot smoke tests + Makefile test commands (2026-04-10)
**Status**: done
**Summary**: Added `tests/test_chatbot_smoke.py` — 6 integration tests that hit a live running API (dev `:8001` or prod `:8000`). Tests cover: health check, SSE event sequence, new session ID, multi-turn session, citation filtering (REFS line), REFS line not leaking into content, and greeting bypass. Added `make test-chatbot` (unit), `make test-chatbot-dev` (smoke vs dev), `make test-chatbot-prod` (smoke vs prod) to Makefile. Updated README with test commands section.
**Touched files**:
- `tests/test_chatbot_smoke.py` (new)
- `Makefile` (added test targets)
- `README.md` (added Testing section, updated data workflow)
- `tasks.md`

### feat: hadith title tagging script + citation enrichment (2026-04-08)
**Status**: done
**Summary**: Added `scripts/tag_titles_jsonl.py` — mirrors the topics script pattern, generates a short Arabic headline (≤150 chars) per hadith using Gemini Flash via OpenRouter. Resumable, JSONL-first, no MongoDB dependency. Updated Qdrant indexer to include `title` in the metadata payload, and enriched the `Citation` model with `title` and `hadith_url` fields so the frontend can display a proper headline and deep-link to the source.
**Touched files**:
- `scripts/tag_titles_jsonl.py` (new)
- `app/chatbot/indexer.py` (added `title` to `_PROJECTION` and metadata payload)
- `app/chatbot/models.py` (added `title: str = ""` and `hadith_url: str = ""` to `Citation`)
- `app/chatbot/router.py` (populate `title` and `hadith_url` when building citations)
- `tasks.md`

### feat: Grafana dashboards — Phase 2 monitoring (2026-04-06)
**Status**: done
**Summary**: Added Grafana with auto-provisioned Prometheus datasource and pre-built "Hadathana API Overview" dashboard. Dashboard has 11 panels: request rate (total + by endpoint), error rate (5xx/total), response latency (p50/p95/p99), latency by endpoint (p95), requests in progress, response size by endpoint, Python process memory (RSS + virtual), and 4 stat panels (total requests, current req/s, avg latency, RSS memory) with color-coded thresholds. Grafana container added to dev (port 3000) and prod (port 3001). Dashboard and datasource provisioned via files — no manual setup needed.
**Touched files**:
- `monitoring/grafana/provisioning/datasources/prometheus.yml` (new — Prometheus datasource with fixed UID)
- `monitoring/grafana/provisioning/dashboards/dashboards.yml` (new — dashboard file provider)
- `monitoring/grafana/dashboards/api-overview.json` (new — 11-panel API dashboard)
- `docker-compose.yml` (added `grafana` service)
- `docker-compose.override.yml` (Grafana dev port 3000, dev volume)
- `docker-compose.prod.yml` (Grafana prod port 3001, prod volume)
- `tasks.md`

### feat: Prometheus metrics monitoring — Phase 1 (2026-04-06)
**Status**: done
**Summary**: Added Prometheus metrics to FastAPI via `prometheus-fastapi-instrumentator`. The `/metrics` endpoint exposes RED metrics (request rate, error rate, duration/latency), Python process metrics (memory, CPU, GC), and per-handler breakdowns. Added Prometheus container to Docker Compose (dev on :9090, prod on :9091). Separate Prometheus configs for dev (scrapes api:8001) and prod (scrapes api:8000). Named volumes for metric retention (30 days).
**Touched files**:
- `requirements.txt` (added `prometheus-client`, `prometheus-fastapi-instrumentator`)
- `app/main.py` (added `Instrumentator().instrument(app).expose(app)`)
- `monitoring/prometheus.yml` (new — prod Prometheus config)
- `monitoring/prometheus.dev.yml` (new — dev Prometheus config, targets api:8001)
- `docker-compose.yml` (added `prometheus` service)
- `docker-compose.override.yml` (Prometheus dev port 9090, dev config override, dev volume)
- `docker-compose.prod.yml` (Prometheus prod port 9091, prod volume)
- `tasks.md`

### feat: Arabic search normalization — _search fields + normalized query pipeline (2026-04-06)
**Status**: done
**Summary**: Implemented full Arabic query normalization for all 4 search endpoints. Extended `normalize_for_search()` with ة→ه (taa marbuta) and ى→ي (alef maqsura) mappings. Added `_search` fields to all preprocessing scripts (shamela hadiths, shamela narrators, podia hadiths, podia narrators). Updated all 4 routers to normalize the incoming query via `normalize_for_search()` + `re.escape()` and match against the `_search` field instead of `_plain`. Added `_search` indexes to `create_indexes.py`. Added optional `_search` fields to all 4 Pydantic response models. 22 unit tests in `tests/test_normalization.py` (TDD: red → green). API query param names are unchanged (backward-compatible).
**Touched files**:
- `normalization.py` (ة→ه, ى→ي added to `normalize_for_search()`)
- `app/routers/hadiths_shamela.py` (normalize query, use `hadith_search` field)
- `app/routers/hadiths_podia.py` (normalize query, use `hadith_text_search` field)
- `app/routers/narrators_shamela.py` (normalize query, use `name_search`, `kunya_search`, `nasab_search`)
- `app/routers/narrators_podia.py` (normalize query, use `full_name_search` field)
- `app/models/hadith.py` (optional `hadith_search` field)
- `app/models/hadith_podia.py` (optional `hadith_text_search`, `sanad_text_search`, `matn_text_search`)
- `app/models/narrator.py` (optional `name_search`, `kunya_search`, `nasab_search`)
- `app/models/narrator_podia.py` (optional `name_in_chain_search`, `full_name_search`)
- `mongo_migration/processed_bukhari_shamela/preprocess_hadiths.py` (generate `hadith_search`)
- `mongo_migration/processed_bukhari_shamela/preprocess_pages.py` (generate `name_search`, `kunya_search`, `nasab_search`)
- `mongo_migration/processed_bukhari_podia/preprocess.py` (generate `hadith_text_search`, `sanad_text_search`, `matn_text_search`, `full_name_search`, `name_in_chain_search`)
- `mongo_migration/create_indexes.py` (added `_search` field indexes for all collections)
- `tests/test_normalization.py` (new — 22 unit tests)
- `tasks.md`



### fix: restore HadithDataDev after mongoimport upsert wiped hadith data (2026-04-02)
**Status**: done
**Summary**: `mongoimport --mode=upsert` on `hadith_topics.jsonl` replaced entire documents in `processed_podia_books` with slim `{hadith_url, topics}` records, destroying all hadith fields. Fixed by: (1) dropping the broken collection, (2) re-importing full `bukhari_podia_hadiths.jsonl`, (3) applying topics via PyMongo `bulk_write` with `$set` updates (merges field, does not replace document). Updated README to warn against `mongoimport --mode=upsert` for partial field updates. Confirmed Atlas (prod) was unaffected — topics were already present.
**Touched files**:
- `README.md` (replaced unsafe `mongoimport --mode=upsert` instructions with `$set` bulk write)
- `tasks.md`

### feat: JSONL-first topic tagging + matn embedding (2026-04-01)
**Status**: done
**Summary**: Two new offline enrichment scripts that read `bukhari_podia_hadiths.jsonl` directly (no MongoDB dependency). `scripts/tag_topics_jsonl.py` outputs slim `hadith_topics.jsonl` (`hadith_url` + `topics`) at repo root. `scripts/embed_matn_jsonl.py` outputs slim `hadith_embeddings.jsonl` (`hadith_url` + `matn_embedding`). Both are resumable by `hadith_url`, append-only, no merge back into source JSONL. Added `/*.jsonl` to `.gitignore` to cover root-level output files. Topics → import to MongoDB via `mongoimport --mode=upsert`. Embeddings → kept for Qdrant/vector DB (deferred).
**Touched files**:
- `scripts/tag_topics_jsonl.py` (new)
- `scripts/embed_matn_jsonl.py` (new)
- `.gitignore`
- `tasks.md`



### chore: self-hosted MongoDB + dev/prod environment split (2026-04-01)
**Status**: done
**Summary**: Atlas cluster became unreachable (shard-00-00 DOWN, port 27017 blocked). Spun up local MongoDB 8 in Docker (`mongodb-hadathana` container, volume `mongodb_hadathana_data`). Imported all collections into both `HadithData` (Atlas mirror) and `HadithDataDev` (dev DB). Added `APP_ENV` env var — `dev` routes to local Docker + `HadithDataDev`, `prod` routes to Atlas + `HadithData`. Config auto-switches URI, DB name, port, and CORS origins based on `APP_ENV`. Fixed `PodiaNarratorTarajem` and related models — `_plain`/`_clean` fields made optional since local JSONL files lack those preprocessing-derived fields. Ran `compute_stats.py` to populate `analytics_narrator_stats_podia` locally (stats are computed, not stored in JSONL). Production runs in `tmux hadathana_deployment` on port 8000.
**Touched files**:
- `app/config.py`
- `app/database.py`
- `app/models/narrator_podia.py`
- `.env` (added `APP_ENV`, `MONGODB_URI_LOCAL`, `DB_NAME_DEV`)
- `.env.example`
- `CLAUDE.md`
- `tasks.md`

### feat: topic listing and topic-filtered hadith endpoints (2026-03-31)
**Status**: done
**Summary**: Added `GET /api/v2/topics` (all distinct topics with counts) and `GET /api/v2/topics/{topic}/hadiths` (paginated hadiths by exact topic). New router `app/routers/search_podia.py`. Added `TopicCount` and `TopicsResponse` Pydantic models.
**Touched files**:
- `app/routers/search_podia.py` (new)
- `app/models/hadith_podia.py`
- `app/main.py`
- `tasks.md`

### feat: matn embedding + topic tagging (2026-03-30)
**Status**: done
**Summary**: Added `matn_embedding` (Cohere embed-v4, 1536-dim float32) and `topics` (1-3 Arabic semantic tags via Gemini Flash) to `processed_podia_books`. Offline script `scripts/embed_matn.py` uses LangChain (langchain-cohere for embeddings, langchain-openai for LLM via OpenRouter). Resumable, batched (96 embed / 20 LLM), exponential backoff. Also adds `?topic=` regex filter to `GET /api/v2/hadiths`. Run `python mongo_migration/create_indexes.py` after embedding to add the topics B-tree index; create the Atlas Vector Search index manually (instructions printed at end of script run).
**Touched files**:
- `scripts/embed_matn.py` (new)
- `requirements.txt`
- `.env.example`
- `mongo_migration/create_indexes.py`
- `app/models/hadith_podia.py`
- `app/routers/hadiths_podia.py`
- `tasks.md`

### fix: detect and patch duplicate rawi_id in advanced extraction data (2026-03-29)
**Status**: in_progress
**Summary**: Two different narrators (نافع and ابن عمر) share rawi_id=1568 in hadith 468 in `bukhari_pedia_advanced_extraction_results.json`. Writing a detection script to find all such conflicts, then patching the source JSON and re-running preprocessing.
**Touched files**:
- `scripts/detect_rawi_id_conflicts.py` (new)
- `bukhari_pedia_advanced_extraction_results.json` (data patch)
- `tasks.md`

### feat: structured JSON logging (2026-03-25)
**Status**: Done
**Summary**: Added structured JSON logging to the FastAPI app. Every HTTP request is logged with method, path, status code, and response time. Every list-endpoint search is logged with active filters and result count. Unhandled exceptions are caught in middleware, logged with full traceback, and return `{"detail": "Internal server error."}`. Logs write to `logs/app.log` (rotating, 10MB/5 backups) and stdout. 4 pytest tests added (TDD).
**Touched files**:
- `app/logging_config.py` (new)
- `app/middleware.py` (new)
- `app/main.py`
- `app/routers/hadiths_shamela.py`
- `app/routers/narrators_shamela.py`
- `app/routers/hadiths_podia.py`
- `app/routers/narrators_podia.py`
- `requirements.txt`
- `.gitignore`
- `tests/__init__.py` (new)
- `tests/conftest.py` (new)
- `tests/test_logging.py` (new)

### Fix: deduplicate Podia narrator stats by rawi_id (2026-03-23)
**Status**: done
**Summary**: Stats endpoint returned duplicate teacher/student entries for the same rawi_id with different Arabic grammatical name forms. Fixed compute_stats.py Stage 9 to group by rawi_id only (not name), matching the Shamela pipeline pattern. Requires re-running compute_stats.py to regenerate the analytics collection.
**Touched files**:
- `mongo_migration/processed_bukhari_podia/compute_stats.py`
- `tasks.md`

### Rename Podia collections: raw_ → processed_ for processed data, raw_ for actual raw scrape (2026-03-17)
**Status**: Done
**Summary**: Renamed Podia MongoDB collections so `raw_` consistently means unprocessed scrape data and `processed_` means pipeline-transformed data. `raw_podia_books` (was advanced extraction) → `processed_podia_books`. `raw_podia_raw_books` (was raw scrape) → `raw_podia_books`. `raw_podia_narrators` → `processed_podia_narrators`. `raw_podia_narrator_biographies` → `processed_podia_narrator_biographies`. Analytics collections unchanged.
**Touched files**:
- `app/database.py`
- `mongo_migration/upload.py`
- `mongo_migration/create_indexes.py`
- `mongo_migration/processed_bukhari_podia/compute_stats.py`
- `mongo_migration/rename_collections.py`
- `CLAUDE.md`
- `README.md`
- `tasks.md`

### Podia data quality fix: advanced extraction as primary source + store raw scrape (2026-03-16)
**Status**: Done
**Summary**: Rewrote `preprocess.py` to use `bukhari_pedia_advanced_extraction_results.json` (7,076 hadiths) as the primary MongoDB source. Primary documents (`raw_podia_books`) now include `sanad_text`, `matn_text`, `tawabi_text`, and `chains[]` with per-narrator transmission data (`transmission`, `transmission_type`, `is_explicit_hearing`, `role`). Added `process_raw_hadiths()` to also output raw scrape data (`raw_podia_raw_books`) with `full_name`, `rank`, `full_tooltip_info` per narrator as audit trail. Fixed `compute_stats.py` to unwind `chains[]` before computing teacher/student adjacency — previously the flat narrator array caused incorrect cross-chain pairings for 22.8% of hadiths. Added `raw_podia_raw_books` to `upload.py`, `create_indexes.py`, and `database.py`.
**Touched files**:
- `mongo_migration/processed_bukhari_podia/preprocess.py`
- `mongo_migration/processed_bukhari_podia/compute_stats.py`
- `mongo_migration/upload.py`
- `mongo_migration/create_indexes.py`
- `app/models/hadith_podia.py`
- `app/database.py`
- `CLAUDE.md`
- `tasks.md`



### MongoDB Collection Rename (2026-03-15)
**Status**: Done
**Summary**: Renamed all 8 MongoDB collections to a structured naming schema with source prefixes.
**Touched files**:
- `mongo_migration/rename_collections.py` (new — one-time Atlas migration script)
- `app/database.py`
- `mongo_migration/upload.py`
- `mongo_migration/create_indexes.py`
- `mongo_migration/processed_bukhari_shamela/compute_stats.py`
- `mongo_migration/processed_bukhari_podia/compute_stats.py`
- `CLAUDE.md`

### Rename Shamela router files to hadiths_shamela / narrators_shamela (2026-03-16)
**Status**: Done
**Summary**: Renamed `hadiths.py` → `hadiths_shamela.py` and `narrators.py` → `narrators_shamela.py` to match the `_shamela` naming pattern. Updated imports in `main.py` and stale references in `CLAUDE.md`.
**Touched files**:
- `app/routers/hadiths_shamela.py` (renamed from `hadiths.py`)
- `app/routers/narrators_shamela.py` (renamed from `narrators.py`)
- `app/main.py`
- `CLAUDE.md`
- `tasks.md`

### Rename Podia API endpoints from /api/v1/podia/* to /api/v2/* (2026-03-16)
**Status**: Done
**Summary**: Changed Podia router prefixes from `/api/v1/podia/hadiths` and `/api/v1/podia/narrators` to `/api/v2/hadiths` and `/api/v2/narrators` to hide the data source from external consumers.
**Touched files**:
- `app/routers/hadiths_podia.py`
- `app/routers/narrators_podia.py`
- `CLAUDE.md`
- `tasks.md`

### Update README.md with full data field comparison tables and advanced extraction docs (2026-03-16)
**Status**: Done
**Summary**: Expanded README.md Endpoints section with v1/v2 split endpoint tables, a full hadith/narrator field comparison table showing what each pipeline provides, a v2-only `/tarajem` field list, and a new section documenting the advanced extraction JSON (7,076 hadiths, chain types, transmission types) that feeds Neo4j.
**Touched files**:
- `README.md`
- `tasks.md`

### Update README.md with current collection names and pipeline commands (2026-03-16)
**Status**: Done
**Summary**: Updated README.md to reflect current MongoDB collection names (`analytics_narrator_stats_shamela`, `raw_shamela_*`, `raw_podia_*`), corrected env var from `MONGODB_URI` to `MONGODB_URI_READ`, and replaced stale pipeline steps with the current two-pipeline structure.
**Touched files**:
- `README.md`
- `tasks.md`

### chore: data change detection via git intent-to-add (2026-03-29)
**Status**: Done
**Summary**: Registered pulled data files (`mongo_migration/processed_bukhari_podia/*.jsonl`, `extract_data_v2/playwrite/*.jsonl`) with `git add --intent-to-add --force` so `git status` shows data changes without ever committing them. `.gitignore` still blocks staging. Workflow: `git status` to detect changes → `push_snapshot.py` if data changed → `git commit` for code only. Updated CLAUDE.md and scripts/r2_sync/README.md with the workflow.
**Touched files**:
- `.git/info/exclude` (cleaned — patterns stay in `.gitignore`)
- `CLAUDE.md` (added Change detection workflow section)
- `scripts/r2_sync/README.md` (added Change Detection Workflow section)
- `tasks.md`

### Cloudflare R2 data sync tooling (2026-03-29)
**Status**: Done
**Summary**: Added Python scripts under `scripts/r2_sync/` to push/pull dataset snapshots to/from Cloudflare R2 (S3-compatible). All data files (`.json`, `.jsonl`, `.csv`, `.xlsx`, `.parquet`) are now stored in R2 instead of git. Snapshots are stored under `snapshots/<dataset>/<YYYY-MM-DD>/`. Scripts use boto3 with multipart transfers (8 MB threshold), resume/retry on pull, tqdm progress bars. Added `data_snapshots/` to `.gitignore`, R2 env vars to `.env.example`, and usage docs.
**Touched files**:
- `scripts/r2_sync/config.py` (new — shared config, env validation, boto3 client factory)
- `scripts/r2_sync/list_snapshots.py` (new — list datasets and snapshots)
- `scripts/r2_sync/pull_snapshot.py` (new — download snapshots with resume/retry/progress)
- `scripts/r2_sync/push_snapshot.py` (new — upload folders with multipart/progress)
- `scripts/r2_sync/README.md` (new — full R2 usage docs and naming conventions)
- `.env.example` (added R2 vars)
- `.gitignore` (added `data_snapshots/`, untracked data files)
- `README.md` (added Data Snapshots section)
- `CLAUDE.md` (added Data Storage section, R2 sync commands, R2 env vars)
- `tasks.md`

### fix: backend serving empty data — add startup validation, bootstrap script, docker-compose (2026-04-04)
**Status**: done
**Summary**: Backend API returned empty results (`{"items":[], "total":0}`) because both local MongoDB databases (`HadithData`, `HadithDataDev`) had zero collections — data was lost (likely volume prune/recreation). This is the 2nd or 3rd occurrence. Fix: (1) re-import all JSONL data including topics and embeddings, (2) add startup validation that logs ERROR when collections are empty, (3) fix health endpoint to actually check MongoDB and report collection counts, (4) create `scripts/bootstrap_local_db.py` for automated recovery (imports JSONL + indexes + stats + enrichments in one command), (5) create `docker-compose.yml` with named volumes and auto-bootstrap on startup, (6) fix Dockerfile dynamic port. Also documented that production currently runs with `APP_ENV=dev` on port 8000 backed by local Docker MongoDB (Atlas was unreachable since 2026-04-01).
**Touched files**:
- `app/database.py` (startup validation, `validate_connection()`, `EXPECTED_COLLECTIONS`)
- `app/main.py` (health endpoint with MongoDB status + collection counts)
- `scripts/bootstrap_local_db.py` (new — full bootstrap: JSONL import + indexes + stats + topics/embeddings)
- `docker-compose.yml` (new — mongo + mongo-init + api with named volume)
- `Dockerfile` (dynamic port via `$PORT` env var)
- `tests/conftest.py` (patch `validate_connection` in test fixture)
- `tasks.md`
- `CLAUDE.md` (docker-compose docs, bootstrap script docs, current production state note)

### Rename analytics_narrator_stats → analytics_narrator_stats_shamela (2026-03-15)
**Status**: Done
**Summary**: Renamed `analytics_narrator_stats` to `analytics_narrator_stats_shamela` to be consistent with the `_shamela` suffix pattern used across all other Shamela collections.
**Touched files**:
- `app/database.py`
- `mongo_migration/create_indexes.py`
- `mongo_migration/rename_collections.py`
- `mongo_migration/processed_bukhari_shamela/compute_stats.py`
- `CLAUDE.md`
- `tasks.md`

### chore: update README.md with dev/prod workflow and feature branch workflow (2026-04-06)
**Status**: done
**Summary**: Rewrote Quick Start, Environments table, and added Feature Development Workflow section with step-by-step git branch → dev test → merge main → make prod promotion. Replaced all tmux/Atlas references with Makefile commands. Updated Data Update Workflow promote-to-prod step. Added note to Railway/Render section that project currently runs on VPS.
**Touched files**:
- `README.md`
- `tasks.md`

### chore: dev/prod Docker Compose separation (2026-04-06)
**Status**: done
**Summary**: Split single docker-compose.yml into base + override (dev) + prod files using Docker Compose file merging. Both stacks run simultaneously on the same VPS — dev on port 8001 (with live reload, exposed MongoDB), prod on port 8000 (code baked into image, MongoDB internal-only). Separate volumes (`hadathana_mongodb_dev` / `hadathana_mongodb_prod`), container names (`*-dev` / `*-prod`), and databases (`HadithDataDev` / `HadithData`). Added Makefile for shorthand commands (`make dev`, `make prod`, `make health`). Updated config.py to default `mongodb_uri_read` to empty string (Atlas unreachable) and fall back to local URI. Promotion workflow: develop on dev → commit → `make prod` rebuilds image and restarts prod.
**Touched files**:
- `docker-compose.yml` (rewritten as base skeleton — no env-specific values)
- `docker-compose.override.yml` (new — dev config, auto-loaded)
- `docker-compose.prod.yml` (new — prod config, explicit `-f` required)
- `app/config.py` (mongodb_uri_read default to "", simplified get_mongodb_uri)
- `Makefile` (new — dev/prod/status/health convenience targets)
- `CLAUDE.md` (rewritten Docker section with dev/prod allocation table, Makefile commands, promotion workflow)
- `tasks.md`

### feat: auth (sign up / sign in / forgot+reset password) + bookmarks (2026-04-18)
**Status**: done
**Summary**: Added full user auth system using fastapi-users v15 with a custom Motor adapter (MongoDBUserDatabase was removed in v13+). HttpOnly cookie sessions (JWT, 15-min TTL). Resend for transactional email. Rate limiting via slowapi. Bookmarks CRUD at /api/v2/bookmarks (authenticated). 42 tests passing.
**Touched files**:
- `app/auth/__init__.py` (new)
- `app/auth/models.py` (new — User, UserRead, UserCreate, UserUpdate)
- `app/auth/database.py` (new — custom MotorUserDatabase adapter)
- `app/auth/config.py` (new — UserManager, cookie transport, JWT strategy, auth_backend)
- `app/routers/bookmarks.py` (new — GET/POST/DELETE /api/v2/bookmarks)
- `app/database.py` (added get_auth_users_collection, get_bookmarks_collection)
- `app/config.py` (added jwt_secret, token TTLs, resend_api_key, from_email)
- `app/main.py` (mounted auth + bookmark routers, slowapi, updated CORS)
- `mongo_migration/create_indexes.py` (auth_users, reset tokens, refresh sessions, bookmarks indexes)
- `requirements.txt` (added fastapi-users, slowapi, resend)
- `.env.example` (added auth + email env vars)
- `tests/conftest.py` (updated fixtures)
- `tests/test_auth_register.py` (new)
- `tests/test_auth_login.py` (new)
- `tests/test_auth_forgot_password.py` (new)
- `tests/test_auth_reset_password.py` (new)
- `tests/test_bookmarks.py` (new)
- `README.md` (added Auth and Bookmarks endpoint sections)
- `tasks.md`
