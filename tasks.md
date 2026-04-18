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
- [x] Add API authentication and rate limiting
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
- [x] Unit tests for API endpoints (pytest + httpx)
- [ ] Integration tests with MongoDB test database
- [x] CI/CD pipeline (GitHub Actions): run tests + linting on every PR, block merge to `main` if checks fail — `.github/workflows/ci.yml` (lint job: ruff; test job: pytest)
- [x] Automated linting and type checking (ruff in CI)
- [ ] Branch protection rule on `main`: require passing CI checks before merge (manual GitHub settings step)

### Auto Deployment
- [ ] Auto-deploy on push to `main` (Railway / Render webhook or GitHub Actions deploy step)
- [ ] Remove manual `tmux` restart workflow — deployment must be zero-touch
- [ ] Health check after deploy to confirm the new version is live

### Deployment & Monitoring
- [ ] Deploy backend to cloud (Railway / Render / AWS)
- [ ] Deploy frontend to Vercel
- [ ] Error tracking (Sentry or similar)
- [x] Prometheus metrics (Phase 1) — request rate, latency, error rate via `/metrics`
- [x] Grafana dashboards (Phase 2) — API Overview dashboard with 11 panels
- [ ] Loki + Promtail log aggregation (Phase 3)
- [ ] Langfuse LLM observability (Phase 4)
- [ ] Alerting (Phase 6)
- [x] Logging infrastructure

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
