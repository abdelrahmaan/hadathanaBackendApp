# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow Rules

Follow these rules strictly for **every task, no exceptions**:

### Task Tracking — MANDATORY steps in order
1. **Read `tasks.md`** first to understand project context and current progress
2. **Mark the relevant item as `in_progress`** in `tasks.md` before starting any work
3. **Write tests first** (API endpoints and data-processing tasks only — skip for docs, renames, config-only changes):
   - Write test cases that define the expected behaviour
   - Run them with `pytest` and confirm they **fail** (red)
   - Then implement the code until they **pass** (green)
   - Tests live in `tests/` using `pytest` + `httpx` for API endpoints
4. **Update docs** immediately after finishing — always update `tasks.md`, then update the docs below that apply:

| Doc | Update when |
|---|---|
| `tasks.md` | **Every task** — status, summary, touched files |
| `README.md` | API endpoints change, setup steps change, env vars added |
| `CLAUDE.md` | Architecture changes, collection names change, new commands or patterns |
| `mongo_migration/processed_bukhari_*/schema_description.md` | Data schema or fields change |
| `.env.example` | New env vars added or existing ones renamed |

> **Steps 2, 3, and 4 are non-negotiable.** Never finish a task without updating `tasks.md`.
> If the task doesn't map to an existing item, add a new entry under the relevant version or the Chore Log section.

### Conventions
- **Commit messages**: Use conventional commits (`feat:`, `fix:`, `refactor:`, `docs:`, `chore:`)
- **Secrets**: Never modify `.env` or commit secrets — only edit `.env.example`
- **Existing patterns**: New endpoints must follow the existing pagination, filter, and response patterns (see Key Implementation Patterns below)
- **Plans**: Always save plan files (e.g., `plan_*.md`) in the project root directory

## Project Overview

Hadathna is a dual-backend Islamic hadith knowledge system combining MongoDB (REST API) and Neo4j (graph queries) to serve Sahih al-Bukhari data. The project supports two data pipelines:
- **Shamela pipeline**: scraped from shamela.ws (7,230 hadiths)
- **Podia pipeline**: scraped from bukhari-pedia.net (more detailed narrator chains)

## Core Architecture

### FastAPI Application (`app/`)

**Entry point**: `app/main.py`
- Uses Motor (async MongoDB driver) with connection lifecycle in `app/database.py`
- Database connection is established in `lifespan` context manager (connects on startup, disconnects on shutdown)
- CORS configured via `settings.get_cors_origins()` (comma-separated in `.env`)
- Router structure: each data source has separate routers (`hadiths_shamela`, `narrators_shamela`, `hadiths_podia`, `narrators_podia`); chatbot lives in `app/chatbot/router.py`
- CORS allows `GET` and `POST` (POST required for chatbot)

**Database access pattern**:
```python
from ..database import get_client, get_db, get_hadiths_collection
db = get_db(get_client())
collection = get_hadiths_collection(db)
```

**MongoDB collections**:

Shamela pipeline:
- `raw_shamela_books` - Shamela hadiths
- `raw_shamela_narrators` - Shamela narrators
- `raw_shamela_hadith_pages` - Raw Shamela pages

Podia pipeline (processed):
- `processed_podia_books` - Podia hadiths (primary — from advanced extraction; includes `sanad_text`, `matn_text`, `tawabi_text`, `chains[]` with transmission data)
- `processed_podia_narrators` - Podia narrators (deduplicated, with clean/plain name variants)
- `processed_podia_narrator_biographies` - Narrator biographies (tarajem)

Podia pipeline (raw):
- `raw_podia_books` - Podia hadiths (raw scrape — includes `full_name`, `rank`, `full_tooltip_info` per narrator; audit trail / fallback)

Analytics:
- `analytics_narrator_stats_shamela` - Shamela teacher/student statistics (`freq` = distinct hadiths where the pair co-appears, not raw chain occurrences)
- `analytics_narrator_stats_podia` - Podia teacher/student statistics (`freq` = distinct hadiths where the pair co-appears, not raw chain occurrences)

Chatbot:
- `chat_sessions_dev` — conversation history in dev (`APP_ENV=dev`)
- `chat_sessions_prod` — conversation history in prod (`APP_ENV=prod`)

Future (not yet populated):
- `canonical_books`, `canonical_hadiths`, `canonical_narrators`, `canonical_chains` (v1.2+)
- `raw_shamela_narrator_biographies`, `analytics_chain_stats`, `analytics_source_coverage` (v1.1+)

### Data Pipeline Architecture

**Two independent data sources**:
1. **Shamela** (`mongo_migration/processed_bukhari_shamela/`) - flat narrator chains
2. **Podia** (`mongo_migration/processed_bukhari_podia/`) - multi-chain structure with transmission types

**Text field hierarchy** (applies to both pipelines):
- Raw fields: original text with tashkeel (e.g., `hadith_text`, `name_in_chain`)
- `_clean` suffix: cleaned but preserves tashkeel
- `_plain` suffix: cleaned + tashkeel stripped
- `_search` suffix: fully normalized for search — tashkeel stripped + hamza unified (أ/إ/آ→ا, ؤ→و, ئ→ي) + ة→ه + ى→ي + tatweel removed. **Routers always query `_search` fields** (never `_plain`), so variant spellings match automatically. Implemented in `app/normalization.py`.

**Narrator ID handling**: MongoDB stores both `int` and `string` variants - use `{"$in": [narrator_id, str(narrator_id)]}` for queries (see `app/routers/hadiths_shamela.py:40`)

## Common Commands

### Docker Compose (dev & prod)

The project uses Docker Compose with file merging to run **dev and prod simultaneously** on the same VPS:

| File | Purpose | Loaded by |
|------|---------|-----------|
| `docker-compose.yml` | Shared skeleton | Always |
| `docker-compose.override.yml` | Dev config | Auto-loaded by `docker compose up` |
| `docker-compose.prod.yml` | Prod config | Explicit: `-f ... -f ...` or `make prod` |

#### Environment allocation

| Service | Dev | Prod |
|---------|-----|------|
| API port | 8001 | 8000 |
| MongoDB port | 27017 (exposed) | 27018 (exposed) |
| Prometheus port | 9090 | 9091 |
| Grafana port | 3002 | 3001 |
| Qdrant port | 6333 (dev only) | — |
| Database | `HadithDataDev` | `HadithData` |
| Volume prefix | `hadathana_*_dev` | `hadathana_*_prod` |
| Containers | `hadathana-*-dev` | `hadathana-*-prod` |
| Code reload | Yes (`--reload` + volume mount) | No (baked into image) |

#### Everyday commands (via Makefile)

```bash
# ── Dev ───────────────────────────────────────
make dev              # start dev stack (API on :8001)
make dev-logs         # follow dev API logs
make dev-down         # stop dev stack
make dev-ps           # check dev service status

# ── Prod ──────────────────────────────────────
make prod             # build + start prod stack (API on :8000)
make prod-logs        # follow prod API logs
make prod-down        # stop prod stack
make prod-ps          # check prod service status
make prod-restart     # restart prod API only

# ── Both ──────────────────────────────────────
make status           # show both stacks
make health           # health check both APIs

# ── Raw commands (equivalent to Makefile) ─────
docker compose up -d                                    # dev
docker compose -p hadathana-prod -f docker-compose.yml -f docker-compose.prod.yml up -d --build  # prod
```

#### Shell into containers

```bash
# Dev
docker exec -it hadathana-api-dev sh
docker exec -it hadathana-mongo-dev mongosh HadithDataDev

# Prod
docker exec -it hadathana-api-prod sh
docker exec -it hadathana-mongo-prod mongosh HadithData
```

#### Connect from MongoDB Compass

```
mongodb://<host>:27017   → HadithDataDev (dev)
mongodb://<host>:27018   → HadithData    (prod)
```

`<host>` is `localhost` if running on the same machine, otherwise the VPS IP (e.g. `72.60.222.229`). Both ports are exposed without auth — for remote access prefer SSH tunneling (`ssh -L 27018:localhost:27018 user@host`) over opening the port publicly.

#### What happens on startup

1. **`mongo`** starts with a named volume — data survives `docker compose down` and restarts
2. **`mongo-init`** checks if the target database is empty — if so, imports all JSONL files + creates indexes + computes stats + applies topics & embeddings. If data exists, exits in <1s.
3. **`api`** starts, connects to MongoDB, validates all collections at startup, logs ERROR if any are missing/empty

#### Health check

```bash
curl http://localhost:8001/health   # dev
curl http://localhost:8000/health   # prod
# Returns: {"status":"ok","mongodb":"connected","collections":{"processed_podia_books":7076,...}}
```

#### Promotion workflow (dev → prod)

```bash
# 1. Develop and test in dev (live reload on :8001)
make dev

# 2. Once satisfied, commit and rebuild prod
git add . && git commit -m "feat: ..."
make prod    # rebuilds image with new code, restarts prod on :8000

# 3. Rollback if needed
git checkout <good-commit>
make prod    # rebuilds from the rolled-back code
```

#### Data safety

```bash
# ⚠️ These commands DESTROY MongoDB data:
docker compose down -v                    # dev volume
make prod-down && docker volume rm hadathana_mongodb_prod  # prod volume

# If data is lost, just restart — mongo-init will auto-bootstrap from JSONL files:
make dev   # or make prod
```

#### Adding future services

Add new services to `docker-compose.yml` (base). A commented-out template for mongo-express is included.

API docs: http://localhost:8001/docs (dev) / http://localhost:8000/docs (prod)
Grafana: http://localhost:3002 (dev) / http://localhost:3001 (prod)
Prometheus: http://localhost:9090 (dev) / http://localhost:9091 (prod)
Qdrant: http://localhost:6333/dashboard (dev only)

### Bootstrap script (standalone)

The bootstrap script can also be run outside docker-compose for manual recovery:

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"

# Bootstrap HadithDataDev (skips if data already exists)
"$PYTHON" scripts/bootstrap_local_db.py

# Bootstrap a specific database
"$PYTHON" scripts/bootstrap_local_db.py --db HadithData

# Force re-import (even if data exists)
"$PYTHON" scripts/bootstrap_local_db.py --force

# Custom MongoDB URI (e.g. inside Docker network)
"$PYTHON" scripts/bootstrap_local_db.py --uri mongodb://mongo:27017/
```

Imports all JSONL files + indexes + stats + topics + embeddings in one command.

### Tests

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m pytest tests/ -v
```

### Linting (ruff)

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m ruff check app/ tests/
"$PYTHON" -m ruff check app/ tests/ --fix   # auto-fix safe violations
```

Ruff config lives in `pyproject.toml`. The CI pipeline runs `ruff check app/ tests/` (no auto-fix).

### CI/CD (GitHub Actions)

Pipeline runs on every PR targeting `main`. Workflow: `.github/workflows/ci.yml`.

| Job | Runs | What it does |
|-----|------|--------------|
| `lint` | first | `ruff check app/ tests/` — fails on any violation |
| `test` | after lint passes | `pytest tests/ -v` with `APP_ENV=dev` and a local MongoDB URI |

Tests use `APP_ENV=dev` + `MONGODB_URI_LOCAL=mongodb://localhost:27017/` (no real DB needed — tests mock at the collection level).

### Monitoring

**Prometheus** (`monitoring/prometheus.dev.yml` / `monitoring/prometheus.yml`):
- Scrapes `/metrics` every 15s
- Dev: `api:8001` → http://localhost:9090
- Prod: `api:8000` → http://localhost:9091
- 30-day metric retention

**Grafana** (`monitoring/grafana/`):
- Auto-provisioned datasource: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- Auto-provisioned dashboard file provider: `monitoring/grafana/provisioning/dashboards/dashboards.yml`
- Pre-built "Hadathana API Overview" dashboard: `monitoring/grafana/dashboards/api-overview.json` (11 panels: request rate, error rate, latency p50/p95/p99, latency by endpoint, requests in progress, response size, Python memory)
- Dev: http://localhost:3002 | Prod: http://localhost:3001
- Login: `admin` / `GRAFANA_ADMIN_PASSWORD` env var (default: `admin`)

**Qdrant** (dev only, port 6333):
- Vector DB for semantic hadith search
- Collection `hadiths_matn`: 7,073 points, Cohere `embed-v4.0` 1536-dim vectors
- Both dense and sparse (`text-sparse`) indexes for hybrid search
- Dashboard: http://localhost:6333/dashboard

### MongoDB Data Pipeline (Shamela)

```bash
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"

# Full rebuild (run in order):
"$PYTHON" mongo_migration/processed_bukhari_shamela/preprocess_pages.py
"$PYTHON" mongo_migration/processed_bukhari_shamela/preprocess_hadiths.py
"$PYTHON" mongo_migration/upload.py
"$PYTHON" mongo_migration/create_indexes.py
"$PYTHON" mongo_migration/processed_bukhari_shamela/compute_stats.py
```

### MongoDB Data Pipeline (Podia)

```bash
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"

# Full rebuild (run in order):
"$PYTHON" mongo_migration/processed_bukhari_podia/preprocess.py
"$PYTHON" mongo_migration/upload.py
"$PYTHON" mongo_migration/create_indexes.py
"$PYTHON" mongo_migration/processed_bukhari_podia/compute_stats.py
```

### Neo4j Graph (Podia only)

```bash
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"

# Dry run (stats only, no DB connection needed)
"$PYTHON" mongo_migration/processed_bukhari_podia/build_graph.py --dry-run

# Full ingestion (requires NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD in .env)
"$PYTHON" mongo_migration/processed_bukhari_podia/build_graph.py
```

**Neo4j management**:
```bash
# Docker container
docker start neo4j-hadith
docker stop neo4j-hadith

# Access Neo4j Browser: http://localhost:7474
# Clear database: MATCH (n) DETACH DELETE n;
```

### R2 Data Sync (Cloudflare R2)

Scripts in `scripts/r2_sync/` push/pull dataset snapshots to/from Cloudflare R2. Snapshots are stored as `snapshots/<dataset>/<YYYY-MM-DD>/` in the bucket.

```bash
# List available snapshots
python scripts/r2_sync/list_snapshots.py

# Download the latest snapshot for a dataset
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_shamela --latest
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_podia --latest

# Upload a folder as today's snapshot
python scripts/r2_sync/push_snapshot.py --dataset bukhari_shamela --source data/

# Upload only specific file types from the whole repo
python scripts/r2_sync/push_snapshot.py --dataset full_backup --source . --extensions json,jsonl,csv,xlsx,parquet

# Dry run (list files without uploading)
python scripts/r2_sync/push_snapshot.py --dataset full_backup --source . --extensions json,jsonl --dry-run
```

Dataset naming: `bukhari_shamela`, `bukhari_podia`, `tarajm`. Downloaded files go to `data_snapshots/` (gitignored).

Requires: `pip install boto3 python-dotenv tqdm` and R2 credentials in `.env`.

Full docs: `scripts/r2_sync/README.md`

### Offline Enrichment Scripts (JSONL-first, no MongoDB required)

Two scripts enrich hadiths without any MongoDB dependency. They read the source JSONL directly and write slim output files to the repo root.

| Script | Input | Output | Destination |
|---|---|---|---|
| `scripts/tag_topics_jsonl.py` | `bukhari_podia_hadiths.jsonl` | `hadith_topics.jsonl` | MongoDB (PyMongo `$set` bulk_write — see below) |
| `scripts/embed_matn_jsonl.py` | `bukhari_podia_hadiths.jsonl` | `hadith_embeddings.jsonl` | Qdrant / vector DB (deferred) |

Both are **resumable** — re-running skips already-processed `hadith_url`s. Source JSONL is never modified.

> ⚠️ **Never use `mongoimport --mode=upsert` or `--mode=merge` for partial field updates** — both replace the entire document with only the fields in the JSONL, destroying all other fields. Always use PyMongo `bulk_write` with `$set` to add/update a single field.

```bash
# Run in tmux enrichment session (topics → embeddings, chained)
tmux new-session -d -s enrichment
tmux send-keys -t enrichment 'cd ~/Projects/hadathanaBackendApp && \
  /home/abdo_kamar/Projects/.venv/bin/python scripts/tag_topics_jsonl.py && \
  /home/abdo_kamar/Projects/.venv/bin/python scripts/embed_matn_jsonl.py' Enter

# After topics finish — apply to HadithDataDev using $set (safe merge, never replaces)
/home/abdo_kamar/Projects/.venv/bin/python - <<'EOF'
import json
from pymongo import MongoClient, UpdateOne

client = MongoClient("mongodb://localhost:27017/")
col = client["HadithDataDev"]["processed_podia_books"]
ops = [UpdateOne({"hadith_url": json.loads(l)["hadith_url"]}, {"$set": {"topics": json.loads(l)["topics"]}}) for l in open("hadith_topics.jsonl")]
result = col.bulk_write(ops, ordered=False)
print(f"matched: {result.matched_count}, modified: {result.modified_count}")
EOF
```

Requires `OPENROUTER_API_KEY` (topics) and `COHERE_API_KEY` (embeddings) in `.env`.

## Key Implementation Patterns

### API Endpoint Structure

All endpoints follow pagination pattern:
- Default: `skip=0`, `limit=20` (max 100)
- Returns `PaginatedHadiths` or `PaginatedNarrators` with `items[]` and `total`

Filter params use normalized MongoDB regex for text search. Always query the `_search` field (not `_plain`), and normalize the user's query first:
```python
from ..normalization import normalize_for_search
import re

if hadith_plain:
    query_filter["hadith_search"] = {"$regex": re.escape(normalize_for_search(hadith_plain)), "$options": "i"}
```

`re.escape()` prevents regex injection. `normalize_for_search()` handles: tashkeel, hamza variants, ة→ه, ى→ي, tatweel.

### MongoDB vs Neo4j Division

**MongoDB** (via FastAPI):
- Text search, pagination, REST API
- Flat narrator arrays for simple queries
- Ideal for: "Find hadiths containing X", "Search narrators by name"

**Neo4j** (via Cypher):
- Chain traversal, network analysis
- Multi-chain structure with transmission types
- Ideal for: "Find all teachers of X", "Show narrator relationships"

**Important**: Podia advanced extraction data (`bukhari_pedia_advanced_extraction_results.json`) feeds BOTH MongoDB (via `preprocess.py`) and Neo4j (via `build_graph.py`). The two databases serve different query patterns with overlapping but distinct data.

**Qdrant** (via `app/chatbot/qdrant.py`):
- Collection: `hadiths_matn` — 7,075 points, 1536-dim dense (Cohere embed-multilingual-v3.0) + BM25 sparse (FastEmbedSparse)
- Populated by `scripts/sync_qdrant.py` (run once by `qdrant-init` container on startup)
- MongoDB stays the **source of truth** — Qdrant is a derived index
- After any Mongo data update: re-run `python scripts/sync_qdrant.py --force`
- Client lifecycle: `connect_qdrant()` / `disconnect_qdrant()` in `app/main.py` lifespan (mirrors `database.py`)

### Chatbot Architecture (`app/chatbot/`)

| File | Purpose |
|---|---|
| `config.py` | Compile-time constants: collection name, embedding dim, reranker model |
| `qdrant.py` | QdrantClient lifecycle (module-level singleton, mirrors `database.py`) |
| `indexer.py` | Populate Qdrant from Mongo — reuses stored `matn_embedding`, computes BM25 |
| `retriever.py` | Build hybrid retriever: Qdrant HYBRID mode + Cohere reranker |
| `agent.py` | LangChain `create_tool_calling_agent` + `@tool search_hadiths`, OpenRouter LLM |
| `prompts.py` | Arabic system prompt (cite-or-refuse guardrail) + thread rename prompt |
| `session.py` | `get_or_create_session()` / `append_turn()` — writes to `chat_sessions_dev` or `chat_sessions_prod` based on `APP_ENV` |
| `router.py` | `POST /api/v2/chat` — SSE streaming, session_id management, citation extraction |
| `models.py` | `ChatRequest`, `Citation`, `SessionMessage`, `ChatSession` |

**SSE event sequence** for `POST /api/v2/chat`:
```
assistant_message_start  → carries session_id for new sessions
content                  → one per token
assistant_message_complete → full text + citations[]
thread_rename            → short Arabic title
stream_end
```

**Data update workflow** (add this step when Mongo data changes):
After updating `processed_podia_books`, re-sync Qdrant:
```bash
python scripts/sync_qdrant.py --force
# or inside Docker:
docker exec hadathana-qdrant-init-dev python scripts/sync_qdrant.py \
  --uri mongodb://mongo:27017/ --db HadithDataDev --qdrant-url http://qdrant:6333 --force
```

## Environment Variables

Required in `.env`:

```bash
# ── Environment ───────────────────────────────────────────────
# "prod" → Atlas + HadithData + port 8000
# "dev"  → local Docker MongoDB + HadithDataDev + port 8001
APP_ENV=prod

# ── MongoDB (prod — Atlas) ────────────────────────────────────
MONGODB_URI_READ=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
MONGODB_URI_READ_WRITE=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
DB_NAME=HadithData

# ── MongoDB (dev — local Docker) ─────────────────────────────
MONGODB_URI_LOCAL=mongodb://localhost:27017/
DB_NAME_DEV=HadithDataDev

# ── CORS ──────────────────────────────────────────────────────
CORS_ORIGINS=https://hadathana.app,https://www.hadathana.app
CORS_ORIGINS_DEV=http://localhost:3000,http://localhost:5173

# ── Neo4j (graph scripts) ─────────────────────────────────────
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# ── Chatbot ───────────────────────────────────────────────────
COHERE_API_KEY=<your_cohere_api_key>
OPENROUTER_API_KEY=<your_openrouter_api_key>
QDRANT_URL=http://qdrant:6333          # docker service name; use http://localhost:6333 for local scripts
CHATBOT_MODEL=qwen/qwen3-235b-a22b    # OpenRouter model slug

# ── Cloudflare R2 (data snapshots) ───────────────────────────
R2_ENDPOINT_URL=https://<account_id>.r2.cloudflarestorage.com
R2_BUCKET=hadathana_data
R2_ACCESS_KEY_ID=<your_access_key>
R2_SECRET_ACCESS_KEY=<your_secret_key>
```

See `app/config.py` — `Settings.get_mongodb_uri()` and `Settings.get_db_name()` switch automatically based on `APP_ENV`.

## Python Environment

The project uses a shared `uv` venv at `/home/abdo_kamar/Projects/.venv/`:

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" mongo_migration/upload.py
```

## Data Storage

**All data files are stored in Cloudflare R2** (S3-compatible object storage), not in git. The repo `.gitignore` excludes all `.json`, `.jsonl`, `.csv`, `.xlsx`, and `.parquet` files under data directories so they can never be accidentally committed. After cloning, pull data with the R2 sync scripts (see below).

### Data update workflow

When enriching or modifying data (adding fields, new embeddings, topic tags, etc.):

1. **Write/update the script** — new enrichment → new file under `scripts/`; schema change → update preprocessing script + `app/models/`
2. **Pull latest data from R2** — `python scripts/r2_sync/pull_snapshot.py --dataset bukhari_podia --latest`
3. **Run against `HadithDataDev` first** — `APP_ENV=dev` must be set in `.env`
4. **Verify in dev** — test endpoints on port 8000, run `pytest`
5. **Push enriched JSONL to R2** — `python scripts/r2_sync/push_snapshot.py --dataset bukhari_podia --source mongo_migration/processed_bukhari_podia/ --extensions jsonl`
6. **Promote to prod (Atlas)** — re-run `upload.py` against Atlas or use `mongodump`/`mongorestore`
7. **Restart API** — `docker compose restart api`

### Change detection workflow

Data files are registered with git via `git add --intent-to-add --force` so `git status` shows when they change — but `.gitignore` prevents them from ever being staged or committed. Use this as your signal to push to R2:

```bash
# 1. See what changed (data + code together)
git status

# 2. If data changed → push to R2
python scripts/r2_sync/push_snapshot.py --dataset bukhari_podia \
  --source mongo_migration/processed_bukhari_podia/ --extensions jsonl

# 3. Code changes → commit normally (data is blocked by .gitignore)
git add app/
git commit -m "feat: ..."
```

To register newly pulled data files for change tracking:
```bash
git add --intent-to-add --force mongo_migration/processed_bukhari_podia/*.jsonl
git add --intent-to-add --force extract_data_v2/playwrite/*.jsonl
```

**Shamela input**:
- `extract_data_v2/firecrawl/shamela_book_1681.jsonl` - raw hadiths
- `extract_data_v2/Bukhari/narrators_list.json` - ground truth

**Podia input**:
- `bukhari_pedia_advanced_extraction_results.json` — primary source (7,076 hadiths, GPT-4o chains with transmission types)
- `extract_data_v2/playwrite/narrators_bukhari_pedia_playwrite.jsonl` — narrator profiles
- `mongo_migration/processed_bukhari_podia/bukhari_narrators_tarajem.jsonl` — narrator biographies

**Processed output** (ready for MongoDB):
- `mongo_migration/processed_bukhari_shamela/*.jsonl`
- `mongo_migration/processed_bukhari_podia/*.jsonl`

**R2 snapshots** (downloaded via pull):
- `data_snapshots/<dataset>/<YYYY-MM-DD>/` — gitignored, local only

## Testing API Endpoints

```bash
# Health check
curl http://localhost:8000/health

# List hadiths (Shamela)
curl http://localhost:8000/api/v1/hadiths

# Search hadith text
curl "http://localhost:8000/api/v1/hadiths?hadith_plain=نام"

# Filter by narrator ID
curl "http://localhost:8000/api/v1/hadiths?narrator_id=822"

# Get single hadith
curl http://localhost:8000/api/v1/hadiths/1

# List narrators
curl http://localhost:8000/api/v1/narrators

# Get narrator stats (teachers/students)
curl http://localhost:8000/api/v1/narrators/822/stats

# Podia endpoints (v2)
curl http://localhost:8000/api/v2/hadiths
curl http://localhost:8000/api/v2/narrators
```

## AI / RAG / Chatbot Stack

All AI features (RAG, chatbot, agents) live in `app/chatbot/`. Use this stack exclusively — do not introduce alternatives.

Reference docs: https://docs.langchain.com/oss/python/langchain/

### Full Stack

| Layer | Choice | Notes |
|---|---|---|
| **Agent orchestration** | LangChain `create_agent` | Single factory — never use `initialize_agent` or `AgentExecutor` (deprecated) |
| **Tools** | `@tool` decorator | Type-annotated; docstring is the LLM-visible description — make it precise |
| **MCP servers** | `MultiServerMCPClient` (`langchain-mcp-adapters`) | Expose external tools as MCP servers; agent consumes via `get_tools()` |
| **Short-term memory** | `InMemorySaver` checkpointer (dev) | `thread_id` in config for per-session state; swap to Redis/Postgres in prod |
| **Context engineering** | `middleware=[...]` on `create_agent` | `dynamic_prompt`, `SummarizationMiddleware` — start static, add dynamic only when needed |
| **Guardrails** | `PIIMiddleware`, `HumanInTheLoopMiddleware` | Stack as middleware; deterministic (regex) checks first, model-based last |
| **Structured output** | `response_format=PydanticModel` on `create_agent` | LangChain picks best strategy (native > tool calling > prompting) automatically |
| **LLM** | `init_chat_model` via OpenRouter | Model swappable via `CHATBOT_MODEL` env var — never hardcode |
| **Streaming** | `agent.astream(stream_mode="updates", version="v2")` | SSE via FastAPI `StreamingResponse` from day one |
| **RAG / Retrieval** | `@tool` wrapping retriever | Use Agentic RAG (tool) not chain-based; agent decides when to retrieve |
| **Vector store** | Qdrant (self-hosted Docker) | Hybrid dense+BM25 via `langchain-qdrant` |
| **Embeddings** | Cohere `embed-multilingual-v3.0` | Same model as stored vectors in Mongo — never re-embed without syncing both |
| **Reranker** | Cohere `rerank-multilingual-v3.0` | Arabic-aware; top_n=5 from top-20 prefetch |
| **API endpoints** | FastAPI `StreamingResponse` | All chat endpoints SSE, prefix `/api/v2/chat` |

### Key imports

```python
# Agent + tools
from langchain.agents import create_agent
from langchain.tools import tool
from langchain.chat_models import init_chat_model

# Messages
from langchain.messages import SystemMessage, HumanMessage, AIMessage, ToolMessage, RemoveMessage

# Memory
from langgraph.checkpoint.memory import InMemorySaver

# Streaming
from langgraph.config import get_stream_writer

# MCP
from langchain_mcp_adapters.client import MultiServerMCPClient

# Qdrant hybrid retrieval
from langchain_qdrant import QdrantVectorStore, RetrievalMode, FastEmbedSparse
from langchain_cohere import CohereRerank
from langchain.retrievers import ContextualCompressionRetriever

# Structured output + guardrails
from langchain.agents.middleware import PIIMiddleware, HumanInTheLoopMiddleware
from langchain.agents import dynamic_prompt
```

---

### Agent pattern

```python
# app/chatbot/agent.py
from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langgraph.checkpoint.memory import InMemorySaver

model = init_chat_model(
    settings.chatbot_model,              # e.g. "openrouter/qwen/qwen3-235b-a22b"
    base_url="https://openrouter.ai/api/v1",
    api_key=settings.openrouter_api_key,
)

agent = create_agent(
    model,
    tools=[search_hadiths, search_narrators],
    system_prompt="أجب فقط من السياق المتاح...",  # always from prompts.py
    checkpointer=InMemorySaver(),        # omit for stateless V1
    middleware=[PIIMiddleware("email", strategy="redact")],  # add guardrails here
    max_iterations=4,                    # always cap for multi-tool agents
)
```

### Tools pattern

```python
from langchain.tools import tool, ToolRuntime
from pydantic import BaseModel, Field

# Basic tool — type hints + docstring required
@tool
def search_hadiths(query: str, limit: int = 5) -> str:
    """Search Bukhari hadiths by meaning or topic. Use for any question about hadith content."""
    docs = retriever.invoke(query)
    return "\n\n".join(
        f"[{i+1}] {d.page_content}\nSource: {d.metadata.get('hadith_url')}"
        for i, d in enumerate(docs[:limit])
    )

# Tool returning content + raw artifact (for citation display)
@tool(response_format="content_and_artifact")
def search_hadiths_with_docs(query: str):
    """Search Bukhari hadiths. Returns text for LLM + raw docs for frontend citations."""
    docs = retriever.invoke(query)
    text = "\n\n".join(f"[{i+1}] {d.page_content}" for i, d in enumerate(docs))
    return text, docs

# Complex schema via Pydantic
class HadithSearchInput(BaseModel):
    query: str = Field(description="Search terms in Arabic or English")
    topic: str | None = Field(default=None, description="Optional topic filter")
    limit: int = Field(default=5, description="Max results, 1–20")

@tool(args_schema=HadithSearchInput)
def search_hadiths_filtered(query: str, topic: str | None = None, limit: int = 5) -> str:
    """Search Bukhari hadiths with optional topic filter."""
    ...

# Tool accessing runtime state (e.g. thread_id, stream writer)
@tool
def log_search(query: str, runtime: ToolRuntime) -> str:
    """Log the search query and return thread info."""
    runtime.stream_writer(f"Searching for: {query}")
    return f"Thread: {runtime.execution_info.thread_id}"
```

Rules:
- **`snake_case` names only** — some providers reject spaces or special characters in tool names
- **Docstring is the LLM-visible description** — start with a precise verb: "Search", "Fetch", "List", "Get"
- **`runtime` parameter is auto-injected and hidden from LLM** — use it for stream_writer, state, context
- **Reserved param names**: never name args `config` or `runtime` — both are reserved
- **Keep tools narrow** — one responsibility. Broad tools make the LLM guess wrong
- **`ToolNode`** handles parallel execution and error handling when used inside LangGraph

```python
# ToolNode for LangGraph-based flows (not needed for create_agent)
from langgraph.prebuilt import ToolNode
tool_node = ToolNode(
    [search_hadiths, search_narrators],
    handle_tool_errors=True,   # catch exceptions, return error message to LLM
)
```

### MCP pattern

```python
# Connect an external MCP server (e.g. a Hadith search microservice)
client = MultiServerMCPClient({
    "hadith_search": {
        "transport": "stdio",
        "command": "python",
        "args": ["scripts/mcp_hadith_server.py"],
    }
})
tools = await client.get_tools()
agent = create_agent(model, tools)
```

Use MCP when a tool is a standalone service that should be reusable outside the agent (e.g. a search microservice). For internal retrieval logic, plain `@tool` is simpler.

### Middleware pattern

Middleware is passed as a list to `create_agent`. It intercepts agent execution at each step for observability, transformation, resilience, and control.

```python
from langchain.agents import create_agent
from langchain.agents.middleware import (
    SummarizationMiddleware,
    PIIMiddleware,
    HumanInTheLoopMiddleware,
    ModelFallbackMiddleware,
    ToolCallLimitMiddleware,
)

agent = create_agent(
    model,
    tools=[search_hadiths, search_narrators],
    middleware=[
        # 1. Safety: redact PII from all user input
        PIIMiddleware("email", strategy="redact", apply_to_input=True),
        # 2. Cost control: cap tool calls per run
        ToolCallLimitMiddleware(max_tool_calls=10, on_limit="end"),
        # 3. Resilience: fallback model if primary fails
        ModelFallbackMiddleware(fallback_models=["openrouter/google/gemini-flash-1.5"]),
        # 4. Context: compress history when approaching token limit
        SummarizationMiddleware(
            model="openrouter/qwen/qwen3-235b-a22b",
            trigger=("tokens", 4000),
            keep=("messages", 20),
        ),
    ],
    checkpointer=InMemorySaver(),  # required for HumanInTheLoopMiddleware
)
```

**Execution order for `middleware=[m1, m2, m3]`:**
- `before_*` hooks: m1 → m2 → m3
- `wrap_*` hooks: nested (m1 wraps m2 wraps m3)
- `after_*` hooks: m3 → m2 → m1 (reversed)

**Built-in middleware reference:**

| Middleware | Purpose | Key params |
|---|---|---|
| `SummarizationMiddleware` | Auto-compress history at token threshold | `trigger`, `keep`, `model` |
| `PIIMiddleware` | Redact/mask/hash/block PII | `"email"/"credit_card"/"ip_address"`, `strategy` |
| `HumanInTheLoopMiddleware` | Pause for human approval before tool | `interrupt_on={tool_name: True}` — requires checkpointer |
| `ModelFallbackMiddleware` | Chain fallback models on failure | `fallback_models=[...]` |
| `ModelCallLimitMiddleware` | Limit LLM calls per thread/run | `max_calls`, `on_limit` |
| `ToolCallLimitMiddleware` | Limit tool calls globally or per-tool | `max_tool_calls`, `on_limit` |
| `LLMToolSelectorMiddleware` | Filter relevant tools from large sets | Reduces context before main call |

**Custom middleware (decorator style — simple):**

```python
from langchain.agents.middleware import before_model, after_model, AgentState
from langgraph.runtime import Runtime

@before_model
def log_request(state: AgentState, runtime: Runtime) -> dict | None:
    print(f"[chatbot] {len(state['messages'])} messages in context")
    return None   # return None to pass through unchanged

@after_model
def log_response(state: AgentState, runtime: Runtime) -> dict | None:
    print(f"[chatbot] response: {state['messages'][-1].content[:100]}")
    return None
```

**Custom middleware (class style — for multiple hooks or async):**

```python
from langchain.agents.middleware import AgentMiddleware, AgentState
from langgraph.runtime import Runtime

class ArabicInputValidator(AgentMiddleware):
    def before_model(self, state: AgentState, runtime: Runtime) -> dict | None:
        # validate or transform input before LLM sees it
        return None

    async def abefore_model(self, state: AgentState, runtime: Runtime) -> dict | None:
        return None
```

**Jump to exit early** (e.g. input rejected by guardrail):
```python
@before_model
def block_off_topic(state: AgentState, runtime: Runtime) -> dict | None:
    last = state["messages"][-1].content
    if "كرة القدم" in last:
        return {"jump_to": "end"}   # valid targets: "end", "tools", "model"
    return None
```

**For this project — middleware stack per version:**
- V1 (stateless): `[PIIMiddleware, ToolCallLimitMiddleware(max=3)]`
- V2 (stateful): add `SummarizationMiddleware` once sessions exceed 10 turns
- V3 (graph tools): add `HumanInTheLoopMiddleware` if any write tool is introduced

### Context engineering pattern

Context engineering = controlling what the model sees at each turn. Use middleware hooks, not ad-hoc prompt concatenation.

```python
from langchain.agents import create_agent, dynamic_prompt

@dynamic_prompt
def adaptive_hadith_prompt(request):
    """Shorten system prompt for long conversations to preserve token budget."""
    if len(request.messages) > 12:
        return "أنت مساعد حديثي. كن موجزاً وأجب فقط من السياق."
    return FULL_SYSTEM_PROMPT   # from prompts.py

agent = create_agent(model, tools=[...], middleware=[adaptive_hadith_prompt])
```

Rules:
- **Start with a static prompt in `prompts.py`** — add `dynamic_prompt` only when token overflow is a measured problem, not a hypothesis
- **Token budget first**: wrong answers → check what context the LLM actually received before blaming the model
- **Max 8–10 tools per agent** — beyond that, split into specialized sub-agents or use `LLMToolSelectorMiddleware`

### Structured output pattern

```python
from pydantic import BaseModel, Field

class HadithAnswer(BaseModel):
    answer: str = Field(description="The answer in Arabic")
    citations: list[str] = Field(description="List of hadith URLs cited")
    confidence: str = Field(description="high / medium / low")

agent = create_agent(
    model,
    tools=[search_hadiths],
    response_format=HadithAnswer,   # LangChain picks best extraction strategy
)
result = agent.invoke({"messages": [...]})
# result is a validated HadithAnswer instance
```

Use structured output on the final response when the frontend needs machine-readable fields (citations list, confidence). Don't use it as a crutch to parse free-form LLM text — design the prompt so the model reasons naturally, then extract structure at the boundary.

### Retrieval (RAG) pattern

```python
# app/chatbot/retriever.py — hybrid Qdrant + Cohere rerank
def build_retriever(client, cohere_embeddings, collection: str, k_fetch=20, k_final=5):
    store = QdrantVectorStore(
        client=client,
        collection_name=collection,
        embedding=cohere_embeddings,
        sparse_embedding=FastEmbedSparse(model_name="Qdrant/bm25"),
        retrieval_mode=RetrievalMode.HYBRID,
        vector_name="dense",
        sparse_vector_name="sparse",
    )
    reranker = CohereRerank(model="rerank-multilingual-v3.0", top_n=k_final)
    return ContextualCompressionRetriever(
        base_compressor=reranker,
        base_retriever=store.as_retriever(search_kwargs={"k": k_fetch}),
    )
```

RAG architecture choice for this project:
- **Agentic RAG** (retriever wrapped as `@tool`) — agent decides when to search. This is V1+.
- Never use chain-based RAG (`RetrievalQA`, `ConversationalRetrievalChain`) — those are legacy patterns.
- Two collections, two retrievers (`hadiths_matn`, `narrators_bio`) — don't mix; scores aren't comparable across corpus types.

### SSE streaming pattern (FastAPI)

```python
# app/chatbot/router.py
from fastapi.responses import StreamingResponse
import json

@router.post("")
async def chat(req: ChatRequest):
    async def stream():
        async for chunk in agent.astream(
            {"messages": [{"role": "user", "content": req.question}]},
            stream_mode="updates",
            version="v2",
            config={"configurable": {"thread_id": req.session_id}},
        ):
            if token := chunk.get("token"):
                yield f"data: {json.dumps({'token': token})}\n\n"
        yield "data: [DONE]\n\n"
    return StreamingResponse(stream(), media_type="text/event-stream")
```

### Short-term memory pattern

```python
# Stateful: pass checkpointer + thread_id per request
agent = create_agent(model, tools=[...], checkpointer=InMemorySaver())
result = agent.invoke(
    {"messages": [HumanMessage("Hello")]},
    config={"configurable": {"thread_id": "session-abc"}},
)

# Trim stale messages to stay within context window
from langchain.messages import RemoveMessage
agent.update_state(config, {"messages": [RemoveMessage(id=old_msg.id)]})
```

**Dev vs prod**: `InMemorySaver` is fine for dev and single-process deployments. For multi-replica prod, swap to `langgraph-checkpoint-redis` — the interface is identical, only the import changes.

### Tracing & observability (LangSmith)

Every chat request is one LangSmith trace. The visible parent run is named **`Hadathana_agent`** and carries `session_id`, `user_id`, and `app_env` as metadata, plus an `app_env` tag.

**Expected waterfall:**
```
Hadathana_agent (parent, run_type="chain", visible in UI)
├── generate_title              ← parallel @traceable child via asyncio.create_task
│   └── ChatOpenAI
└── LangGraph                   ← main agent
    ├── model → ChatOpenAI
    └── tools → search_hadiths
        └── ContextualCompressionRetriever (Qdrant + Cohere rerank)
```

**The two nesting techniques** — used together in [app/chatbot/router.py](app/chatbot/router.py):

```python
from langsmith import trace, tracing_context

async with trace(
    "Hadathana_agent",
    run_type="chain",
    project_name=settings.get_langsmith_project(),
    metadata={"session_id": ..., "user_id": ..., "app_env": ...},
    tags=[settings.app_env],
    inputs={"question": request.question},
) as run:
    # (A) @traceable function under asyncio.create_task — pass parent explicitly
    title_task = asyncio.create_task(
        generate_title(request.question, langsmith_extra={"parent": run})
    )

    # (B) LangChain/LangGraph Runnable — bridge via tracing_context(parent=run)
    with tracing_context(parent=run):
        async for chunk in agent.astream(...):
            ...

    run.end(outputs={"title": title, "response_chars": ...})
```

| Child run kind | Pattern | Why |
|---|---|---|
| `@traceable` async function called via `asyncio.create_task` | `langsmith_extra={"parent": run}` | `create_task` schedules the coroutine in a fresh context — the LangSmith contextvar is reset, so explicit parent passing is the official escape hatch |
| LangChain / LangGraph `Runnable` (`.astream` / `.ainvoke`) | `with tracing_context(parent=run):` | `tracing_context` sets the contextvar that LangChain's `LangChainTracer` reads to assign `parent_run_id` to downstream runs |
| `@traceable` function called inline (no `create_task`) | Nothing — just `await` | Async contextvars propagate naturally on `await` |

**Multiple parallel tasks under one parent** — same pattern scales:

```python
async with trace("Hadathana_agent", ...) as run:
    title_task   = asyncio.create_task(generate_title(q,  langsmith_extra={"parent": run}))
    summary_task = asyncio.create_task(summarize_q(q,    langsmith_extra={"parent": run}))
    embed_task   = asyncio.create_task(embed_query(q,    langsmith_extra={"parent": run}))

    with tracing_context(parent=run):
        result = await agent.ainvoke(...)

    title, summary, embed = await asyncio.gather(title_task, summary_task, embed_task)
```

All four children appear as siblings under `Hadathana_agent` in the waterfall.

**Anti-patterns (don't do):**

| Anti-pattern | What goes wrong |
|---|---|
| Wrapping `agent.astream` in `async with trace(...)` *only* | `async with trace()` does **not** set the contextvar in the calling coroutine — LangChain's tracer never sees the parent. Must add `tracing_context(parent=run)`. |
| Manually seeding `LangChainTracer.run_map` / `order_map` and passing a custom `AsyncCallbackManager` in `config["callbacks"]` | Fights the framework. `tracing_context(parent=run)` is the official bridge — use it. |
| Relying on `asyncio.create_task` to inherit the LangSmith parent automatically | `create_task` runs in a fresh context — parent contextvar is reset. Use `langsmith_extra={"parent": run}` or `copy_context().run(...)`. |
| Wrapping `generate_title` in `tracing_context(parent=run)` instead of `langsmith_extra` | `tracing_context` is sync — won't propagate across the `create_task` boundary. `langsmith_extra` is the right tool for `@traceable` functions. |

**Env vars** (set in `.env`):
```bash
LANGSMITH_API_KEY_DEV=ls__...
LANGSMITH_API_KEY_PROD=ls__...
LANGSMITH_PROJECT_DEV=hadathana_dev
LANGSMITH_PROJECT=hadathana_prod
```

`app/main.py` translates these into the canonical `LANGSMITH_TRACING` / `LANGSMITH_API_KEY` / `LANGSMITH_PROJECT` env vars at startup, switching by `APP_ENV`.

**Reference:** [LangSmith — Nesting Traces](https://docs.smith.langchain.com/observability/how_to_guides/nest_traces) (the "Combining decorated code with LangGraph and LangChain" pattern is what we use).

---

### Hard rules for all AI code

| Rule | Why |
|---|---|
| Always use `create_agent` | `AgentExecutor` / `initialize_agent` are deprecated and removed in 0.4+ |
| `@tool` docstring is mandatory | It is the tool description the LLM sees — vague docstrings cause wrong tool selection |
| LLM never writes Cypher or raw MongoDB | All DB access through typed Python helpers only |
| `CHATBOT_MODEL` always from env | Never hardcode model slug in agent code |
| `max_iterations=4` on all multi-tool agents | Prevents runaway tool loops |
| Start with static prompt, add `dynamic_prompt` only when measured | Premature context engineering adds complexity with no benefit |
| Qdrant sync after every Mongo data update | Run `scripts/sync_qdrant.py`, verify point count matches Mongo doc count |
| Pin package versions | `langchain>=0.3.15`, `langchain-qdrant>=0.2.0`, `langchain-cohere>=0.4.0` |
| Nest LangChain Runnables under a `trace()` parent with `tracing_context(parent=run)` | `async with trace()` alone does NOT set the contextvar that LangChain reads — the `tracing_context` bridge is required |
| Nest `@traceable` calls inside `asyncio.create_task` with `langsmith_extra={"parent": run}` | `create_task` runs in a fresh context; the LangSmith parent contextvar is reset, so explicit parent passing is the official fix |

### New env vars (chatbot)

```bash
QDRANT_URL=http://qdrant:6333        # docker service name; http://localhost:6333 locally
QDRANT_API_KEY=                      # empty for self-hosted, set for Qdrant Cloud
CHATBOT_MODEL=qwen/qwen3-235b-a22b   # OpenRouter model slug — swappable without code change
# OPENROUTER_API_KEY and COHERE_API_KEY already defined above
```

### Chatbot directory layout

```
app/chatbot/
  __init__.py
  config.py       # chatbot-specific settings (extends app/config.py)
  qdrant.py       # QdrantClient lifecycle (mirrors app/database.py)
  retriever.py    # build_retriever() — hybrid + rerank, one per collection
  agent.py        # create_agent() + all @tool definitions
  prompts.py      # Arabic system prompts — single source of truth, never inline
  router.py       # POST /api/v2/chat (SSE streaming)
  models.py       # ChatRequest, ChatResponse, HadithHit, NarratorHit, HadithAnswer
  graph.py        # (V3) async Neo4j helpers — parametrized Cypher only, LLM never writes Cypher

scripts/
  sync_qdrant.py       # CLI: populate/resync Qdrant from Mongo (idempotent)
  embed_narrators.py   # (V2) Cohere embeddings for narrator bios
```

## Deployment Notes

**Railway/Render**:
1. Platform auto-detects `Dockerfile`
2. Set env vars in platform dashboard (never commit them)
3. In MongoDB Atlas → Network Access → allow `0.0.0.0/0` or platform IP range
4. The FastAPI app binds to `0.0.0.0:8000` (see `Dockerfile`)

**Security**: Credentials must be passed at runtime via env vars. The Docker image contains NO secrets.
