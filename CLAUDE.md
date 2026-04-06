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
- Router structure: each data source has separate routers (`hadiths_shamela`, `narrators_shamela`, `hadiths_podia`, `narrators_podia`)

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
- `_plain` suffix: cleaned + tashkeel stripped (for search/matching)

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

| | Dev | Prod |
|---|---|---|
| API port | 8001 | 8000 |
| MongoDB port | 27017 (exposed) | internal only |
| Database | `HadithDataDev` | `HadithData` |
| Volume | `hadathana_mongodb_dev` | `hadathana_mongodb_prod` |
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

Add new services to `docker-compose.yml` (base). Commented-out templates for Qdrant and mongo-express are already included.

API docs: http://localhost:8001/docs (dev) / http://localhost:8000/docs (prod)

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
| `scripts/tag_topics_jsonl.py` | `bukhari_podia_hadiths.jsonl` | `hadith_topics.jsonl` | MongoDB (`mongoimport --mode=upsert`) |
| `scripts/embed_matn_jsonl.py` | `bukhari_podia_hadiths.jsonl` | `hadith_embeddings.jsonl` | Qdrant / vector DB (deferred) |

Both are **resumable** — re-running skips already-processed `hadith_url`s. Source JSONL is never modified.

```bash
# Run in tmux enrichment session (topics → embeddings, chained)
tmux new-session -d -s enrichment
tmux send-keys -t enrichment 'cd ~/Projects/hadathanaBackendApp && \
  /home/abdo_kamar/Projects/.venv/bin/python scripts/tag_topics_jsonl.py && \
  /home/abdo_kamar/Projects/.venv/bin/python scripts/embed_matn_jsonl.py' Enter

# After topics finish — import into HadithDataDev
docker exec -i mongodb-hadathana mongoimport --db HadithDataDev \
  --collection processed_podia_books --mode=upsert --upsertFields=hadith_url \
  < hadith_topics.jsonl
```

Requires `OPENROUTER_API_KEY` (topics) and `COHERE_API_KEY` (embeddings) in `.env`.

## Key Implementation Patterns

### API Endpoint Structure

All endpoints follow pagination pattern:
- Default: `skip=0`, `limit=20` (max 100)
- Returns `PaginatedHadiths` or `PaginatedNarrators` with `items[]` and `total`

Filter params use MongoDB regex for text search:
```python
if hadith_plain:
    query_filter["hadith_plain"] = {"$regex": hadith_plain, "$options": "i"}
```

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

## Deployment Notes

**Railway/Render**:
1. Platform auto-detects `Dockerfile`
2. Set env vars in platform dashboard (never commit them)
3. In MongoDB Atlas → Network Access → allow `0.0.0.0/0` or platform IP range
4. The FastAPI app binds to `0.0.0.0:8000` (see `Dockerfile`)

**Security**: Credentials must be passed at runtime via env vars. The Docker image contains NO secrets.
