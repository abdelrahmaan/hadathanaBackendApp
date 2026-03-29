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

### Development Server

```bash
# Run FastAPI dev server (assumes uv venv in parent directory)
/Users/a.kamar/Documents/Abdo\ Kaamar/projects/.venv/bin/uvicorn app.main:app --reload

# Or if using a local venv:
uvicorn app.main:app --reload
```

API docs: http://localhost:8000/docs

### Tests

```bash
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"
"$PYTHON" -m pytest tests/ -v
```

### Docker

```bash
# Build image
docker build -t hadathna-api .

# Run container (pass env vars at runtime, never bake into image)
docker run -p 8000:8000 \
  -e MONGODB_URI_READ="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/" \
  -e DB_NAME="HadithData" \
  -e CORS_ORIGINS="*" \
  hadathna-api
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
# MongoDB (FastAPI)
MONGODB_URI_READ=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
DB_NAME=HadithData
CORS_ORIGINS=*

# Neo4j (graph scripts)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Cloudflare R2 (data snapshots — see scripts/r2_sync/README.md)
R2_ENDPOINT_URL=https://<account_id>.r2.cloudflarestorage.com
R2_BUCKET=hadathana_data
R2_ACCESS_KEY_ID=<your_access_key>
R2_SECRET_ACCESS_KEY=<your_secret_key>
```

**Note**: Use `MONGODB_URI_READ` in production (follows principle of least privilege). See `app/config.py` for settings loading.

## Python Environment

The project uses a shared `uv` venv at parent directory level:
```bash
/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/
```

When running scripts, use the full path to the Python interpreter:
```bash
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"
"$PYTHON" mongo_migration/upload.py
```

## Data Storage

**All data files are stored in Cloudflare R2** (S3-compatible object storage), not in git. The repo `.gitignore` excludes all `.json`, `.jsonl`, `.csv`, `.xlsx`, and `.parquet` files under data directories. After cloning, pull data with the R2 sync scripts (see below).

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
