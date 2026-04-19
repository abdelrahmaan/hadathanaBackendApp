# Hadathana API

FastAPI + MongoDB backend for the Hadathana Islamic hadith platform. Exposes the full Sahih al-Bukhari corpus (7,000+ hadiths) from two independent data pipelines with narrator chain analysis, Arabic semantic search, and an AI-powered RAG chatbot — **Al-Rawi**.

**System at a glance:**
- **7,076 hadiths** (Podia) + **7,008 hadiths** (Shamela) — two independent pipelines
- **98.3%** narrator disambiguation accuracy across 1,555 ambiguous name pairs
- **Hybrid semantic search** — Cohere `embed-v4.0` dense + BM25 sparse vectors, Cohere multilingual reranker
- **Al-Rawi chatbot** — LangChain RAG agent with SSE streaming, MongoDB session persistence, cited answers
- **32,000+ API requests** served · **~10.9 ms** avg latency · users across US, Egypt, Europe

---

## Quick Start

```bash
# 1. Copy env template and fill in credentials
cp .env.example .env

# 2. Start dev stack (MongoDB + bootstrap + API on port 8001)
make dev

# 3. Verify
curl http://localhost:8001/health
```

API docs: http://localhost:8001/docs (dev) / http://localhost:8000/docs (prod)

---

## Environments

Both dev and prod run on the same VPS using Docker Compose file merging.

| | Dev | Prod |
|---|---|---|
| API port | `8001` | `8000` |
| MongoDB port | `27017` (exposed) | internal only |
| Database | `HadithDataDev` | `HadithData` |
| Volume | `hadathana_mongodb_dev` | `hadathana_mongodb_prod` |
| Code reload | live (`--reload` + volume mount) | baked into image |

---

## Commands

### Dev

```bash
make dev              # start dev stack (API on :8001)
make dev-logs         # follow dev API logs
make dev-down         # stop dev stack
make dev-ps           # check dev services status
```

### Prod

```bash
make prod             # build image + start prod stack (API on :8000)
make prod-logs        # follow prod API logs
make prod-down        # stop prod stack
make prod-ps          # check prod services status
make prod-restart     # restart prod API only (no rebuild)
```

### Both stacks

```bash
make status           # show running containers for both stacks
make health           # health check both APIs
```

### Testing

```bash
# Unit tests — mocked, no server needed
make test             # all tests
make test-chatbot     # chatbot unit tests only

# Smoke tests — require a live stack
make test-chatbot-dev    # chatbot smoke tests against dev :8001  (requires: make dev)
make test-chatbot-prod   # chatbot smoke tests against prod :8000 (requires: make prod)

# Data presence tests — verify hadiths/narrators/topics are loaded
make test-db-dev         # check data in dev :8001  (requires: make dev)
make test-db-prod        # check data in prod :8000 (requires: make prod)
```

---

## No Data? Recovery Guide

If the API returns `{"items": [], "total": 7076}` (total > 0 but items empty), or the health endpoint shows degraded collections, the database has corrupted or missing documents.

### Step 1 — Diagnose

```bash
# Check health
curl http://localhost:8001/health

# Check if hadiths load
curl "http://localhost:8001/api/v2/hadiths?limit=1"

# Check API logs for validation errors
make dev-logs
```

If you see `Skipping malformed hadith doc` warnings, documents were corrupted (missing required fields). This usually means a partial-field import replaced full documents.

### Step 2 — Restore the collection

```bash
# Shell into the dev MongoDB container
docker exec -it hadathana-mongo-dev mongosh HadithDataDev

# Drop the corrupted collection
db.processed_podia_books.drop()
exit
```

### Step 3 — Re-import full hadith data

```bash
# Import from the full JSONL (must be present in the repo)
docker exec -i hadathana-mongo-dev mongoimport \
  --db HadithDataDev --collection processed_podia_books \
  < mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl
```

If the JSONL is missing, pull it from R2 first:
```bash
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_podia --latest
```

### Step 4 — Re-apply enrichments (topics)

> ⚠️ Never use `mongoimport --mode=upsert` or `--mode=merge` to apply partial field updates — it replaces entire documents. Always use PyMongo `$set`.

```bash
# Apply topics (merges topics field, preserves all hadith data)
/home/abdo_kamar/Projects/.venv/bin/python - <<'EOF'
import json
from pymongo import MongoClient, UpdateOne

client = MongoClient("mongodb://localhost:27017/")
col = client["HadithDataDev"]["processed_podia_books"]
ops = [
    UpdateOne({"hadith_url": json.loads(l)["hadith_url"]}, {"$set": {"topics": json.loads(l)["topics"]}})
    for l in open("hadith_topics.jsonl")
]
result = col.bulk_write(ops, ordered=False)
print(f"matched: {result.matched_count}, modified: {result.modified_count}")
EOF
```

### Step 5 — Rebuild indexes

```bash
/home/abdo_kamar/Projects/.venv/bin/python mongo_migration/create_indexes.py
```

### Step 6 — Verify

```bash
curl http://localhost:8001/health
curl "http://localhost:8001/api/v2/hadiths?limit=2"
curl http://localhost:8001/api/v2/topics
```

### Alternative: full auto-bootstrap

If you want to wipe and fully restart from scratch:

```bash
# Stop stack and destroy dev volume (WARNING: deletes all dev data)
make dev-down
docker volume rm hadathana_mongodb_dev

# Restart — mongo-init will auto-bootstrap from JSONL files
make dev
```

### Services and Ports

| Service | Dev port | Prod port | Notes |
|---------|----------|-----------|-------|
| FastAPI | `8001` | `8000` | Main API |
| MongoDB | `27017` | internal | Exposed only in dev |
| Prometheus | `9090` | `9091` | Metrics scraper (30-day retention) |
| Grafana | `3002` | `3001` | Dashboards (login: admin / see `.env`) |
| Qdrant | `6333` | `6333` | Vector DB — hadiths_matn collection (7,073 vectors) |

---

## Feature Development Workflow

```
feat branch  →  dev :8001  →  validate  →  merge main  →  make prod :8000
```

### 1. Create a feature branch

```bash
git checkout main
git checkout -b feat/your-feature
```

### 2. Develop on dev (live reload)

```bash
make dev      # already running? skip this
# edit code in app/ — live reload picks up changes instantly
# test at http://localhost:8001/docs
```

### 3. Run tests

```bash
make test
make test-chatbot-dev   # chatbot smoke tests against live dev stack
make test-db-dev        # data presence tests — verify hadiths/narrators/topics loaded
```

### 4. Merge to main and promote to prod

```bash
git checkout main
git merge feat/your-feature
make prod     # rebuilds image from main, restarts prod on :8000
```

Prod rebuild takes ~20–30 seconds. The old container keeps serving until the new one is ready.

### 5. Rollback if something breaks

```bash
git checkout <last-good-commit>
make prod
```

---

## CI/CD

GitHub Actions runs on every pull request targeting `main`. The pipeline has two sequential jobs:

| Job | Tool | What it checks |
|-----|------|---------------|
| `lint` | `ruff` | Code style and import order across `app/` and `tests/` |
| `test` | `pytest` | Full test suite (runs only if lint passes) |

Workflow file: [`.github/workflows/ci.yml`](.github/workflows/ci.yml)

Tests run with a mocked MongoDB connection (`APP_ENV=dev`, `MONGODB_URI_LOCAL=mongodb://localhost:27017/`). To run locally:

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m ruff check app/ tests/
"$PYTHON" -m pytest tests/ -v
```

---

## Monitoring

The stack ships Prometheus + Grafana out of the box — no manual setup needed. Both are provisioned automatically on `make dev` / `make prod`.

### Prometheus

Scrapes the FastAPI `/metrics` endpoint every 15 seconds. Metrics include:

- **RED**: request rate, error rate (5xx), response latency (p50/p95/p99) — per endpoint
- **Process**: Python RSS memory, virtual memory, CPU, GC stats

Config files:
- `monitoring/prometheus.dev.yml` — scrapes `api:8001`
- `monitoring/prometheus.yml` — scrapes `api:8000` (prod)

Access: http://localhost:9090 (dev) / http://localhost:9091 (prod)

### Grafana

Auto-provisioned with a Prometheus datasource and a pre-built **"Hadathana API Overview"** dashboard (11 panels):

| Panel | Description |
|-------|-------------|
| Total requests / current req/s | Stat panels |
| Avg latency / RSS memory | Stat panels |
| Request rate by endpoint | Time series |
| Error rate (5xx/total) | Time series |
| Response latency p50/p95/p99 | Time series |
| Latency by endpoint (p95) | Time series |
| Requests in progress | Gauge |
| Response size by endpoint | Time series |
| Python memory (RSS + virtual) | Time series |

Access: http://localhost:3002 (dev) / http://localhost:3001 (prod)
Login: `admin` / value of `GRAFANA_ADMIN_PASSWORD` in `.env` (default: `admin`)

Dashboard source: `monitoring/grafana/dashboards/api-overview.json`
Provisioning: `monitoring/grafana/provisioning/`

### Qdrant

Vector database for hybrid semantic hadith search. The `hadiths_matn` collection holds **7,073 points** with:
- **Dense vectors** — 1,536-dim Cohere `embed-v4.0` embeddings
- **Sparse vectors** — BM25 via FastEmbed (`text-sparse`) for keyword recall
- **Payload** — `matn_text_plain`, `book`, `chapter`, `topics`, `title`, `hadith_url`, `narrators`

Access: http://localhost:6333/dashboard

---

## Bootstrap (manual / recovery)

`mongo-init` handles this automatically on `make dev` / `make prod`. For manual recovery:

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"

# Bootstrap HadithDataDev (skips if data already exists)
"$PYTHON" scripts/bootstrap_local_db.py

# Bootstrap HadithData (prod DB)
"$PYTHON" scripts/bootstrap_local_db.py --db HadithData

# Force full re-import even if data exists
"$PYTHON" scripts/bootstrap_local_db.py --force
```

---

## Al-Rawi — RAG Chatbot

**Al-Rawi** ("the narrator") is an AI assistant that answers Islamic questions grounded exclusively in Sahih al-Bukhari. It is exposed at `POST /api/v2/chat` and streams responses via Server-Sent Events (SSE).

### Architecture

```
User question
    │
    ▼
LangChain Agent (LangGraph, tool-calling)
    │  calls search_hadiths tool
    ▼
Hybrid Retriever (Qdrant)
    ├── Dense: Cohere embed-v4.0  ──┐
    └── Sparse: BM25 (FastEmbed) ──┴─► merged candidates (top-20)
    │
    ▼
Cohere Reranker (rerank-multilingual-v3.0, top-5)
    │  relevance score filter (≥ 0.30)
    ▼
LLM (via OpenRouter) — streams Arabic answer with citations
    │
    ▼
SSE stream: assistant_message_start → content chunks → assistant_message_complete
            → thread_rename → stream_end
    │
    ▼
MongoDB session persistence (chat_sessions collection)
```

### Key design decisions

| Decision | Detail |
|----------|--------|
| Hybrid search | Dense + BM25 combined — better recall for rare terms |
| Reranker | Cohere multilingual v3 — re-scores candidates, not keywords |
| Relevance filter | Docs scoring < 0.30 dropped before reaching the LLM |
| Grounding | LLM instructed never to answer from prior knowledge |
| Citations | Each passage numbered [١][٢]… — REFS:[n,n] parsed from LLM output → `Citation` objects returned to frontend |
| Thread titles | Auto-generated (Gemini Flash) on first turn — `thread_rename` SSE event |
| Session store | MongoDB `chat_sessions` collection — full turn history persisted |

### SSE event stream

```
data: {"type": "assistant_message_start", "content": "", "session_id": "..."}
data: {"type": "content", "content": "إنما الأعمال بالنيات"}
...
data: {"type": "assistant_message_complete", "data": {"message_type": "assistant", "content": "...", "citations": [...]}}
data: {"type": "thread_rename", "title": "أول حديث في صحيح البخاري"}
data: {"type": "stream_end"}
```

### Chatbot module layout

```
app/chatbot/
├── agent.py       # LangChain create_agent + search_hadiths @tool
├── retriever.py   # Hybrid Qdrant retriever + Cohere reranker
├── indexer.py     # Qdrant collection setup + Mongo → Qdrant sync
├── router.py      # FastAPI SSE endpoint (POST /api/v2/chat)
├── session.py     # MongoDB session persistence
├── models.py      # ChatRequest, Citation, SessionTurn
├── prompts.py     # Arabic system prompt + thread rename prompt
├── config.py      # Module constants (collection, model names, thresholds)
└── qdrant.py      # QdrantClient singleton
```

### Test the chatbot

```bash
# Smoke test — SSE stream (watch events appear)
curl -s -X POST http://localhost:8001/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "ما هو أول حديث في صحيح البخاري؟"}' \
  --no-buffer

# Run unit tests
make test-chatbot

# Run smoke tests against dev
make test-chatbot-dev
```

---

## Endpoints

### Auth

All auth endpoints are under `/auth`. Login uses **form data** (`application/x-www-form-urlencoded`), all others use JSON. Sessions are managed via **HttpOnly cookies** — frontend must send requests with `credentials: "include"` (fetch) or `withCredentials: true` (axios).

| Method | Path | Body | Response | Notes |
|--------|------|------|----------|-------|
| POST | `/auth/register` | `{ "email", "password" }` | `201` UserRead | |
| POST | `/auth/login` | form: `username=&password=` | `204` + sets cookie | |
| POST | `/auth/logout` | — | `204` + clears cookie | |
| POST | `/auth/forgot-password` | `{ "email" }` | `202` always | No user enumeration |
| POST | `/auth/reset-password` | `{ "token", "password" }` | `200` | Token from email link |

### Bookmarks (authenticated)

Require valid session cookie. Returns `401` if not logged in.

| Method | Path | Body | Response |
|--------|------|------|----------|
| GET | `/api/v2/bookmarks` | — | `{ items[], total }` (skip/limit) |
| POST | `/api/v2/bookmarks` | `{ "hadith_url", "source" }` | `201` BookmarkRead · `409` if duplicate |
| DELETE | `/api/v2/bookmarks/{hadith_url}` | — | `204` · `404` if not found |

### v1 — Shamela

| Method | Path | Query params | Response |
|--------|------|-------------|----------|
| GET | `/api/v1/hadiths` | `hadith_plain`, `narrator_id`, `chain_type`, `skip`, `limit` | `PaginatedHadiths` |
| GET | `/api/v1/hadiths/{hadith_index}` | — | `Hadith` |
| GET | `/api/v1/narrators` | `name_plain`, `kunya`, `nasab`, `skip`, `limit` | `PaginatedNarrators` |
| GET | `/api/v1/narrators/{narrator_id}` | — | `Narrator` |
| GET | `/api/v1/narrators/{narrator_id}/stats` | — | `NarratorStats` |

### v2 — Podia

| Method | Path | Query params | Response |
|--------|------|-------------|----------|
| GET | `/api/v2/hadiths` | `hadith_text_plain`, `rawi_id`, `book`, `topic`, `skip`, `limit` | `PaginatedPodiaHadiths` |
| GET | `/api/v2/hadiths/{hadith_index}` | — | `PodiaHadith` |
| GET | `/api/v2/narrators` | `full_name_plain`, `rank`, `skip`, `limit` | `PaginatedPodiaNarrators` |
| GET | `/api/v2/narrators/{rawi_id}` | — | `PodiaNarrator` |
| GET | `/api/v2/narrators/{rawi_id}/tarajem` | — | `PodiaNarratorTarajem` |
| GET | `/api/v2/narrators/{rawi_id}/stats` | — | `PodiaNarratorStats` |
| GET | `/api/v2/topics` | — | `TopicsResponse` |
| GET | `/api/v2/topics/{topic}/hadiths` | `skip`, `limit` | `PaginatedPodiaHadiths` |
| POST | `/api/v2/chat` | body: `{ "question", "session_id?" }` | SSE stream |
| GET | `/health` | — | `{ "status": "ok", "chatbot": true/false }` |

### Chatbot feature flag

Set `CHATBOT_ENABLED=false` in `.env` to disable all chatbot routes and skip Qdrant on startup. The `/health` endpoint exposes `"chatbot": true/false` so the frontend can show/hide the chat icon accordingly.

To toggle without a full rebuild:

```bash
# Edit .env: CHATBOT_ENABLED=false

# Dev (already running)
docker restart hadathana-api-dev

# Prod (already running)
make prod-restart
```

### Chatbot — POST /api/v2/chat

```bash
# New session (server generates session_id):
curl -X POST http://localhost:8001/api/v2/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"ما حكم النية في الصلاة؟"}' --no-buffer

# Resume session (pass session_id from first assistant_message_start event):
curl -X POST http://localhost:8001/api/v2/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"وضح أكثر","session_id":"<uuid>"}' --no-buffer
```

Response is `text/event-stream` (SSE). Event types:

| Event | Description |
|---|---|
| `assistant_message_start` | Stream begins; includes `session_id` for new sessions |
| `content` | Token chunk |
| `assistant_message_complete` | Full text + `citations[]` (only cited hadiths, filtered by REFS line) |
| `thread_rename` | Suggested Arabic conversation title |
| `stream_end` | Stream closed |

---

## Example Requests

```bash
# Health
curl http://localhost:8001/health

# Auth
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"Str0ngPass!123"}'

curl -c cookies.txt -X POST http://localhost:8000/auth/login \
  -d 'username=user@example.com&password=Str0ngPass!123'

# Bookmarks (authenticated)
curl -b cookies.txt http://localhost:8000/api/v2/bookmarks
curl -b cookies.txt -X POST http://localhost:8000/api/v2/bookmarks \
  -H "Content-Type: application/json" \
  -d '{"hadith_url":"https://hadathana.app/hadith/1","source":"podia"}'
curl -b cookies.txt -X DELETE "http://localhost:8000/api/v2/bookmarks/https%3A%2F%2Fhadathana.app%2Fhadith%2F1"

# v1 — Shamela
curl http://localhost:8001/api/v1/hadiths
curl "http://localhost:8001/api/v1/hadiths?hadith_plain=نام"
curl "http://localhost:8001/api/v1/hadiths?narrator_id=822"
curl http://localhost:8001/api/v1/narrators/822/stats

# v2 — Podia
curl http://localhost:8000/api/v2/hadiths
curl "http://localhost:8000/api/v2/hadiths?hadith_text_plain=الصلاة"
curl "http://localhost:8000/api/v2/hadiths?topic=الصلاة"
curl http://localhost:8000/api/v2/narrators/822/tarajem
curl http://localhost:8000/api/v2/narrators/822/stats
curl http://localhost:8000/api/v2/topics
curl "http://localhost:8000/api/v2/topics/الصلاة/hadiths"

# Al-Rawi chatbot (SSE stream)
curl -s -X POST http://localhost:8000/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "ما حكم الصلاة في وقتها؟"}' --no-buffer
```

### Arabic Search Normalization

Search is normalization-tolerant — variant spellings match automatically:


```bash
# taa marbuta: الصلاه = الصلاة
curl "http://localhost:8001/api/v2/hadiths?hadith_text_plain=الصلاه"

# hamza: ابراهيم = إبراهيم
curl "http://localhost:8001/api/v1/narrators?name_plain=ابراهيم"

# alef maqsura: موسي = موسى
curl "http://localhost:8001/api/v2/narrators?full_name_plain=موسي"
```

---

## Data Field Comparison

### Hadith fields

| Field | v1 Shamela | v2 Podia | Notes |
|-------|:---:|:---:|-------|
| `hadith_index` / `hadith_indices` | ✅ single int | ✅ list | Podia hadiths can span multiple indices |
| `hadith` / `hadith_text` | ✅ | ✅ | Full text with tashkeel |
| `hadith_plain` / `hadith_text_plain` | ✅ | ✅ | Tashkeel stripped |
| `hadith_search` / `hadith_text_search` | ✅ | ✅ | Fully normalized for search |
| `sanad_text`, `matn_text`, `tawabi_text` | ❌ | ✅ | Segmented text |
| `book`, `chapter` | ❌ | ✅ | |
| `hadith_url` | ❌ | ✅ | Source URL |
| `topics[]` | ❌ | ✅ | Arabic semantic topic tags (LLM-generated) |
| `matn_embedding` | ❌ | ✅ | 1536-dim Cohere vector (for semantic search) |
| `chains[]` | ✅ | ✅ | Multi-chain with transmission types |
| `narrators[]` | ❌ | ✅ | Per-hadith narrator list |

### Narrator fields

| Field | v1 Shamela | v2 Podia |
|-------|:---:|:---:|
| `name` / `name_in_chain` | ✅ | ✅ |
| `full_name`, `full_name_plain` | ❌ | ✅ |
| `kunya`, `nasab`, `tabaqa` | ✅ | ❌ |
| `rank`, `rank_plain` | ❌ | ✅ |
| `rank_ibn_hajar`, `rank_dhahabi` | ✅ | ❌ |
| `jarh_wa_tadil[]` | ✅ | via `/tarajem` |

---

## Environment Variables

See `.env.example` for the full list. Key variables:

```bash
APP_ENV=dev                          # "dev" or "prod"

# MongoDB — prod (Atlas)
MONGODB_URI_READ=mongodb+srv://...
MONGODB_URI_READ_WRITE=mongodb+srv://...
DB_NAME=HadithData

# MongoDB — dev (local Docker)
MONGODB_URI_LOCAL=mongodb://localhost:27017/
DB_NAME_DEV=HadithDataDev

# CORS
CORS_ORIGINS=https://hadathana.app,https://www.hadathana.app
CORS_ORIGINS_DEV=http://localhost:3000,http://localhost:5173

# Chatbot
CHATBOT_ENABLED=true                 # set to false to disable chatbot routes + Qdrant entirely
COHERE_API_KEY=
OPENROUTER_API_KEY=
QDRANT_URL=http://qdrant:6333
CHATBOT_MODEL=qwen/qwen3-235b-a22b

# Cloudflare R2 (data snapshots)
R2_ENDPOINT_URL=
R2_BUCKET=hadathana-data
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
```

---

## Data Snapshots (Cloudflare R2)

All JSONL data files are stored in R2, not git. Pull before any data work.

```bash
# See available snapshots
python scripts/r2_sync/list_snapshots.py

# Pull latest snapshot
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_podia --latest
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_shamela --latest
```

Full docs: [scripts/r2_sync/README.md](scripts/r2_sync/README.md)

---

## Data Update Workflow

When enriching or modifying data (adding topics, embeddings, new fields):

**1. Pull latest data from R2**
```bash
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_podia --latest
```

**2. Run against dev first**
```bash
# Ensure APP_ENV=dev in .env
python scripts/your_script.py
```

**3. Verify in dev**
```bash
curl "http://localhost:8001/api/v2/hadiths?limit=3"
make test-chatbot-dev
```

**4. Push enriched JSONL to R2**
```bash
python scripts/r2_sync/push_snapshot.py --dataset bukhari_podia \
  --source mongo_migration/processed_bukhari_podia/ --extensions jsonl
```

**5. Promote to prod**
```bash
docker exec hadathana-mongo-dev mongodump --db HadithDataDev --out /tmp/dump
docker exec hadathana-mongo-prod mongorestore --db HadithData /tmp/dump/HadithDataDev
make prod-restart
```

---

## Deploy to Railway / Render

> Currently running on a VPS with local Docker MongoDB. These steps apply if migrating to a cloud platform.

1. Push repo to GitHub
2. Create project on Railway or Render, connect the repo
3. Set env vars in the platform dashboard (see `.env.example`)
4. Platform auto-detects `Dockerfile` and deploys

---

## Neo4j Graph (optional)

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"

# Dry run
"$PYTHON" mongo_migration/processed_bukhari_podia/build_graph.py --dry-run

# Full ingestion
"$PYTHON" mongo_migration/processed_bukhari_podia/build_graph.py

# Manage container
docker start neo4j-hadith
docker stop neo4j-hadith
# Browser: http://localhost:7474
```
