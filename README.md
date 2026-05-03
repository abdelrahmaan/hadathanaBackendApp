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

### 4. Open a PR and merge

Push your branch and open a PR to `main` on GitHub. CI runs automatically (lint + tests). Once it passes, merge — the CD pipeline deploys to prod automatically via SSH.

### 5. Rollback if something breaks

```bash
git revert HEAD   # or checkout a previous commit
git push origin main   # triggers CD again with the reverted code
```

Or manually on the VPS:
```bash
git checkout <last-good-commit>
make prod
```

---

## CI/CD

### Flow

```
feature branch  →  PR to main  →  CI (lint + test)  →  merge  →  auto-deploy to VPS
```

### CI — pull requests

GitHub Actions runs on every PR targeting `main`. Two sequential jobs:

| Job | Tool | What it checks |
|-----|------|---------------|
| `lint` | `ruff` | Code style and import order across `app/` and `tests/` |
| `test` | `pytest` | Unit + quota tests (runs only if lint passes) |

Workflow: [`.github/workflows/ci.yml`](.github/workflows/ci.yml)

### CD — deploy on merge

Merging to `main` triggers automatic deployment to the VPS:

1. SSH into the VPS
2. `git pull origin main`
3. `make prod` — rebuilds image, restarts prod stack, waits up to 120s for health check

Workflow: [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml)

Required GitHub secrets: `VPS_HOST`, `VPS_USER`, `VPS_SSH_KEY`.

### Run CI checks locally

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m ruff check app/ tests/
"$PYTHON" -m pytest tests/ -v
```

### Release tagging

```bash
make tag VERSION=v1.0.0
git push origin v1.0.0
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
| Session store | MongoDB `chat_sessions_dev/prod` — full turn history persisted, owned by authenticated user |
| Auth | `POST /api/v2/chat` requires JWT cookie — returns `401` if unauthenticated |
| Ownership | Sessions are user-scoped — resuming another user's session returns `403` |
| Quota | Per-user daily limits (free=3, supporter=10) — atomic MongoDB `$inc`, returns `429` before LLM call |

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

> All paginated responses accept `skip` (default `0`) and `limit` (default `20`, max `100`) query params.
> Text search params accept Arabic with or without tashkeel — normalization is applied automatically.
> Auth endpoints use **HttpOnly JWT cookies**. Frontend must send `credentials: "include"` (fetch) or `withCredentials: true` (axios).

---

### `GET /health`

No input. Returns service health and collection counts.

```json
{
  "status": "ok",
  "mongodb": "connected",
  "chatbot": true,
  "collections": {
    "processed_podia_books": 7076,
    "processed_podia_narrators": 1555
  }
}
```

---

### Auth

#### `POST /auth/register`

**Body** (JSON):
```json
{ "email": "user@example.com", "password": "Str0ngPass!123" }
```

**Response** `201`:
```json
{ "id": "<uuid>", "email": "user@example.com", "is_active": true, "is_verified": false, "tier": "free" }
```

#### `POST /auth/login`

**Body** (form data — `application/x-www-form-urlencoded`):
```
username=user@example.com&password=Str0ngPass!123
```

**Response** `204` + sets `fastapiusersauth` HttpOnly cookie.

#### `POST /auth/logout`

No body. **Response** `204` + clears cookie.

#### `POST /auth/forgot-password`

**Body**: `{ "email": "user@example.com" }`
**Response** `202` always (no user enumeration).

#### `POST /auth/reset-password`

**Body**: `{ "token": "<token-from-email>", "password": "NewPass!123" }`
**Response** `200`.

---

### Bookmarks — requires auth cookie

#### `GET /api/v2/bookmarks`

Query params: `skip`, `limit`

**Response**:
```json
{
  "items": [
    { "hadith_url": "https://...", "source": "podia", "created_at": "2024-01-01T00:00:00Z" }
  ],
  "total": 12
}
```

#### `POST /api/v2/bookmarks`

**Body**:
```json
{ "hadith_url": "https://hadathana.app/hadith/1", "source": "podia" }
```

**Response** `201`:
```json
{ "hadith_url": "https://...", "source": "podia", "created_at": "2024-01-01T00:00:00Z" }
```
Returns `409` if already bookmarked.

#### `DELETE /api/v2/bookmarks/{hadith_url}`

URL-encode the `hadith_url` path segment. No body. **Response** `204`. Returns `404` if not found.

---

### v1 — Shamela Hadiths

#### `GET /api/v1/hadiths`

Query params:

| Param | Type | Description |
|-------|------|-------------|
| `hadith_plain` | string | Search hadith text (Arabic, normalized) |
| `narrator_id` | int | Filter by narrator ID |
| `chain_type` | string | Filter by chain type (`primary`, `nested`, `follow_up`) |
| `skip` / `limit` | int | Pagination |

**Response**:
```json
{
  "items": [
    {
      "id": "<mongo_id>",
      "hadith_index": 1,
      "source": "shamela",
      "hadith": "حَدَّثَنَا الْحُمَيْدِيُّ...",
      "hadith_plain": "حدثنا الحميدي...",
      "matn_plain": ["إنما الأعمال بالنيات"],
      "n_matn": 1,
      "n_chains": 1,
      "chains": [
        {
          "chain_id": "c1",
          "type": "primary",
          "narrators": [
            { "name": "البخاري", "role": "lead", "narrator_id": 1 }
          ]
        }
      ],
      "unique_narrators": [{ "name": "عمر بن الخطاب", "narrator_id": 822 }]
    }
  ],
  "total": 7008
}
```

#### `GET /api/v1/hadiths/{hadith_index}`

**Response**: Single `Hadith` object (same shape as items above). Returns `404` if not found.

---

### v1 — Shamela Narrators

#### `GET /api/v1/narrators`

Query params:

| Param | Type | Description |
|-------|------|-------------|
| `name_plain` | string | Search by name (Arabic, normalized) |
| `kunya` | string | Search by kunya (e.g. أبو عبدالله) |
| `nasab` | string | Search by nasab (lineage) |
| `skip` / `limit` | int | Pagination |

**Response**:
```json
{
  "items": [
    {
      "id": "<mongo_id>",
      "narrator_id": 822,
      "name": "عُمَرُ بْنُ الْخَطَّابِ",
      "name_plain": "عمر بن الخطاب",
      "kunya": "أبو حفص",
      "nasab": "القرشي",
      "death_date": "23 هـ",
      "tabaqa": "الصحابة",
      "rank_ibn_hajar": "صحابي",
      "rank_dhahabi": "صحابي",
      "jarh_wa_tadil": [
        { "scholar": "ابن حجر", "quotes": ["ثقة"] }
      ]
    }
  ],
  "total": 380
}
```

#### `GET /api/v1/narrators/{narrator_id}`

**Response**: Single `Narrator` object. Returns `404` if not found.

#### `GET /api/v1/narrators/{narrator_id}/stats`

**Response**:
```json
{
  "narrator_id": 822,
  "hadith_count": 45,
  "teachers": [{ "narrator_id": 100, "name": "النبي ﷺ", "freq": 30 }],
  "students": [{ "narrator_id": 900, "name": "ابن عمر", "freq": 20 }]
}
```
`freq` = number of distinct hadiths in which the teacher→student pair appears together.

---

### v2 — Podia Hadiths

#### `GET /api/v2/hadiths`

Query params:

| Param | Type | Description |
|-------|------|-------------|
| `hadith_text_plain` | string | Search full hadith text (Arabic, normalized) |
| `rawi_id` | int | Filter by narrator ID |
| `book` | string | Filter by book name |
| `topic` | string | Filter by topic tag |
| `skip` / `limit` | int | Pagination |

**Response**:
```json
{
  "items": [
    {
      "id": "<mongo_id>",
      "hadith_url": "https://bukhari-pedia.net/...",
      "hadith_indices": [1],
      "source": "podia",
      "book": "كتاب بدء الوحي",
      "chapter": "باب كيف كان بدء الوحي",
      "hadith_text": "حَدَّثَنَا الْحُمَيْدِيُّ...",
      "hadith_text_plain": "حدثنا الحميدي...",
      "sanad_text": "حَدَّثَنَا...",
      "sanad_text_plain": "حدثنا...",
      "matn_text": "إِنَّمَا الْأَعْمَالُ بِالنِّيَّاتِ",
      "matn_text_plain": "إنما الأعمال بالنيات",
      "tawabi_text": null,
      "topics": ["النية", "الأعمال"],
      "title": "حديث النية",
      "chains": [
        {
          "chain_id": "c1",
          "type": "primary",
          "narrators": [
            {
              "rawi_id": 1,
              "name": "البخاري",
              "name_clean": "البخاري",
              "name_plain": "البخاري",
              "role": "lead",
              "transmission": "حدثنا",
              "transmission_type": "سماع",
              "is_explicit_hearing": true
            }
          ]
        }
      ],
      "narrators": [
        { "rawi_id": 822, "name_in_chain": "عمر", "name_in_chain_clean": "عمر", "name_in_chain_plain": "عمر" }
      ]
    }
  ],
  "total": 7076
}
```

#### `GET /api/v2/hadiths/{hadith_index}`

**Response**: Single `PodiaHadith` object. Returns `404` if not found.

---

### v2 — Podia Narrators

#### `GET /api/v2/narrators`

Query params:

| Param | Type | Description |
|-------|------|-------------|
| `full_name_plain` | string | Search by full name (Arabic, normalized) |
| `rank` | string | Filter by reliability rank |
| `skip` / `limit` | int | Pagination |

**Response**:
```json
{
  "items": [
    {
      "id": "<mongo_id>",
      "rawi_id": 822,
      "name_in_chain": "عُمَرُ",
      "name_in_chain_clean": "عمر",
      "name_in_chain_plain": "عمر",
      "full_name": "عُمَرُ بْنُ الْخَطَّابِ",
      "full_name_plain": "عمر بن الخطاب",
      "rank": "صَحَابِيٌّ",
      "rank_plain": "صحابي",
      "full_tooltip_info": "..."
    }
  ],
  "total": 1555
}
```

#### `GET /api/v2/narrators/{rawi_id}`

**Response**: Single `PodiaNarrator` object. Returns `404` if not found.

#### `GET /api/v2/narrators/{rawi_id}/stats`

**Response**:
```json
{
  "rawi_id": 822,
  "hadith_count": 45,
  "teachers": [{ "rawi_id": 100, "name": "النبي ﷺ", "freq": 30 }],
  "students": [{ "rawi_id": 900, "name": "ابن عمر", "freq": 20 }]
}
```

#### `GET /api/v2/narrators/{rawi_id}/tarajem`

**Response**:
```json
{
  "id": "<mongo_id>",
  "rawi_id": 822,
  "url": "https://...",
  "name_in_chain": "عمر",
  "full_name": "عمر بن الخطاب",
  "rank": "صحابي",
  "narrator_info": [
    { "action": "ولادة", "text": "...", "text_plain": "..." }
  ],
  "tarajim": [
    { "source": "ابن حجر", "tarjama": "...", "tarjama_plain": "..." }
  ]
}
```

---

### v2 — Topics

#### `GET /api/v2/topics`

No params. Returns all distinct topic tags with hadith counts.

**Response**:
```json
{
  "items": [
    { "topic": "الصلاة", "count": 312 },
    { "topic": "الزكاة", "count": 89 }
  ],
  "total": 47
}
```

#### `GET /api/v2/topics/{topic}/hadiths`

Query params: `skip`, `limit`

**Response**: `PaginatedPodiaHadiths` — same shape as `GET /api/v2/hadiths`.

---

### Admin — superuser only

#### `GET /api/v2/admin/stats`

Requires auth cookie + `is_superuser: true`. Returns a single JSON object with system health, user/quota breakdown, chatbot activity, and data collection sizes.

**Response**:
```json
{
  "system": {
    "status": "ok",
    "mongodb": "connected",
    "qdrant": "connected",
    "chatbot_enabled": true
  },
  "users": {
    "total": 42,
    "by_tier": { "free": 38, "supporter": 3, "unlimited": 1 },
    "quota_used_today": 15,
    "users_at_limit_today": 2
  },
  "chatbot": {
    "total_sessions": 120,
    "total_messages": 540,
    "messages_today": 18,
    "avg_messages_per_session": 4.5
  },
  "data": {
    "podia_hadiths": 7076,
    "shamela_hadiths": 7008,
    "podia_narrators": 1555,
    "topics": 47,
    "qdrant_points": 7073
  }
}
```

Returns `401` if not authenticated, `403` if not superuser.

To grant superuser access:
```bash
# Dev
docker exec -it hadathana-mongo-dev mongosh HadithDataDev --eval \
  'db.auth_users.updateOne({email: "you@example.com"}, {$set: {is_superuser: true}})'

# Prod
docker exec -it hadathana-mongo-prod mongosh HadithData --eval \
  'db.auth_users.updateOne({email: "you@example.com"}, {$set: {is_superuser: true}})'
```

#### `PATCH /api/v2/admin/users/{user_id}/tier`

Upgrade or downgrade a user's tier. Superuser only.

**Path param:** `user_id` — the user's UUID string (from `auth_users.id`)

**Body**:
```json
{ "tier": "supporter" }
```

Valid values: `"free"` · `"supporter"` · `"unlimited"`

**Response** `200`:
```json
{
  "id": "<uuid>",
  "email": "user@example.com",
  "tier": "supporter",
  "is_active": true,
  "is_superuser": false
}
```

Returns `404` if user not found, `422` if tier value is invalid, `403` if not superuser.

---

### v2 — Al-Rawi Chatbot — requires auth cookie

#### `POST /api/v2/chat`

**Body**:
```json
{
  "question": "ما حكم الصلاة في وقتها؟",
  "session_id": null,
  "topic": null,
  "book": null
}
```

- `session_id`: omit or `null` to start a new session; pass a UUID to resume.
- `topic` / `book`: optional Qdrant payload filters to narrow retrieval scope.

**Response**: `text/event-stream` (SSE). Events in order:

| Event type | Payload | Notes |
|---|---|---|
| `assistant_message_start` | `{ "content": "", "session_id": "<uuid>" }` | Always first; carries new `session_id` |
| `content` | `{ "content": "إنما..." }` | One event per token chunk |
| `assistant_message_complete` | `{ "data": { "message_type": "assistant", "content": "...", "citations": [...] } }` | Full answer + citations |
| `thread_rename` | `{ "title": "أول حديث في البخاري" }` | Auto-generated short title |
| `stream_end` | — | Stream closed |

Citation object:
```json
{
  "resource_id": "<hadith_id>",
  "text_span": "إنما الأعمال...",
  "confidence": 0.92,
  "title": "حديث النية",
  "hadith_url": "https://..."
}
```

Returns `401` if unauthenticated, `429` if daily quota exceeded, `403` if session belongs to another user.

#### `GET /api/v2/chat/sessions`

Query params: `skip`, `limit`

**Response** (metadata only, no messages):
```json
[
  { "session_id": "<uuid>", "title": "أول حديث", "created_at": "...", "message_count": 4 }
]
```

#### `GET /api/v2/chat/sessions/{session_id}`

**Response** (full history):
```json
{
  "session_id": "<uuid>",
  "user_id": "<uuid>",
  "title": "أول حديث",
  "created_at": "...",
  "messages": [
    { "role": "user", "content": "...", "citations": [], "timestamp": "..." },
    { "role": "assistant", "content": "...", "citations": [...], "timestamp": "..." }
  ]
}
```

#### `DELETE /api/v2/chat/sessions/{session_id}`

No body. **Response** `204`.

### Chatbot — user quota (daily limits)

`POST /api/v2/chat` enforces per-user daily request limits based on user tier:

| Tier | Daily limit | Notes |
|------|-------------|-------|
| `free` | 3 requests | Default for all new users |
| `supporter` | 10 requests | Set manually after donation |
| `unlimited` | no limit | Admins / special accounts |

When the limit is reached, the endpoint returns **HTTP 429** before invoking the LLM (no cost incurred):

```json
{
  "detail": {
    "ar": "لقد وصلت إلى الحد اليومي. ادعم المشروع للحصول على المزيد.",
    "limit": 3,
    "used": 4,
    "upgrade_hint": "supporter"
  }
}
```

Counters are stored in the `user_quotas` MongoDB collection and expire automatically after 2 days. Limits are configurable via env vars:

```bash
QUOTA_FREE_DAILY=3
QUOTA_SUPPORTER_DAILY=10
QUOTA_UNLIMITED_DAILY=-1   # -1 = no limit
```

Check current usage via `GET /api/v2/chat/quota` (auth required):

```json
{ "tier": "free", "limit": 3, "used": 2, "remaining": 1, "resets_at": "2026-04-28T00:00:00Z" }
```

Call this once on page load and after each chat turn to show a live usage indicator without waiting for a `429`.

To upgrade a user's tier (after a donation), set `tier: "supporter"` directly on their document in `auth_users`:

```bash
# Dev
docker exec -it hadathana-mongo-dev mongosh HadithDataDev
db.auth_users.updateOne({ email: "mahmoud2abdalfattah@gmail.com" }, { $set: { tier: "supporter" } })

# Prod
docker exec -it hadathana-mongo-prod mongosh HadithData
db.auth_users.updateOne({ email: "mahmoud2abdalfattah@gmail.com" }, { $set: { tier: "supporter" } })
```

---

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

### Chatbot — authenticated session endpoints

All chatbot endpoints require a valid session cookie (login first via `POST /auth/login`).

```bash
# New session (server generates session_id):
curl -b cookies.txt -X POST http://localhost:8001/api/v2/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"ما حكم النية في الصلاة؟"}' --no-buffer

# Resume session (pass session_id from first assistant_message_start event):
curl -b cookies.txt -X POST http://localhost:8001/api/v2/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"وضح أكثر","session_id":"<uuid>"}' --no-buffer

# List my sessions (metadata, no messages):
curl -b cookies.txt http://localhost:8001/api/v2/chat/sessions

# Get full session with message history:
curl -b cookies.txt http://localhost:8001/api/v2/chat/sessions/<session_id>

# Delete a session:
curl -b cookies.txt -X DELETE http://localhost:8001/api/v2/chat/sessions/<session_id>
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

# Admin stats (superuser only)
curl -b cookies.txt http://localhost:8000/api/v2/admin/stats | python3 -m json.tool

# Upgrade user tier (superuser only — replace <user_id> with the UUID from auth_users.id)
curl -b cookies.txt -X PATCH http://localhost:8000/api/v2/admin/users/<user_id>/tier \
  -H "Content-Type: application/json" \
  -d '{"tier": "supporter"}'

# Al-Rawi chatbot (SSE stream — requires auth cookie)
curl -b cookies.txt -s -X POST http://localhost:8000/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "ما حكم الصلاة في وقتها؟"}' --no-buffer

# Session management
curl -b cookies.txt http://localhost:8000/api/v2/chat/sessions
curl -b cookies.txt http://localhost:8000/api/v2/chat/sessions/<session_id>
curl -b cookies.txt -X DELETE http://localhost:8000/api/v2/chat/sessions/<session_id>
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
