# Hadathana API

FastAPI + MongoDB backend for the Hadathana Islamic hadith app. Exposes Sahih al-Bukhari data from two independent pipelines (Shamela and Podia) with narrator chain analysis.

---

## Quick Start

```bash
# 1. Copy env template and fill in credentials
cp .env.example .env

# 2. Start the dev stack (MongoDB + bootstrap + API on port 8001)
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
| Start command | `make dev` | `make prod` |

---

## Docker Compose Commands

```bash
make dev          # start dev stack (port 8001)
make prod         # build image + start prod stack (port 8000)
make dev-logs     # follow dev API logs
make prod-logs    # follow prod API logs
make dev-down     # stop dev stack
make prod-down    # stop prod stack
make status       # show both stacks
make health       # health check both APIs
```

On first run, `mongo-init` auto-bootstraps the database from JSONL files. Subsequent starts skip bootstrap if data already exists (<1s).

---

## Feature Development Workflow

All development happens on the VPS. Dev (`:8001`) is your staging — prod (`:8000`) is only touched on promotion.

```
feat branch  →  dev :8001  →  validate  →  merge main  →  make prod :8000
```

### 1. Create a feature branch

```bash
git checkout main
git checkout -b feat_your_feature
```

### 2. Develop and test on dev

```bash
make dev          # already running? skip this
# edit code in app/ — live reload picks up changes instantly
# test at http://<vps-ip>:8001
```

### 3. Merge to main and promote to prod

```bash
git checkout main
git merge feat_your_feature
make prod         # rebuilds image from main, restarts prod on :8000
```

Prod rebuild takes ~20-30 seconds. Users see no downtime during the build; the old container keeps serving until the new one is ready.

### 4. Rollback if something breaks

```bash
git checkout <last-good-commit>
make prod         # rebuilds from rolled-back code
```

---

## Bootstrap (manual / recovery)

`mongo-init` handles this automatically on `make dev` / `make prod`. For manual recovery:

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"

# Bootstrap HadithDataDev (skips if data already exists)
"$PYTHON" scripts/bootstrap_local_db.py

# Bootstrap HadithData (prod DB)
"$PYTHON" scripts/bootstrap_local_db.py --db HadithData

# Force re-import
"$PYTHON" scripts/bootstrap_local_db.py --force
```

---

## Endpoints

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
| GET | `/health` | — | `{ "status": "ok" }` |

---

## Example Requests

```bash
# Health
curl http://localhost:8000/health

# v1 — Shamela
curl http://localhost:8000/api/v1/hadiths
curl "http://localhost:8000/api/v1/hadiths?hadith_plain=نام"
curl "http://localhost:8000/api/v1/hadiths?narrator_id=822"
curl http://localhost:8000/api/v1/narrators/822/stats

# v2 — Podia
curl http://localhost:8000/api/v2/hadiths
curl "http://localhost:8000/api/v2/hadiths?hadith_text_plain=الصلاة"
curl "http://localhost:8000/api/v2/hadiths?topic=الصلاة"
curl http://localhost:8000/api/v2/narrators/822/tarajem
curl http://localhost:8000/api/v2/narrators/822/stats
curl http://localhost:8000/api/v2/topics
curl "http://localhost:8000/api/v2/topics/الصلاة/hadiths"
```

---

## Data Field Comparison

### Hadith fields

| Field | v1 Shamela | v2 Podia | Notes |
|-------|:---:|:---:|-------|
| `hadith_index` / `hadith_indices` | ✅ single int | ✅ list | Podia hadiths can span multiple indices |
| `hadith` / `hadith_text` | ✅ | ✅ | Full text with tashkeel |
| `hadith_plain` / `hadith_text_plain` | ✅ | ✅ | Tashkeel stripped, for search |
| `book`, `chapter` | ❌ | ✅ | |
| `hadith_url` | ❌ | ✅ | Source URL |
| `sanad_text`, `matn_text`, `tawabi_text` | ❌ | ✅ | Segmented text |
| `topics[]` | ❌ | ✅ | Arabic semantic topic tags (LLM-generated) |
| `matn_embedding` | ❌ | ✅ | 1536-dim Cohere vector (for semantic search) |
| `chains[]` | ✅ | ✅ | Multi-chain with transmission types |
| `narrators[]` | ❌ | ✅ | Per-hadith narrator list |

### Narrator fields

| Field | v1 Shamela | v2 Podia |
|-------|:---:|:---:|
| `name` / `name_in_chain` | ✅ | ✅ |
| `full_name`, `full_name_plain` | ❌ | ✅ |
| `rank`, `rank_plain` | ❌ | ✅ |
| `kunya`, `nasab`, `tabaqa` | ✅ | ❌ |
| `rank_ibn_hajar`, `rank_dhahabi` | ✅ | ❌ |
| `jarh_wa_tadil[]` | ✅ | via `/tarajem` |

---

## Matn Embeddings & Topics

All 7,076 Podia hadiths are enriched with:
- **`matn_embedding`** — 1536-dim Cohere `embed-v4.0` vector (for semantic search / RAG)
- **`topics`** — 1–3 Arabic semantic tags generated by Gemini Flash via OpenRouter

### JSONL-first enrichment (preferred — no MongoDB dependency)

Two offline scripts write slim output files to the repo root, then import into MongoDB separately:

```bash
# Run in tmux enrichment session (topics first, then embeddings automatically)
tmux new-session -d -s enrichment
tmux send-keys -t enrichment 'cd ~/Projects/hadathanaBackendApp && \
  /home/abdo_kamar/Projects/.venv/bin/python scripts/tag_topics_jsonl.py && \
  /home/abdo_kamar/Projects/.venv/bin/python scripts/embed_matn_jsonl.py' Enter

# Smoke test (5 docs each)
python scripts/tag_topics_jsonl.py --limit 5
python scripts/embed_matn_jsonl.py --limit 5

# Dry run (estimate API calls)
python scripts/tag_topics_jsonl.py --dry-run
python scripts/embed_matn_jsonl.py --dry-run
```

Output files (repo root, gitignored):
- `hadith_topics.jsonl` — `hadith_url` + `topics` → import to MongoDB
- `hadith_embeddings.jsonl` — `hadith_url` + `matn_embedding` → Qdrant / vector DB (deferred)

Both scripts are **resumable** — re-running skips already-processed `hadith_url`s.

Import topics into MongoDB after the script completes (use the `$set` script — **do not** use `mongoimport --mode=upsert` as it replaces entire documents):
```bash
# Apply topics as $set updates (merges topics field, preserves all other hadith data)
python scripts/apply_topics.py --db HadithDataDev
```

Or run inline:
```python
# scripts/apply_topics.py equivalent — inline version
import json
from pymongo import MongoClient, UpdateOne
col = MongoClient("mongodb://localhost:27017/")["HadithDataDev"]["processed_podia_books"]
ops = [UpdateOne({"hadith_url": d["hadith_url"]}, {"$set": {"topics": d.get("topics", [])}})
       for d in (json.loads(l) for l in open("hadith_topics.jsonl"))]
result = col.bulk_write(ops, ordered=False)
print(f"Modified: {result.modified_count}")
```

Requires `COHERE_API_KEY` and `OPENROUTER_API_KEY` in `.env`.

### Legacy (Atlas-dependent)

```bash
python scripts/embed_matn.py              # full run — requires Atlas write access
python scripts/embed_matn.py --dry-run    # estimate cost only
python scripts/embed_matn.py --limit 5    # smoke test on 5 docs
```

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

# Embeddings & topic tagging
COHERE_API_KEY=
OPENROUTER_API_KEY=

# Cloudflare R2 (data snapshots)
R2_ENDPOINT_URL=
R2_BUCKET=hadathana-data
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
```

---

## Data Snapshots (Cloudflare R2)

All JSONL data files are stored in R2, not git. **Always check R2 for the latest snapshot before starting any data work.**

```bash
# See what snapshots exist
python scripts/r2_sync/list_snapshots.py

# Pull latest snapshot after cloning or before data work
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_podia --latest
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_shamela --latest
```

Full docs: [scripts/r2_sync/README.md](scripts/r2_sync/README.md)

---

## Data Update Workflow

When enriching or modifying data (e.g. adding topics, embeddings, new fields):

**1. Write/update the script**
- New enrichment → new file under `scripts/` (e.g. `scripts/embed_matn.py`)
- Schema change → update the relevant preprocessing script and `app/models/`

**2. Pull latest data from R2**
```bash
python scripts/r2_sync/pull_snapshot.py --dataset bukhari_podia --latest
```

**3. Run the script against `HadithDataDev` first**
```bash
# Make sure APP_ENV=dev in .env (points to local Docker + HadithDataDev)
python scripts/your_script.py
```

**4. Verify in dev, test the API**
```bash
# Confirm data looks right
curl "http://localhost:8001/api/v2/hadiths?limit=3"
# Run tests
python -m pytest tests/ -v
```

**5. Push enriched JSONL to R2**
```bash
python scripts/r2_sync/push_snapshot.py --dataset bukhari_podia \
  --source mongo_migration/processed_bukhari_podia/ --extensions jsonl
```

**6. Promote to prod**
```bash
# mongodump from dev, mongorestore to prod
docker exec hadathana-mongo-dev mongodump --db HadithDataDev --out /tmp/dump
docker exec hadathana-mongo-prod mongorestore --db HadithData /tmp/dump/HadithDataDev
```

**7. Restart prod API**
```bash
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
