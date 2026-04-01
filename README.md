# Hadathana API

FastAPI + MongoDB backend for the Hadathana Islamic hadith app. Exposes Sahih al-Bukhari data from two independent pipelines (Shamela and Podia) with narrator chain analysis.

---

## Quick Start

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"

# 1. Copy env template and fill in credentials
cp .env.example .env

# 2. Start local MongoDB (Docker)
docker start mongodb-hadathana   # or: docker run -d --name mongodb-hadathana -p 27017:27017 -v mongodb_hadathana_data:/data/db mongo:8

# 3. Run dev server (APP_ENV=dev in .env)
"$PYTHON" -m uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

API docs: http://localhost:8001/docs

---

## Environments

Two environments controlled by `APP_ENV` in `.env`:

| `APP_ENV` | MongoDB | Database | Port | CORS |
|---|---|---|---|---|
| `dev` | Local Docker (`mongodb://localhost:27017/`) | `HadithDataDev` | 8001 | localhost |
| `prod` | Atlas cloud (`MONGODB_URI_READ`) | `HadithData` | 8000 | hadathana.app |

**Production runs in `tmux hadathana_deployment`** on port 8000. Never restart it without verifying `APP_ENV=prod`.

---

## Setup — Local MongoDB

```bash
# Start container
docker start mongodb-hadathana

# Bootstrap a database (run for both HadithData and HadithDataDev)
DB=HadithDataDev
docker exec -i mongodb-hadathana mongoimport --db $DB --collection processed_podia_books \
  < mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl
docker exec -i mongodb-hadathana mongoimport --db $DB --collection processed_podia_narrators \
  < mongo_migration/processed_bukhari_podia/bukhari_podia_narrators.jsonl
docker exec -i mongodb-hadathana mongoimport --db $DB --collection processed_podia_narrator_biographies \
  < mongo_migration/processed_bukhari_podia/bukhari_narrators_tarajem.jsonl
docker exec -i mongodb-hadathana mongoimport --db $DB --collection raw_shamela_books \
  < mongo_migration/processed_bukhari_shamela/preprocessed_bukhari.jsonl
docker exec -i mongodb-hadathana mongoimport --db $DB --collection raw_shamela_narrators \
  < mongo_migration/processed_bukhari_shamela/narrators.jsonl

# Create indexes
MONGODB_URI_READ_WRITE=mongodb://localhost:27017/ DB_NAME=$DB \
  python mongo_migration/create_indexes.py

# Compute narrator stats (not in JSONL — must be derived)
MONGODB_URI_READ_WRITE=mongodb://localhost:27017/ DB_NAME=$DB \
  python mongo_migration/processed_bukhari_podia/compute_stats.py
```

---

## Populate Atlas (Production)

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"

# Shamela pipeline
"$PYTHON" mongo_migration/processed_bukhari_shamela/preprocess_pages.py
"$PYTHON" mongo_migration/processed_bukhari_shamela/preprocess_hadiths.py
"$PYTHON" mongo_migration/upload.py
"$PYTHON" mongo_migration/create_indexes.py
"$PYTHON" mongo_migration/processed_bukhari_shamela/compute_stats.py

# Podia pipeline
"$PYTHON" mongo_migration/processed_bukhari_podia/preprocess.py
"$PYTHON" mongo_migration/upload.py
"$PYTHON" mongo_migration/create_indexes.py
"$PYTHON" mongo_migration/processed_bukhari_podia/compute_stats.py
```

Collections populated:
- **Shamela**: `raw_shamela_books`, `raw_shamela_narrators`, `raw_shamela_hadith_pages`, `analytics_narrator_stats_shamela`
- **Podia**: `processed_podia_books`, `raw_podia_books`, `processed_podia_narrators`, `processed_podia_narrator_biographies`, `analytics_narrator_stats_podia`

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

Import topics into MongoDB after the script completes:
```bash
docker exec -i mongodb-hadathana mongoimport --db HadithDataDev \
  --collection processed_podia_books --mode=upsert --upsertFields=hadith_url \
  < hadith_topics.jsonl
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

**6. Promote to prod (Atlas)**
```bash
# Option A — re-run upload against Atlas
MONGODB_URI_READ_WRITE=<atlas_uri> DB_NAME=HadithData python mongo_migration/upload.py

# Option B — mongodump from local dev, mongorestore to Atlas
mongodump --uri mongodb://localhost:27017/ --db HadithDataDev --out /tmp/dump
mongorestore --uri <atlas_uri> --db HadithData /tmp/dump/HadithDataDev
```

**7. Restart prod**
```bash
# In tmux hadathana_deployment — make sure APP_ENV=prod in .env
# Ctrl+C, then:
/home/abdo_kamar/Projects/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## Deploy to Railway / Render

1. Push repo to GitHub
2. Create project on Railway or Render, connect the repo
3. Set env vars in the platform dashboard (see `.env.example`)
4. In MongoDB Atlas → **Network Access** → allow `0.0.0.0/0`
5. Platform auto-detects `Dockerfile` and deploys

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
