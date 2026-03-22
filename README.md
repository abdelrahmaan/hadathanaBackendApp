# Hadith Narrator Graph

A knowledge graph of Sahih Al-Bukhari narrator chains built from shamela.ws data, queryable via Neo4j. Designed to power a natural-language chatbot that converts user questions into Cypher queries.

## Quick Start

```bash
# 1. Activate virtualenv
source backend/venv/bin/activate

# 2. Start Neo4j
docker start neo4j-hadith

# 3. Build the graph
python extract_data_v2/build_graph.py

# 4. Verify in Neo4j Browser
# Open http://localhost:7474
```

---

## Data Pipeline (Current — V3 Shamela)

Raw data is scraped directly from shamela.ws. No LLM extraction needed — narrator IDs come from shamela itself.

```
shamela_book_1681.jsonl          shamela_narrators.jsonl     narrator_hadith_names.json
(7,230 hadiths + chains)    +    (1,527 narrator bios)   +   (1,525 name variant lists)
         │                                 │                            │
         └─────────────────────────────────┴────────────────────────────┘
                                           │
                                   build_graph.py
                                           │
                                    Neo4j Graph (V3)
                     ┌──────────────────────────────────────────┐
                     │  Book → Chapter → Hadith                 │
                     │  Narrator → Narrator → Hadith            │
                     └──────────────────────────────────────────┘
```

### Run ingestion

```bash
source backend/venv/bin/activate

# Dry run (no writes, prints stats)
python extract_data_v2/build_graph.py --dry-run

# Full ingestion (reads credentials from .env)
python extract_data_v2/build_graph.py
```

Credentials are loaded from `.env` automatically (`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`).

### Re-ingest from scratch

```bash
# Clear the database first in Neo4j Browser:
# MATCH (n) DETACH DELETE n;
# Then re-run:
python extract_data_v2/build_graph.py
```

---

## Graph Schema (V3)

### Nodes

| Label | Key | Main Properties |
|---|---|---|
| `Book` | `section_id` | `book_id`, `name` |
| `Chapter` | `section_id` | `book_id`, `name` |
| `Hadith` | `hadith_id` (`"1681_{page}"`) | `page_number`, `book_id`, `full_text`, `matn` |
| `Narrator` | `narrator_id` (shamela int) | `name`, `kunya`, `nasab`, `tabaqa`, `rank_ibn_hajar`, `rank_dhahabi`, `death_date`, `original_names[]` |

### Relationships

| Type | Direction | Properties |
|---|---|---|
| `IN_CHAPTER` | Hadith → Chapter | — |
| `IN_BOOK` | Chapter → Book | — |
| `NARRATED` | Narrator → Narrator | `position`, `hadith_id` |
| `TRANSMITTED_HADITH` | Narrator → Hadith | `position` |

**Chain convention:** `narrators[0]` is the collector (البخاري), `narrators[-1]` is closest to the Prophet ﷺ. `NARRATED` links go left→right. The last narrator has a `TRANSMITTED_HADITH` edge to the Hadith node.

Full schema for chatbot integration: [extract_data_v2/schema_description.md](extract_data_v2/schema_description.md)

---

## Querying

Open Neo4j Browser at [http://localhost:7474](http://localhost:7474) (`neo4j` / `password`).

See [queries.cypher](queries.cypher) for the full query library. Quick examples:

```cypher
// Node counts
MATCH (b:Book) RETURN count(b);
MATCH (c:Chapter) RETURN count(c);
MATCH (h:Hadith) RETURN count(h);
MATCH (n:Narrator) RETURN count(n);

// Full chain for a hadith
MATCH (n:Narrator)-[:NARRATED*]->(last:Narrator)-[:TRANSMITTED_HADITH]->(h:Hadith {hadith_id:'1681_11'})
RETURN n.name, last.name, h.matn;

// Most frequent narrators
MATCH (n:Narrator)-[:NARRATED|TRANSMITTED_HADITH]->()
RETURN n.name, count(*) AS freq ORDER BY freq DESC LIMIT 10;
```

---

## Docker

### Run the API locally with Docker

```bash
# Build the image
docker build -t hadathna-api .

# Run (pass env vars at runtime, never bake them into the image)
docker run -p 8000:8000 \
  -e MONGODB_URI_READ="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/" \
  -e DB_NAME="HadithData" \
  -e CORS_ORIGINS="*" \
  hadathna-api
```

API available at http://localhost:8000 — health check: http://localhost:8000/health

### Neo4j container (legacy graph pipeline only)

```bash
# Create container (first time)
docker run -d \
  --name neo4j-hadith \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:latest

# Daily use
docker start neo4j-hadith
docker stop neo4j-hadith
docker logs neo4j-hadith
```

---

## Web Application

The project includes a Next.js frontend and FastAPI backend.

```bash
# Terminal 1 — Neo4j
docker start neo4j-hadith

# Terminal 2 — FastAPI backend
cd backend
source venv/bin/activate
uvicorn main:app --reload --port 8000

# Terminal 3 — Next.js frontend
cd frontend
npm run dev
```

Frontend: [http://localhost:3000](http://localhost:3000) — Backend: [http://localhost:8000](http://localhost:8000)

---

## File Structure

```
hadith_graph/
├── extract_data_v2/
│   ├── build_graph.py                  # ← V3 ingestion script (current)
│   ├── schema_description.md           # ← Schema for chatbot system prompt
│   ├── firecrawl/
│   │   ├── shamela_book_1681.jsonl     # 7,230 hadiths (raw scrape)
│   │   ├── shamela_narrators.jsonl     # 1,527 narrator biographies
│   │   ├── narrator_hadith_names.json       # 1,525 narrator → name variants
│   │   ├── enrich_narrator_ids.py          # Phase 1: exact name match → narrator IDs (68% coverage)
│   │   ├── resolve_remaining_narrators.py  # Phase 2: Shamela + context rules → 88% coverage
│   │   ├── bukhari_narrator_coverage.py    # generates per-narrator bio report for Bukhari V2
│   │   └── bukhari_narrator_coverage.jsonl # output: 1,305 narrators with bio fields
│   ├── Bukhari/
│   │   ├── Bukhari_Without_Tashkel_results_advanced_with_matn.json        # V2 LLM-extracted hadith chains (no tashkeel)
│   │   ├── Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json  # ← enriched with narrator IDs
│   │   ├── narrator_hadith_names_bukhari.json  # narrator_id → name variants seen in Bukhari chains (1,366 IDs)
│   │   ├── covered_narrators.csv               # 1,366 resolved narrators: ID, canonical name, name forms, role, method, sample hadith indices
│   │   └── uncovered_narrators.csv             # 699 unresolved narrator names: name, role, occurrences, sample hadith indices
│
├── backend/
│   └── main.py                         # FastAPI server
├── frontend/                           # Next.js app
│
├── queries.cypher                      # Cypher query examples (V3 schema)
├── requirements.txt                    # Python dependencies
└── .env                                # Credentials (gitignored)
```

**Legacy scripts** (V1/V2 LLM-based pipeline — kept for reference):
`langExtract.py`, `ingest.py`, `extract_chains.py`, `normalization.py`, `neo4j_client.py`

---

## Environment Variables (.env)

```bash
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
```

---

## hadathana-api (FastAPI + MongoDB)

FastAPI backend for the Hadathana Islamic app — exposes the REST API consumed by the frontend.

### Setup

```bash
# 1. Create and activate venv
python -m venv venv && source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy env template and fill in credentials
cp .env.example .env

# 4. Run dev server
uvicorn app.main:app --reload
```

API docs: http://localhost:8000/docs

### Deploy to Railway / Render

1. Push the repo to GitHub
2. Create a new project on [Railway](https://railway.app) or [Render](https://render.com) and connect the repo
3. Set environment variables in the platform dashboard:
   ```
   MONGODB_URI_READ=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
   DB_NAME=HadithData
   CORS_ORIGINS=*
   ```
4. In MongoDB Atlas → **Network Access** → allow `0.0.0.0/0` (or the platform's IP range)
5. The platform will auto-detect the `Dockerfile` and deploy

### Populate the database

Before the API returns data, run the migration pipeline:

```bash
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"

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

This populates the following collections:

**Shamela**: `raw_shamela_books`, `raw_shamela_narrators`, `raw_shamela_hadith_pages`, `analytics_narrator_stats_shamela`
**Podia**: `processed_podia_books`, `raw_podia_books`, `processed_podia_narrators`, `processed_podia_narrator_biographies`, `analytics_narrator_stats_podia`

`compute_narrator_stats.py` is idempotent — re-run it any time after new hadiths are added.

### Endpoints

#### v1 — Shamela

| Method | Path | Query params | Response |
|--------|------|-------------|----------|
| GET | `/api/v1/hadiths` | `hadith_plain`, `narrator_id`, `chain_type`, `skip`, `limit` | `PaginatedHadiths` |
| GET | `/api/v1/hadiths/{hadith_index}` | — | `Hadith` |
| GET | `/api/v1/narrators` | `name_plain`, `kunya`, `nasab`, `skip`, `limit` | `PaginatedNarrators` |
| GET | `/api/v1/narrators/{narrator_id}` | — | `Narrator` |
| GET | `/api/v1/narrators/{narrator_id}/stats` | — | `NarratorStats` |
| GET | `/health` | — | `{ "status": "ok" }` |

#### v2 — Podia

| Method | Path | Query params | Response |
|--------|------|-------------|----------|
| GET | `/api/v2/hadiths` | `hadith_text_plain`, `rawi_id`, `book`, `hadith_index`, `skip`, `limit` | `PaginatedPodiaHadiths` |
| GET | `/api/v2/hadiths/{hadith_index}` | — | `PodiaHadith` |
| GET | `/api/v2/narrators` | `full_name_plain`, `rank`, `skip`, `limit` | `PaginatedPodiaNarrators` |
| GET | `/api/v2/narrators/{rawi_id}` | — | `PodiaNarrator` |
| GET | `/api/v2/narrators/{rawi_id}/tarajem` | — | `PodiaNarratorTarajem` |
| GET | `/api/v2/narrators/{rawi_id}/stats` | — | `PodiaNarratorStats` |

---

### Data Field Comparison

#### Hadith fields

| Field | v1 Shamela | v2 Podia | Notes |
|-------|:---:|:---:|-------|
| `id` | ✅ | ✅ | MongoDB ObjectId as string |
| `hadith_index` / `hadith_indices` | ✅ single int | ✅ list of ints | Podia hadiths can span multiple indices |
| `source` | ✅ | ✅ | |
| `hadith` / `hadith_text` | ✅ | ✅ | Full text with tashkeel |
| `hadith_plain` / `hadith_text_plain` | ✅ | ✅ | Tashkeel stripped, for search |
| `book` | ❌ | ✅ | |
| `chapter` | ❌ | ✅ | |
| `hadith_url` | ❌ | ✅ | Source URL |
| `matn_plain` | ✅ list | ❌ | Matn segments (Shamela only) |
| `n_matn` | ✅ | ❌ | Count of matn segments |
| `n_chains` | ✅ | ❌ | Count of chains |
| `chains[]` | ✅ | ❌ | Typed chain structure (primary/nested/follow_up) |
| `unique_narrators[]` | ✅ | ❌ | Deduped narrator list |
| `narrators[]` | ❌ | ✅ | Per-hadith narrator list with rank |

#### Narrator fields

| Field | v1 Shamela | v2 Podia | Notes |
|-------|:---:|:---:|-------|
| `id` | ✅ | ✅ | MongoDB ObjectId as string |
| `narrator_id` / `rawi_id` | ✅ | ✅ | Integer narrator ID |
| `name` / `name_in_chain` | ✅ | ✅ | Name as it appears in chain |
| `name_plain` / `name_in_chain_plain` | ✅ | ✅ | Tashkeel stripped |
| `full_name` | ❌ | ✅ | |
| `full_name_plain` | ❌ | ✅ | |
| `rank` | ❌ | ✅ | |
| `rank_plain` | ❌ | ✅ | |
| `full_tooltip_info` | ❌ | ✅ | Raw tooltip text from source |
| `kunya` | ✅ | ❌ | |
| `nasab` | ✅ | ❌ | |
| `tabaqa` | ✅ | ❌ | Generation/layer |
| `rank_ibn_hajar` | ✅ | ❌ | Individual scholar ranking |
| `rank_dhahabi` | ✅ | ❌ | Individual scholar ranking |
| `relations` | ✅ | ❌ | |
| `jarh_wa_tadil[]` | ✅ | ❌ | Available in v2 via `/tarajem` |
| `death_date` | ✅ | ❌ | Available in v2 via `/tarajem` |

#### Narrator stats (same structure in both)

| Field | v1 Shamela | v2 Podia |
|-------|:---:|:---:|
| `narrator_id` / `rawi_id` | ✅ | ✅ |
| `hadith_count` | ✅ | ✅ |
| `teachers[]` (`narrator_id`, `name`, `freq`) | ✅ | ✅ |
| `students[]` (`narrator_id`, `name`, `freq`) | ✅ | ✅ |

#### v2-only: `/tarajem` endpoint (narrator biography)

| Field | Notes |
|-------|-------|
| `rawi_id` | Narrator ID |
| `url` | Source URL |
| `name_in_chain`, `name_in_chain_plain` | Name as in chain |
| `full_name`, `full_name_plain` | Full canonical name |
| `rank`, `rank_plain` | |
| `narrator_info[]` | `action` + `text` + `text_plain` — structured biography entries |
| `tarajim[]` | `source` + `tarjama` + `tarjama_plain` — scholar biography texts |

---

### Advanced Extraction Data (Neo4j only)

`extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json` contains richer chain data for **7,076 hadiths** extracted by LLM. This data goes to **Neo4j**, not MongoDB.

| Field | Present | Notes |
|-------|:---:|-------|
| `hadith_indices` | ✅ | List of ints |
| `hadith_url` | ✅ | |
| `book_name`, `chapter` | ✅ | |
| `hadith_text`, `hadith_text_clean` | ✅ | Raw + cleaned |
| `sanad_text` | ✅ | Chain text only |
| `matn_text` | ✅ | Matn text only |
| `tawabi_text` | ✅ | Follow-up text |
| `chains[]` | ✅ | Multi-chain support (1,614 hadiths have >1 chain) |
| `chains[].type` | ✅ | `primary` / `nested` / `follow_up` |
| `chains[].narrators[].rawi_id` | ✅ | |
| `chains[].narrators[].name` | ✅ | |
| `chains[].narrators[].role` | ✅ | `narrator` / `lead` |
| `chains[].narrators[].transmission` | ✅ | Arabic transmission word (e.g. حدثنا) |
| `chains[].narrators[].transmission_type` | ✅ | `samaa` / `ambiguous` / `anana` / `ijaza_or_munawala` / `mukataba` / `samaa_or_ard` / `unknown` |
| `chains[].narrators[].is_explicit_hearing` | ✅ | Boolean |
| `ground_truth_match` | ✅ | LLM validation flag |
| `model_used` | ✅ | LLM model used for extraction |
| `route_reason` | ✅ | Why this route was chosen (e.g. `length_threshold`) |

---

#### Example requests

```bash
# v1 — Shamela
curl http://localhost:8000/api/v1/hadiths
curl "http://localhost:8000/api/v1/hadiths?hadith_plain=نام"
curl "http://localhost:8000/api/v1/hadiths?narrator_id=822"
curl http://localhost:8000/api/v1/hadiths/1
curl http://localhost:8000/api/v1/narrators
curl "http://localhost:8000/api/v1/narrators?name_plain=مالك"
curl http://localhost:8000/api/v1/narrators/822
curl http://localhost:8000/api/v1/narrators/822/stats

# v2 — Podia
curl http://localhost:8000/api/v2/hadiths
curl "http://localhost:8000/api/v2/hadiths?hadith_text_plain=نام"
curl "http://localhost:8000/api/v2/hadiths?rawi_id=822"
curl http://localhost:8000/api/v2/hadiths/1
curl http://localhost:8000/api/v2/narrators
curl "http://localhost:8000/api/v2/narrators?full_name_plain=مالك"
curl http://localhost:8000/api/v2/narrators/822
curl http://localhost:8000/api/v2/narrators/822/tarajem
curl http://localhost:8000/api/v2/narrators/822/stats
```

#### Response shapes

```jsonc
// v1 PaginatedHadiths
{ "items": [ { "id": "...", "hadith_index": 1, "source": "bukhari",
               "hadith": "...", "hadith_plain": "...", "matn_plain": [...],
               "n_matn": 1, "n_chains": 2,
               "chains": [{ "chain_id": "...", "type": "primary",
                            "narrators": [{ "name": "...", "role": "narrator", "narrator_id": 1 }] }],
               "unique_narrators": [{ "name": "...", "narrator_id": 1 }] }],
  "total": 7230 }

// v1 PaginatedNarrators
{ "items": [ { "id": "...", "narrator_id": 1, "name": "...", "name_plain": "...",
               "kunya": "...", "nasab": "...", "death_date": "...", "tabaqa": "...",
               "rank_ibn_hajar": "...", "rank_dhahabi": "...", "relations": "...",
               "jarh_wa_tadil": [{ "scholar": "...", "quotes": ["..."] }] }],
  "total": 1527 }

// v1 NarratorStats
{ "narrator_id": 822, "hadith_count": 195,
  "teachers": [{ "narrator_id": 3320, "name": "أبو هريرة", "freq": 38 }],
  "students": [{ "narrator_id": 857,  "name": "أيوب",      "freq": 47 }] }

// v2 PaginatedPodiaHadiths
{ "items": [ { "id": "...", "hadith_url": "...", "hadith_indices": [1],
               "source": "bukhari", "book": "...", "chapter": "...",
               "hadith_text": "...", "hadith_text_plain": "...",
               "narrators": [{ "rawi_id": 1, "name_in_chain": "...",
                               "name_in_chain_plain": "...", "full_name": "...",
                               "rank": "...", "rank_plain": "..." }] }],
  "total": 7008 }

// v2 PaginatedPodiaNarrators
{ "items": [ { "id": "...", "rawi_id": 1, "name_in_chain": "...",
               "name_in_chain_plain": "...", "full_name": "...", "full_name_plain": "...",
               "rank": "...", "rank_plain": "...", "full_tooltip_info": "..." }],
  "total": 1600 }

// v2 PodiaNarratorTarajem
{ "id": "...", "rawi_id": 1, "url": "...",
  "name_in_chain": "...", "full_name": "...", "rank": "...",
  "narrator_info": [{ "action": "...", "text": "...", "text_plain": "..." }],
  "tarajim": [{ "source": "...", "tarjama": "...", "tarjama_plain": "..." }] }

// v2 PodiaNarratorStats
{ "rawi_id": 822, "hadith_count": 195,
  "teachers": [{ "rawi_id": 3320, "name": "أبو هريرة", "freq": 38 }],
  "students": [{ "rawi_id": 857,  "name": "أيوب",      "freq": 47 }] }
```

### Test results (2026-02-24)

| Endpoint | Status | Result |
|----------|--------|--------|
| `GET /api/v1/hadiths` | ✅ 200 | `total=7008`, 20 items per page |
| `GET /api/v1/hadiths?hadith_plain=نام` | ✅ 200 | `total=125` matching hadiths |
| `GET /api/v1/hadiths/{id}` | ✅ 200 | Full hadith with `chains[]` and `unique_narrators[]` |
| `GET /api/v1/narrators` | ✅ 200 | `total=1523`, 20 items per page |
| `GET /api/v1/narrators?name_plain=مالك` | ✅ 200 | `total=145` matching narrators |
| `GET /api/v1/narrators/{id}` | ✅ 200 | Full narrator with `jarh_wa_tadil[]` |
| `GET /api/v1/narrators/{id}/stats` | ✅ 200 | `hadith_count`, `teachers[]`, `students[]` with frequencies |

### Environment Variables

```bash
MONGODB_URI_READ=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
DB_NAME=HadithData

# Use * to allow all origins (public API).
# For restricted access, use comma-separated list:
# CORS_ORIGINS=https://hadathana.com,https://www.hadathana.com
CORS_ORIGINS=*
```

---

## Troubleshooting

**Auth error:** Check `docker inspect neo4j-hadith` for `NEO4J_AUTH` value — that's the real password.

**Connection refused:** `docker start neo4j-hadith` and wait ~15 seconds.

**Duplicate data:** Clear with `MATCH (n) DETACH DELETE n;` in Neo4j Browser, then re-run `build_graph.py`.
