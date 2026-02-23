# Plan A: Backend Repo (`hadathana-api`)

## Context

The MongoDB URI currently lives in the frontend `.env`, which is a security risk. The backend needs to be extracted into a separate FastAPI repo that connects to MongoDB Atlas and exposes the REST API the frontend consumes.

MongoDB collections:
- `bukhari_book` — hadiths with multi-chain narrator structure
- `narrators` — full narrator profiles (jarh_wa_tadil embedded)
- `hadith_pages` — book/page metadata

---

## Stack

- **FastAPI** (Python)
- **Motor** (async MongoDB driver)
- **Pydantic v2** (models + serialization)
- **python-dotenv** (env config)
- **uvicorn** (ASGI server)

---

## Directory Structure

```
hadathana-api/
├── app/
│   ├── main.py              # FastAPI app, CORS, router mounts
│   ├── config.py            # Settings from env vars
│   ├── database.py          # Motor client + collection accessors
│   ├── models/
│   │   ├── hadith.py        # Pydantic models for bukhari_book
│   │   └── narrator.py      # Pydantic models for narrators
│   └── routers/
│       ├── hadiths.py       # GET /hadiths, GET /hadiths/{id}
│       └── narrators.py     # GET /narrators, GET /narrators/{id}
├── .env                     # MONGODB_URI, DB_NAME (gitignored)
├── .env.example             # Safe template to commit
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Environment Variables

```bash
# .env (gitignored — never commit)
MONGODB_URI=mongodb+srv://...
DB_NAME=HadithData
```

```bash
# .env.example (committed)
MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
DB_NAME=HadithData
```

---

## Pydantic Models

### `app/models/hadith.py`

```python
from pydantic import BaseModel, Field
from typing import Literal

class ChainNarrator(BaseModel):
    name: str
    role: Literal["narrator", "lead"]
    narrator_id: int

class Chain(BaseModel):
    chain_id: str
    type: Literal["primary", "nested"]
    narrators: list[ChainNarrator]

class UniqueNarrator(BaseModel):
    name: str
    narrator_id: int

class Hadith(BaseModel):
    id: str                          # MongoDB _id serialized as string
    hadith_index: int
    source: str
    hadith: str                      # full formatted text
    hadith_plain: str                # plain text (used for search)
    matn_plain: list[str]
    n_matn: int
    n_chains: int
    chains: list[Chain]
    unique_narrators: list[UniqueNarrator]

class PaginatedHadiths(BaseModel):
    items: list[Hadith]
    total: int
```

### `app/models/narrator.py`

```python
class JarhWaTadil(BaseModel):
    scholar: str
    quotes: list[str]

class Narrator(BaseModel):
    id: str
    narrator_id: int
    name: str
    name_plain: str
    kunya: str
    nasab: str
    death_date: str
    tabaqa: str
    rank_ibn_hajar: str
    rank_dhahabi: str
    relations: str
    jarh_wa_tadil: list[JarhWaTadil]

class PaginatedNarrators(BaseModel):
    items: list[Narrator]
    total: int
```

---

## API Endpoints

### Hadiths — `app/routers/hadiths.py`

```
GET /api/v1/hadiths
  Query params:
    hadith_plain: str       → $regex case-insensitive on hadith_plain field
    narrator_id: int        → match unique_narrators.narrator_id
    chain_type: str         → match chains[].type ("primary" | "nested")
    skip: int = 0
    limit: int = 20 (max 100)
  Response: PaginatedHadiths

GET /api/v1/hadiths/{hadith_id}    # MongoDB ObjectId string
  Response: Hadith
  Error 404: { "detail": "Hadith not found." }
```

MongoDB query pattern:
```python
filter = {}
if hadith_plain:
    filter["hadith_plain"] = {"$regex": hadith_plain, "$options": "i"}
if narrator_id:
    filter["unique_narrators.narrator_id"] = narrator_id
if chain_type:
    filter["chains.type"] = chain_type

cursor = collection.find(filter).skip(skip).limit(limit)
total = await collection.count_documents(filter)
```

### Narrators — `app/routers/narrators.py`

```
GET /api/v1/narrators
  Query params:
    name_plain: str    → $regex case-insensitive
    kunya: str         → $regex case-insensitive
    nasab: str         → $regex case-insensitive
    skip: int = 0
    limit: int = 20
  Response: PaginatedNarrators

GET /api/v1/narrators/{narrator_id}    # MongoDB ObjectId string
  Response: Narrator
  Error 404: { "detail": "Narrator not found." }
```

---

## CORS Configuration (`app/main.py`)

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # frontend dev URL
    allow_methods=["GET"],
    allow_headers=["*"],
)
```

---

## `requirements.txt`

```
fastapi>=0.110.0
motor>=3.3.2
pydantic>=2.0.0
pydantic-settings>=2.0.0
python-dotenv>=1.0.0
uvicorn[standard]>=0.27.0
```

---

## `README.md` Content

```markdown
# hadathana-api

FastAPI backend for the Hadathana Islamic app.

## Setup

1. Clone repo and create venv:
   python -m venv venv && source venv/bin/activate

2. Install dependencies:
   pip install -r requirements.txt

3. Copy env template and fill in credentials:
   cp .env.example .env

4. Run dev server:
   uvicorn app.main:app --reload

API docs available at: http://localhost:8000/docs
```

---

## Security Notes

- `MONGODB_URI` lives ONLY in this backend repo's `.env` (gitignored)
- Remove `MONGODB_URI` from the frontend `.env` / `.env.local`
- Frontend only needs: `NEXT_PUBLIC_API_BASE=http://localhost:8000/api/v1`

---

## Verification

- `uvicorn app.main:app --reload` starts without errors
- `http://localhost:8000/docs` shows Swagger UI with all endpoints
- `GET /api/v1/hadiths?hadith_plain=نام` returns `{ items: [...], total: N }`
- `GET /api/v1/hadiths/{id}` returns full hadith with `chains[]` and `unique_narrators[]`
- `GET /api/v1/narrators?name_plain=مالك` returns narrator list
- `GET /api/v1/narrators/{id}` returns narrator with `jarh_wa_tadil[]`
