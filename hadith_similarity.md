# Hadith Similarity Search — شجرة الأسانيد

## Context

The project has 7,230 Bukhari hadiths stored in Neo4j (with `matn_plain` field) and MongoDB Atlas (`hadith_pages` collection). Some hadiths share the same prophetic statement in different variations — for example, "إنما الأعمال بالنيات" appears ~5 times with different narrator chains. The goal is to detect these semantically similar hadiths using vector embeddings, expose them via a new API endpoint, and visualize their combined isnad chains as a شجرة الأسانيد in the frontend.

**Choices:** Cohere `embed-multilingual-v3` (1024-dim) for embeddings, MongoDB Atlas Vector Search for similarity queries, full API + frontend feature.

---

## Architecture

```
Neo4j (source of truth)
  └─ matn_plain for all hadiths
        │
        ▼
embed/generate_embeddings.py  (one-time offline script)
        │  Cohere embed-multilingual-v3
        ▼
MongoDB Atlas: hadith_embeddings collection
  └─ { hadith_id, matn_plain, embedding[1024], ... }
        │  Atlas Vector Search index
        ▼
FastAPI: GET /api/hadith/{source}/{id}/similar
  └─ vector search in Mongo + chain enrichment from Neo4j
        │
        ▼
Frontend: SimilarHadiths section + CombinedChainGraphView
```

---

## Step 1 — Create Embedding Script

**New file:** `embed/generate_embeddings.py`

- Pull all `(hadith_id, matn_plain, page_number, book_id)` from Neo4j using:
  ```cypher
  MATCH (h:Hadith) WHERE h.matn_plain IS NOT NULL AND h.matn_plain <> ''
  RETURN h.hadith_id, h.page_number, h.book_id, h.matn_plain
  ```
- Skip already-embedded IDs (resume support via `hadith_embeddings.find({}, {hadith_id:1})`)
- Call Cohere in batches of 96 (`input_type="search_document"`, `model="embed-multilingual-v3"`)
- Sleep 1.2s between batches to respect rate limits
- Retry (3x, exponential backoff) on `TooManyRequestsError`
- Upsert into `hadith_embeddings` using `UpdateOne(..., upsert=True)` bulk writes (batch 200)

**MongoDB document schema:**
```json
{
  "hadith_id": "1681_11",
  "book_id": 1681,
  "page_number": 11,
  "matn_plain": "إنما الأعمال بالنيات...",
  "matn_preview": "first 200 chars of matn_plain",
  "embedding": [1024 floats],
  "embedded_at": "ISODate",
  "model": "embed-multilingual-v3",
  "dimensions": 1024
}
```

**Update:** `requirements.txt` — add `cohere>=5.0.0`

---

## Step 2 — Atlas Vector Search Index (Manual Step)

In Atlas UI → `hadith_graph.hadith_embeddings` → Search Indexes → Create Search Index → JSON Editor:

```json
{
  "name": "hadith_embedding_index",
  "type": "vectorSearch",
  "fields": [
    { "type": "vector", "path": "embedding", "numDimensions": 1024, "similarity": "cosine" },
    { "type": "filter", "path": "book_id" },
    { "type": "filter", "path": "hadith_id" }
  ]
}
```

Wait for index status → **Active** before proceeding.

> **Why cosine?** Cohere `embed-multilingual-v3` produces normalized vectors. Cosine similarity correctly measures semantic closeness for Arabic text variations.

> **Why filter fields?** Allows the `$vectorSearch` aggregation to pre-filter by `book_id` or exclude a specific `hadith_id` (self-exclusion) at the index level, which is faster than post-filtering.

---

## Step 3 — FastAPI Endpoint

**Modify:** `backend/main.py`

### 3a. Add MongoClient at module level

```python
from pymongo import MongoClient

mongo_client = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=5000)
```

### 3b. Add Pydantic response models

```python
class SimilarHadithResult(BaseModel):
    hadith_id: str
    hadith_index: int
    source: str
    similarity: float
    matn_preview: str
    chains: list[dict]

class SimilarHadithsResponse(BaseModel):
    query_hadith_id: str
    results: list[SimilarHadithResult]
    total: int
```

### 3c. Add endpoint

```
GET /api/hadith/{source}/{hadith_index}/similar?limit=10&threshold=0.85
```

Logic:
1. Reconstruct `hadith_id = f"{SOURCE_TO_BOOK_ID[source]}_{hadith_index}"` (e.g. `"1681_11"`)
2. Fetch query embedding from `hadith_embeddings` — return 404 if not found
3. Run `$vectorSearch` aggregation:
   ```python
   pipeline = [
       {
           "$vectorSearch": {
               "index": "hadith_embedding_index",
               "path": "embedding",
               "queryVector": query_vector,
               "numCandidates": limit * 15,   # min 100 per Atlas docs
               "limit": limit + 1,
               "filter": {"hadith_id": {"$ne": hadith_id}},  # exclude self
           }
       },
       {
           "$project": {
               "hadith_id": 1, "page_number": 1, "book_id": 1,
               "matn_preview": 1,
               "score": {"$meta": "vectorSearchScore"},
               "_id": 0,
           }
       },
       {"$match": {"score": {"$gte": threshold}}},
       {"$limit": limit},
   ]
   ```
4. Enrich each result with chains from Neo4j (reuse existing chain query, parameterized by `hadith_id`)
5. Return `SimilarHadithsResponse`
6. Error cases: 404 embedding missing, 503 Atlas index not ready (`OperationFailure`)

---

## Step 4 — Frontend Types & API

**Modify:** `frontend/src/lib/types.ts` — add:

```typescript
export interface SimilarHadithResult {
  hadith_id: string;
  hadith_index: number;
  source: string;
  similarity: number;      // 0.0–1.0 cosine score
  matn_preview: string;
  chains: Chain[];
}

export interface SimilarHadithsResponse {
  query_hadith_id: string;
  results: SimilarHadithResult[];
  total: number;
}
```

**Modify:** `frontend/src/lib/api.ts` — add:

```typescript
export async function getSimilarHadiths(
  source: string,
  hadithIndex: number,
  limit = 10,
  threshold = 0.85,
): Promise<SimilarHadithsResponse> {
  if (USE_MOCK) {
    return { query_hadith_id: `1681_${hadithIndex}`, results: [], total: 0 };
  }
  const params = new URLSearchParams({ limit: String(limit), threshold: String(threshold) });
  return fetchJson<SimilarHadithsResponse>(
    `${API_URL}/api/hadith/${source}/${hadithIndex}/similar?${params}`,
  );
}
```

---

## Step 5 — New Frontend Components

### `frontend/src/components/SimilarHadiths.tsx` (new)

Props: `results: SimilarHadithResult[]`, `isLoading: boolean`

Renders:
- Loading state → "جارٍ البحث عن أحاديث مشابهة..."
- Empty state → "لا توجد أحاديث مشابهة بهذا المستوى من التطابق"
- Card list — each card shows:
  - Similarity % badge (large, prominent)
  - `matn_preview` text (RTL, Arabic font)
  - Chain count
  - Link to hadith detail page
- Toggle button "شجرة الأسانيد المجمعة" → expands `CombinedChainGraphView`

### `frontend/src/components/CombinedChainGraphView.tsx` (new)

Thin wrapper around the existing `ChainGraphView` — no changes to `ChainGraphView` needed:

```typescript
import ChainGraphView from "./ChainGraphView";

export default function CombinedChainGraphView({ chains, onNarratorClick = () => {} }) {
  return (
    <div>
      <p dir="rtl" className="text-xs text-muted mb-2">
        الراوي المشترك بين الأحاديث يظهر مرة واحدة فقط
      </p>
      <ChainGraphView chains={chains} onNarratorClick={onNarratorClick} />
    </div>
  );
}
```

The existing node deduplication in `ChainGraphView` (nodes keyed by narrator name) handles shared narrators across multiple hadiths' chains automatically.

---

## Step 6 — Wire into Hadith Detail Page

**Modify:** `frontend/src/app/hadith/[source]/[index]/page.tsx`

1. Add state: `similarHadiths: SimilarHadithResult[]`, `isSimilarLoading: boolean`
2. After the main hadith loads, fire a non-blocking call to `getSimilarHadiths()` — does not delay main content rendering
3. Render `<SimilarHadiths results={similarHadiths} isLoading={isSimilarLoading} />` below the chains section

---

## Files Changed / Created

| File | Action |
|---|---|
| `embed/generate_embeddings.py` | **Create** — one-time offline embedding script |
| `requirements.txt` | **Modify** — add `cohere>=5.0.0` |
| `backend/main.py` | **Modify** — add MongoClient, Pydantic models, `/similar` endpoint |
| `frontend/src/lib/types.ts` | **Modify** — add `SimilarHadithResult`, `SimilarHadithsResponse` |
| `frontend/src/lib/api.ts` | **Modify** — add `getSimilarHadiths` |
| `frontend/src/components/SimilarHadiths.tsx` | **Create** |
| `frontend/src/components/CombinedChainGraphView.tsx` | **Create** — wraps existing `ChainGraphView` |
| `frontend/src/app/hadith/[source]/[index]/page.tsx` | **Modify** — add similar hadiths section |

**Reused without modification:**
- `frontend/src/components/ChainGraphView.tsx` — existing graph component
- `mongo_migration/upload.py` — batch upsert pattern reference
- `extract_data_v2/build_graph.py` — reference for Neo4j `hadith_id` format (`"{book_id}_{page_number}"`)

---

## Key Technical Notes

| Topic | Detail |
|---|---|
| `hadith_id` format | `"1681_{page_number}"` — defined in `build_graph.py`, used as the join key between Neo4j and MongoDB |
| Cohere batch size | Hard limit of 96 texts per API call for `embed-multilingual-v3` |
| `input_type` | Use `"search_document"` for storage embeddings. Use `"search_query"` if adding free-text semantic search later |
| `numCandidates` | Set to `limit * 15` (minimum 100). Controls ANN candidate pool — larger = more accurate, slightly slower |
| Threshold default | `0.85` cosine similarity. The "إنما الأعمال بالنيات" variations should score ~0.87–0.95 against each other |
| Resume support | Script checks `hadith_embeddings` for existing IDs before calling Cohere — safe to re-run after interruption |

---

## Verification

```bash
# 1. Test embedding script on first 10 hadiths
python embed/generate_embeddings.py --limit 10
# Check: db.hadith_embeddings.findOne().embedding.length === 1024

# 2. Full embedding run (~2 minutes)
python embed/generate_embeddings.py
# Check: db.hadith_embeddings.countDocuments() ~= 7230

# 3. Create Atlas Vector Search index in Atlas UI → wait for "Active"

# 4. Start FastAPI backend
uvicorn backend.main:app --reload

# 5. Test with known variation hadith (إنما الأعمال بالنيات)
curl "http://localhost:8000/api/hadith/bukhari/1/similar?threshold=0.85"
# Expect: 4–6 results with similarity >= 0.85

# 6. Frontend smoke test: navigate to /hadith/bukhari/1
# - "أحاديث مشابهة" section loads below chains
# - Cards show similarity %, Arabic matn, chain count
# - "شجرة الأسانيد المجمعة" button renders combined graph
# - Shared narrators appear as single nodes (deduplicated)
```
