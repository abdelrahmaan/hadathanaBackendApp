# Plan: Semantic Search for Hadith Matn

## Context

7,075 hadith matn embeddings (Cohere embed-v4.0, 1536-dim) are already generated in `hadith_embeddings.jsonl` at the repo root. They are NOT yet in MongoDB, and there is no semantic search endpoint. The goal is `GET /api/v2/hadiths/search/semantic?q=...`.

Approach: **MongoDB Atlas Vector Search** — already our DB, no new infra needed.

---

## Steps

### 1. `scripts/apply_embeddings.py` (new)

Bulk `$set` embeddings from `hadith_embeddings.jsonl` into `processed_podia_books`. Same safe pattern as `apply_topics.py` — merges field, does not replace document.

```bash
python scripts/apply_embeddings.py --db HadithDataDev
```

---

### 2. Create Atlas Vector Search index (manual, one-time)

Atlas vector indexes can't be created via pymongo — done in Atlas UI:

> Browse Collections → Search Indexes → Create Search Index → JSON editor

```json
{
  "fields": [{
    "type": "vector",
    "path": "matn_embedding",
    "numDimensions": 1536,
    "similarity": "cosine"
  }]
}
```

Index name: `matn_embedding_vector_index`

Add a printed reminder to `mongo_migration/create_indexes.py`.

---

### 3. `app/models/hadith_podia.py`

Add `matn_embedding` to the internal model but **exclude it from API responses** — 1536 floats ≈ 12KB per doc, not suitable for list endpoints.

Add a `PodiaHadithSlim` response model (no embedding field) used by list and search endpoints.

---

### 4. `app/routers/hadiths_podia.py` — new endpoint

```
GET /api/v2/hadiths/search/semantic?q=<arabic text>&limit=20&topic=<optional>
```

Logic:
1. Embed the user query with Cohere at request time (`input_type="search_query"`)
2. Run Atlas `$vectorSearch` aggregation:
   ```python
   {"$vectorSearch": {
       "index": "matn_embedding_vector_index",
       "path": "matn_embedding",
       "queryVector": query_embedding,
       "numCandidates": limit * 10,
       "limit": limit,
       "filter": {"topics": topic} if topic else {}
   }}
   ```
3. Return normal hadith shape + `score: float` (cosine similarity)

---

### 5. `app/config.py`

Confirm `COHERE_API_KEY` is in `Settings`. Already in `.env.example`.

---

## Rollout order

1. `scripts/apply_embeddings.py` → load into HadithDataDev
2. Create vector index in Atlas UI on HadithDataDev
3. Model + endpoint changes
4. Test locally on port 8001
5. Load embeddings into Atlas HadithData (prod)
6. Create vector index on Atlas HadithData too

---

## Verification

```bash
# Embeddings loaded?
docker exec mongodb-hadathana mongosh --quiet --eval \
  "db.getSiblingDB('HadithDataDev').processed_podia_books.findOne({matn_embedding: {\$exists:true}}).matn_embedding.length"
# Expected: 1536

# Semantic search working?
curl "http://localhost:8001/api/v2/hadiths/search/semantic?q=الصلاة+في+وقتها&limit=5"

# With topic pre-filter
curl "http://localhost:8001/api/v2/hadiths/search/semantic?q=الوضوء&topic=الطهارة&limit=5"
```

---

## Files touched

| File | Action |
|---|---|
| `scripts/apply_embeddings.py` | New — bulk $set embeddings into MongoDB |
| `app/models/hadith_podia.py` | Add `matn_embedding`, add `PodiaHadithSlim` response model |
| `app/routers/hadiths_podia.py` | Add semantic search endpoint + Cohere client |
| `app/config.py` | Confirm `COHERE_API_KEY` in Settings |
| `mongo_migration/create_indexes.py` | Add vector index reminder |
| `README.md` | Document endpoint + Atlas index setup steps |
| `tasks.md` | Mark v1.3 in progress |
