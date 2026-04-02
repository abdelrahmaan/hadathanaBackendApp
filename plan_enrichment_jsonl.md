# Plan: Re-run Topic Tagging + Embeddings Offline (JSONL-first, Atlas-free)

## Context

The original `embed_matn.py` wrote `topics` and `matn_embedding` directly into **MongoDB Atlas** (`processed_podia_books`). Atlas is now unreachable. The local `bukhari_podia_hadiths.jsonl` has **neither field** — so `HadithDataDev` has no topics or embeddings either.

Goal: re-run both enrichments **without depending on MongoDB at all** during processing. Output goes to JSONL files first, then `topics` imported into `HadithDataDev`, then push to R2.

---

## Criticism of current `embed_matn.py`

| Problem | Impact |
|---|---|
| Writes directly to MongoDB — Atlas dependency at runtime | Entire job fails if Atlas is down (exactly what happened) |
| No JSONL output — enriched data never saved locally | Lost all topics/embeddings when Atlas went down |
| `backfill_topics_to_jsonl.py` reads *back* from Mongo | Useless when Atlas is down |
| Resume logic depends on MongoDB field existence | Can't resume without Mongo |

---

## New Scripts

### `scripts/tag_topics_jsonl.py`
- Reads `mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl` directly (no Mongo)
- Outputs `hadith_topics.jsonl` in repo root — **slim**: only `hadith_url` + `topics`
- Resume: loads existing output file at startup, skips already-processed `hadith_url`s
- Same LLM chain as `embed_matn.py` (Gemini Flash via OpenRouter, `langchain-openai`)
- CLI: `--dry-run`, `--limit N`, `--llm-batch N` (default 20)

### `scripts/embed_matn_jsonl.py`
- Reads `mongo_migration/processed_bukhari_podia/bukhari_podia_hadiths.jsonl` directly (no Mongo)
- Outputs `hadith_embeddings.jsonl` in repo root — **slim**: only `hadith_url` + `matn_embedding`
- Resume: loads existing output file at startup, skips already-processed `hadith_url`s
- Same Cohere embedder as `embed_matn.py` (`embed-v4.0`, `langchain-cohere`)
- CLI: `--dry-run`, `--limit N`, `--batch-size N` (default 96)

### No merge
Topics and embeddings serve different destinations — no merge back into `bukhari_podia_hadiths.jsonl`.
That file is **never modified**.

---

## Output Files (repo root)

| File | Content | Destination |
|---|---|---|
| `hadith_topics.jsonl` | `hadith_url` + `topics` (slim) | MongoDB `processed_podia_books` |
| `hadith_embeddings.jsonl` | `hadith_url` + `matn_embedding` (slim) | Qdrant / TBD |

Both are gitignored — need to add `/*.jsonl` to `.gitignore` (current rules only cover subdirs).

---

## tmux Session

```bash
tmux new-session -d -s enrichment
# Step 1 — topics
tmux send-keys -t enrichment 'cd ~/Projects/hadathanaBackendApp && /home/abdo_kamar/Projects/.venv/bin/python scripts/tag_topics_jsonl.py' Enter
# Step 2 — embeddings (after topics done)
tmux send-keys -t enrichment '/home/abdo_kamar/Projects/.venv/bin/python scripts/embed_matn_jsonl.py' Enter
```

---

## Import Topics into HadithDataDev

```bash
docker exec -i mongodb-hadathana mongoimport --db HadithDataDev \
  --collection processed_podia_books --mode=upsert --upsertFields=hadith_url \
  < hadith_topics.jsonl
```

**Embeddings** → kept in `hadith_embeddings.jsonl` only. Vector DB (Qdrant vs Atlas) deferred.

## Push to R2

```bash
python scripts/r2_sync/push_snapshot.py --dataset bukhari_podia \
  --source mongo_migration/processed_bukhari_podia/ --extensions jsonl
```

---

## Files to Create / Keep

| File | Action |
|---|---|
| `scripts/tag_topics_jsonl.py` | **New** |
| `scripts/embed_matn_jsonl.py` | **New** |
| `.gitignore` | Add `/*.jsonl` |
| `scripts/embed_matn.py` | Keep — still useful when Atlas is up |
| `scripts/backfill_topics_to_jsonl.py` | Keep — useful for Atlas backfill |

## Patterns to Reuse

- [scripts/embed_matn.py](scripts/embed_matn.py) — LLM chain, Cohere embedder, retry logic, `TOPIC_LIST`
- [scripts/backfill_topics_to_jsonl.py](scripts/backfill_topics_to_jsonl.py) — JSONL read/write, `hadith_url` key

---

## Verification

```bash
# Topics smoke test
python scripts/tag_topics_jsonl.py --limit 5
head -1 hadith_topics.jsonl \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(list(d.keys()), d.get('topics'))"
# Expected: ['hadith_url', 'topics'] ['الصلاة', ...]

# Embeddings smoke test
python scripts/embed_matn_jsonl.py --limit 5
head -1 hadith_embeddings.jsonl \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(list(d.keys()), len(d.get('matn_embedding',[])), 'dims')"
# Expected: ['hadith_url', 'matn_embedding'] 1536 dims

# After mongoimport into HadithDataDev
curl "http://localhost:8001/api/v2/topics"
curl "http://localhost:8001/api/v2/topics/الصلاة/hadiths?limit=2"
```
