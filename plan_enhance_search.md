# Plan: Enhance Arabic Text Search

## Context

Search currently uses raw MongoDB `$regex` on `_plain` fields (tashkeel-stripped only). Users must type exact characters — e.g., "أبوهريرة" works but "ابوهريره" doesn't, because:
- `_plain` fields don't normalize hamza variants (أ/إ/آ vs ا)
- `_plain` fields don't normalize taa marbuta (ة vs ه)
- `_plain` fields don't normalize alef maqsura (ى vs ي)
- The existing `normalize_for_search()` in `normalization.py` handles hamza + tashkeel but is **never called in the API search path**
- It also **misses** taa marbuta and alef maqsura normalization

## Approach: Two Phases

### Phase 1 — Normalize Query + Add `_search` Fields (single deployment)

Since the data lives in local Docker MongoDB and can be re-bootstrapped from JSONL, we can do both the query-side and data-side fixes together.

#### 1. Extend `normalize_for_search()` in `normalization.py`

Add these mappings after existing hamza normalization:
- `ة` (taa marbuta U+0629) → `ه` (haa U+0647)
- `ى` (alef maqsura U+0649) → `ي` (yaa U+064A)

#### 2. Add `_search` fields to preprocessing scripts

Both scripts already have `strip_tashkeel()`. Add `normalize_for_search()` calls to generate `_search` fields alongside `_plain` fields:

- `mongo_migration/processed_bukhari_shamela/preprocess_hadiths.py` — add `hadith_search`, `matn_search`
- `mongo_migration/processed_bukhari_podia/preprocess.py` — add `hadith_text_search`, `sanad_text_search`, `matn_text_search`
- Narrator preprocessing — add `name_search`, `full_name_search`, etc.

#### 3. Normalize user queries in all 4 router files

Import `normalize_for_search` and `re.escape()`, apply to every text search parameter:

| File | Fields |
|---|---|
| `app/routers/hadiths_shamela.py` | `hadith_plain` → query `hadith_search` |
| `app/routers/hadiths_podia.py` | `hadith_text_plain` → query `hadith_text_search` |
| `app/routers/narrators_shamela.py` | `name_plain`, `kunya`, `nasab` → query `_search` variants |
| `app/routers/narrators_podia.py` | `full_name_plain`, `rank` → query `_search` variants |

Query parameter names stay the same for API backward compatibility; only the MongoDB field queried changes.

Also add `re.escape()` to prevent regex injection.

#### 4. Update Pydantic models

Add optional `_search` fields to models (backward-compatible `| None = None`):
- `app/models/` — `Hadith`, `PodiaHadith`, `Narrator`, `PodiaNarrator`

#### 5. Add indexes in `mongo_migration/create_indexes.py`

Add ascending indexes on new `_search` fields (same pattern as existing `_plain` indexes).

#### 6. Update bootstrap data

Re-run preprocessing to regenerate JSONL files with `_search` fields, then `docker compose down -v && docker compose up -d` to re-bootstrap.

### Phase 2 — Tests

- Unit tests for `normalize_for_search()` covering: hamza variants, taa marbuta, alef maqsura, tashkeel, tatweel, combined cases
- Integration tests for search endpoints verifying normalized queries match documents

## Files to Modify

| File | Change |
|---|---|
| `normalization.py` | Add ة→ه, ى→ي to `normalize_for_search()` |
| `app/routers/hadiths_shamela.py` | Import normalize, apply to query, use `_search` field |
| `app/routers/hadiths_podia.py` | Same |
| `app/routers/narrators_shamela.py` | Same |
| `app/routers/narrators_podia.py` | Same |
| `app/models/hadith.py` | Add optional `_search` fields |
| `app/models/hadith_podia.py` | Add optional `_search` fields |
| `app/models/narrator.py` | Add optional `_search` fields |
| `app/models/narrator_podia.py` | Add optional `_search` fields |
| `mongo_migration/processed_bukhari_shamela/preprocess_hadiths.py` | Generate `_search` fields |
| `mongo_migration/processed_bukhari_podia/preprocess.py` | Generate `_search` fields |
| `mongo_migration/create_indexes.py` | Add `_search` field indexes |
| `tests/test_normalization.py` | New — unit tests |

## Verification

1. Run `pytest tests/test_normalization.py` — normalization logic is correct
2. Re-run preprocessing, re-bootstrap Docker DB
3. Test searches manually:
   - `curl "http://localhost:8000/api/v1/hadiths?hadith_plain=ابوهريره"` — should match "أبو هريرة"
   - `curl "http://localhost:8000/api/v2/narrators?full_name_plain=ابراهيم"` — should match "إبراهيم"
   - `curl "http://localhost:8000/api/v2/hadiths?hadith_text_plain=موسي"` — should match "موسى"
4. Run full test suite: `pytest tests/ -v`
