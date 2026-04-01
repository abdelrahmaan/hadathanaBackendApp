# Plan: Fix rawi_id 1568 Data Bug (نافع vs ابن عمر)

## Context

In `bukhari_pedia_advanced_extraction_results.json`, the LLM-based narrator matching incorrectly assigned `rawi_id=1568` (which belongs to **نافع**, the freedman of Ibn Umar) to **ابن عمر** as well. This causes ~292 chain entries across affected hadiths to have a duplicate rawi_id for two distinct historical figures, manifesting as duplicate nodes in the graph visualization.

The fix scripts already exist (`detect_rawi_id_conflicts.py`, `patch_rawi_id_1568.py`). What remains is: run the patch on the right file, copy it to the location `preprocess.py` expects, and re-run the full Podia preprocessing + upload pipeline.

---

## Critical Files

- **Source data (patch input):** `data_snapshots/full_backup/2026-03-29/extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json`
- **Patch script:** `scripts/patch_rawi_id_1568.py`
- **Detect script:** `scripts/detect_rawi_id_conflicts.py`
- **preprocess.py input:** `extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json`
- **preprocess.py:** `mongo_migration/processed_bukhari_podia/preprocess.py`
- **Upload script:** `mongo_migration/upload.py`

---

## Implementation Steps

### Step 1 — Dry run to confirm scope
```bash
python scripts/patch_rawi_id_1568.py --dry-run
```
Expected: ~292 entries across ~292 hadiths would be patched.

### Step 2 — Patch the data_snapshots copy, write to the working location
```bash
python scripts/patch_rawi_id_1568.py \
  --output extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json
```
This leaves the snapshot copy untouched and writes the patched file where `preprocess.py` expects it.

### Step 3 — Verify no conflicts remain (optional sanity check)
```bash
python scripts/detect_rawi_id_conflicts.py \
  --input extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json
```
Expected output: "No rawi_id conflicts found."

### Step 4 — Re-run Podia preprocessing
```bash
python mongo_migration/processed_bukhari_podia/preprocess.py
```
Regenerates all 4 JSONL output files in `mongo_migration/processed_bukhari_podia/`.

### Step 5 — Re-upload to MongoDB
```bash
python mongo_migration/upload.py
```
Upserts updated documents into `processed_podia_books` and related collections.

### Step 6 — Recompute narrator stats
```bash
python mongo_migration/processed_bukhari_podia/compute_stats.py
```
Regenerates `analytics_narrator_stats_podia` with corrected chain data.

### Step 7 — Update tasks.md
Mark the chore log entry "fix: detect and patch duplicate rawi_id" as Done, add touched files and summary.

---

## Verification

1. Run `detect_rawi_id_conflicts.py` after step 2 — should report zero conflicts.
2. Query MongoDB: hadith 468 in `processed_podia_books` should show نافع with rawi_id=1568 and ابن عمر with rawi_id=772 in separate chain positions.
3. Hit the API: `GET /api/v2/hadiths/468` — chains should have no duplicate rawi_ids.
4. Frontend graph for hadith 468 should render نافع and ابن عمر as distinct nodes with different IDs.
