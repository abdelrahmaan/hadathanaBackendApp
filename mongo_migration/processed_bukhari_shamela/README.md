# Bukhari Shamela Pipeline

Data source: [shamela.ws](https://shamela.ws) (book 1681 — Sahih al-Bukhari), scraped via Firecrawl + LLM extraction.

## Input files

From `extract_data_v2/firecrawl/`:

| File | Description |
|------|-------------|
| `shamela_book_1681.jsonl` | Raw scraped pages from Shamela |

From `extract_data_v2/Bukhari/`:

| File | Description |
|------|-------------|
| `narrators_list.json` | Narrator ground truth data |

## Output files

| File | MongoDB Collection | Description |
|------|--------------------|-------------|
| `hadith_pages.jsonl` | `hadith_pages` | Cleaned page-level hadith data |
| `narrators.jsonl` | `narrators` | Narrator profiles |
| `preprocessed_bukhari.jsonl` | `bukhari_book` | Preprocessed hadiths with narrator chains |

## Scripts

| Script | Description |
|--------|-------------|
| `preprocess_pages.py` | Cleans raw Shamela pages, produces `hadith_pages.jsonl` |
| `preprocess_hadiths.py` | Extracts hadiths with chains, produces `preprocessed_bukhari.jsonl` and `narrators.jsonl` |
| `compute_stats.py` | Computes narrator statistics (teacher/student relationships) |

## Build MongoDB (full pipeline)

```bash
# Using the uv venv at the parent projects directory
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"

# 1. Preprocess pages → hadith_pages.jsonl
"$PYTHON" mongo_migration/processed_bukhari_shamela/preprocess_pages.py

# 2. Preprocess hadiths → preprocessed_bukhari.jsonl + narrators.jsonl
"$PYTHON" mongo_migration/processed_bukhari_shamela/preprocess_hadiths.py

# 3. Upload JSONL → MongoDB Atlas
"$PYTHON" mongo_migration/upload.py

# 4. Create indexes
"$PYTHON" mongo_migration/create_indexes.py

# 5. Compute narrator stats
"$PYTHON" mongo_migration/processed_bukhari_shamela/compute_stats.py
```
