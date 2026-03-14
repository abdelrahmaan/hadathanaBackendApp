# Bukhari Podia Pipeline

Data source: [bukhari-pedia.net](https://bukhari-pedia.net), scraped via Playwright.

## Input files

From `extract_data_v2/playwrite/`:

| File | Used by | Description |
|------|---------|-------------|
| `hadith_narrators_bukhari_pedia_playwrite_preprocessing.jsonl` | MongoDB | Raw hadiths with flat narrator list |
| `narrators_bukhari_pedia_playwrite.jsonl` | MongoDB | Raw narrator profiles |
| `bukhari_pedia_advanced_extraction_results.json` | Neo4j | LLM-extracted chains with transmission types, sanad/matn separation, multi-chain support |

From this directory (user-provided):

| File | Description |
|------|-------------|
| `bukhari_narrators_tarajem.jsonl` | Narrator biographies (tarajim) from multiple sources |

## Output files

| File | MongoDB Collection | Description |
|------|--------------------|-------------|
| `bukhari_podia_hadiths.jsonl` | `bukhari_book_podia` | Cleaned hadiths with narrator chains |
| `bukhari_podia_narrators.jsonl` | `narrators_podia` | Deduplicated narrator profiles |
| `narrators_tarajem.jsonl` | `narrators_tarajem_podia` | Narrator biographies with plain text variants |

## Text field hierarchy

All text fields follow a 3-level hierarchy:

| Suffix | Description | Example |
|--------|-------------|---------|
| *(raw)* | Original text with tashkeel, as-is from source | `name_in_chain`, `hadith_text` |
| `_clean` | Cleaned (glyphs, footnotes, tatweel, prefixes removed), tashkeel kept | `name_in_chain_clean` |
| `_plain` | Cleaned + tashkeel stripped — for search/matching | `name_in_chain_plain`, `hadith_text_plain` |

## Text cleaning applied

- Hadith index prefixes removed (e.g. `7427_`, `3-`) using `hadith_indices`
- Scrape glyphs removed: `☺♦♣♠☻╡√~•|{}[]`
- Footnote markers removed: `( 1 )`, `( 2 )`, etc.
- `صلعم` expanded to `صلى الله عليه وسلم`
- Tatweel/kashida (`ـ`) removed
- Whitespace normalized

## Scripts

| Script | Target | Description |
|--------|--------|-------------|
| `preprocess.py` | MongoDB | Cleans raw data, produces output JSONL files |
| `compute_stats.py` | MongoDB | Computes narrator statistics (teacher/student relationships) |
| `build_graph.py` | Neo4j | Ingests advanced extraction data into Neo4j graph |
| `hadith_preprocessing.py` | — | Shared text cleaning utilities |

## Build MongoDB (full pipeline)

```bash
# Using the uv venv at the parent projects directory
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"

# 1. Preprocess raw data → JSONL
"$PYTHON" mongo_migration/processed_bukhari_podia/preprocess.py

# 2. Upload JSONL → MongoDB Atlas
"$PYTHON" mongo_migration/upload.py

# 3. Create indexes
"$PYTHON" mongo_migration/create_indexes.py

# 4. Compute narrator stats
"$PYTHON" mongo_migration/processed_bukhari_podia/compute_stats.py
```

## Build Neo4j Graph

Uses the advanced extraction file (multi-chain, transmission types, sanad/matn) — graph-native data NOT duplicated from MongoDB.

```bash
PYTHON="/Users/a.kamar/Documents/Abdo Kaamar/projects/.venv/bin/python"

# Dry run (stats only, no Neo4j needed)
"$PYTHON" mongo_migration/processed_bukhari_podia/build_graph.py --dry-run

# Full ingestion (requires NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD in .env)
"$PYTHON" mongo_migration/processed_bukhari_podia/build_graph.py
```

### What each database owns

| Aspect | MongoDB | Neo4j |
|--------|---------|-------|
| Hadith text | Flat text + narrator list (search/API) | Sanad + matn separated, tawabi |
| Chains | Flattened narrator array | Multi-chain (primary/nested/follow_up) with Chain nodes |
| Transmission | Not stored | samaa, anana, ijaza, mukataba, etc. per narrator |
| Narrators | Profiles, tarajem, stats | Graph relationships + biographical JSON |
| Best for | Text search, pagination, REST API | Chain traversal, network analysis, pattern queries |

See [schema_description.md](schema_description.md) for full Neo4j schema and example Cypher queries.
