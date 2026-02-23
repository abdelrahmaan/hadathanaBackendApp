# Firecrawl Data Pipeline

This directory contains scripts and outputs for scraping Shamela hadith pages and narrator profiles.

## Files In This Directory

- `shamela_firecrawl.py`
  - Main scraper for book pages (`/book/1681/<page>`).
  - Extracts hadith blocks, matn, narrators, breadcrumb links.
  - Writes page results to `shamela_book_1681.jsonl`.

- `retry_failed.py`
  - Retries failed pages from `shamela_book_1681.jsonl`.
  - Skips pages where failure reason is `no_narrators`.
  - Removes old failed rows for retryable pages, then appends fresh retry results.

- `shamela_narrator_scraper.py`
  - Scrapes narrator profile pages (`/narrator/<id>`).
  - Extracts fields like name/kunya/nasab/death date/ranks/jarh wa ta'dil.
  - Writes results to `shamela_narrators.jsonl` as `status=success|failed`.
  - Also builds `narrator_hadith_names.json` from hadith data.

- `enrich_narrator_ids.py`
  - Matches narrator names in `Bukhari_Without_Tashkel_results_advanced_with_matn.json` (no tashkeel) against `narrator_hadith_names.json` (with tashkeel).
  - Normalizes all File 2 name variants by stripping tashkeel and leading waw connectors, then builds an exact-match lookup.
  - Injects `narrator_id` into each narrator object in the output copy.
  - Marks ambiguous names (same stripped form maps to 2+ IDs) with `narrator_id: null, narrator_id_ambiguous: true`.
  - Prints match statistics and a list of unmatched/ambiguous names for manual review.
  - Output: `extract_data_v2/Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json`.

- `resolve_remaining_narrators.py`
  - Second-pass resolution for narrators left null by `enrich_narrator_ids.py`.
  - **Pass 1** — Shamela JSONL cross-reference: strips tashkeel from every narrator name in `shamela_book_1681.jsonl` and builds a richer lookup (4,302 unambiguous forms vs 2,858 in the initial lookup).
  - **Pass 2** — Context mappings: uses `Bukhari/resolved_context_mappings.json` (1,529 rules) to disambiguate short names like `سفيان` by looking at the next narrator in the chain.
  - **Pass 3** — Name mapping fallback: applies `narrator_mappings.json` (204 rules) for remaining cases.
  - Result: 68.2% → 87.8% resolution rate (39,260 / 44,733 narrator mentions).
  - Updates `Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json` in-place.

- `bukhari_narrator_coverage.py`
  - Builds `bukhari_narrator_coverage.jsonl`: one line per unique narrator ID found in the Bukhari enriched file.
  - Cross-references each ID against `shamela_narrators.jsonl` to attach full bio (name, kunya, nasab, tabaqa, rank_ibn_hajar, rank_dhahabi, death_date).
  - Prints a summary: unique IDs, bio coverage, top narrators by mention count.
  - 1,305 unique narrator IDs found; 1,303 (99.8%) have a bio in Shamela.

- `bukhari_narrator_coverage.jsonl`
  - Per-narrator coverage report for the Bukhari V2 dataset.
  - Fields: `narrator_id`, `mention_count`, `resolution_methods`, `bio_status`, bio fields from Shamela.

- `narrators_info_check.py`
  - Coverage + consistency checker between:
    - `shamela_book_1681.jsonl`
    - `narrator_hadith_names.json`
    - `shamela_narrators.jsonl`
  - Uses `status=success` rows from `shamela_narrators.jsonl` to decide "scraped" IDs.
  - Also checks name-variant completeness per narrator ID (not only ID existence).
  - Can:
    - append missing narrator IDs into `shamela_narrators.jsonl`
    - call `scrape_narrators(...)` from `shamela_narrator_scraper.py` for missing IDs
    - expand missing name variants into `narrator_hadith_names.json`

- `shamela_book_1681.jsonl`
  - Main page-level scrape output (JSONL).

- `shamela_narrators.jsonl`
  - Narrator profile scrape output (JSONL).

- `narrator_hadith_names.json`
  - Map: `narrator_id -> [name variants seen in hadith chains]`.

- `failure_report_1681.json`
  - Failure summary for book page scraping.

- `debug_html_1681/`
  - Saved HTML snapshots for page scraper debugging.

- `debug_html_1681_retry/`
  - Saved HTML snapshots for retry run debugging.

- `__pycache__/`
  - Python bytecode cache.

## Typical Workflow

1. Scrape/refresh hadith book pages.
2. Retry failed pages.
3. Scrape narrator profiles.
4. Run consistency checks and fill gaps.

## Commands

Run from this directory:

```bash
cd extract_data_v2/firecrawl
```

Book pages:

```bash
python3 shamela_firecrawl.py
python3 retry_failed.py
```

Narrator profiles:

```bash
python3 shamela_narrator_scraper.py
```

Enrich Bukhari hadith chains with narrator IDs (Phase 1 — exact match):

```bash
python3 enrich_narrator_ids.py
```

Resolve remaining unmatched/ambiguous narrators using Shamela + context rules (Phase 2):

```bash
python3 resolve_remaining_narrators.py
```

Check Bukhari narrator coverage and generate per-narrator bio report:

```bash
python3 bukhari_narrator_coverage.py
```

Coverage and gap handling:

```bash
python3 narrators_info_check.py
python3 narrators_info_check.py --expand-narrator-hadith-names
python3 narrators_info_check.py --scrape-missing --api-key "YOUR_FIRECRAWL_KEY"
python3 narrators_info_check.py --scrape-missing --append-missing-to-shamela --api-key "YOUR_FIRECRAWL_KEY"
```

Use multiple keys by repeating `--api-key` or setting env var:

```bash
export FIRECRAWL_API_KEYS="key1,key2"
python3 narrators_info_check.py --scrape-missing
```

Useful optional flags:

- `--show-limit 50`
  - Change how many IDs are printed in terminal summaries.
- `--output-json report.json`
  - Save full check report to a JSON file.
- `--max-workers 2 --delay 3`
  - Control narrator scrape concurrency and pacing when using `--scrape-missing`.

## Output Conventions

- JSONL files are append-oriented logs.
- For narrator data:
  - `status=success` means profile exists and is scraped.
  - `status=failed` means scrape attempt failed or profile is empty/invalid.
- `narrators_info_check.py` treats only `status=success` as "already scraped".
- `IDs with missing name variants` means:
  - The narrator ID exists in `narrator_hadith_names.json`, but one or more names seen in hadith chains are missing from that ID's name list.
  - Use `--expand-narrator-hadith-names` to merge them.

## Known Behavior

- IDs like `0` and `1` may appear in hadith chains but resolve to empty narrator profile pages on Shamela.
- These IDs can remain `failed` in `shamela_narrators.jsonl` even after retries.
