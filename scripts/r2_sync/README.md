# R2 Data Sync — Cloudflare R2 Snapshot Manager

Large datasets are stored in Cloudflare R2 (S3-compatible) instead of git to keep the repo lightweight. This directory contains scripts to list, push, and pull dated snapshots.

## Scripts

| Script | Description |
|---|---|
| `config.py` | Shared config — loads `.env`, validates R2 credentials, creates boto3 client |
| `list_snapshots.py` | List all datasets and their available date snapshots |
| `pull_snapshot.py` | Download a snapshot to `data_snapshots/` (with resume, retry, progress) |
| `push_snapshot.py` | Upload a local folder as a dated snapshot (with multipart for large files) |

## Setup

1. Install dependencies:
   ```bash
   pip install boto3 python-dotenv tqdm
   ```

2. Add R2 credentials to your `.env` (see `.env.example`):
   ```
   R2_ENDPOINT_URL=https://<account_id>.r2.cloudflarestorage.com
   R2_BUCKET=hadathana_data
   R2_ACCESS_KEY_ID=<your_access_key>
   R2_SECRET_ACCESS_KEY=<your_secret_key>
   ```

   Generate API tokens at: Cloudflare Dashboard > R2 > Manage R2 API Tokens.

## R2 Bucket Layout

```
hadathana_data/                       # bucket
  snapshots/                          # prefix (configurable via R2_PREFIX)
    bukhari_shamela/                  # dataset name
      2026-03-29/                     # date-stamped snapshot
        raw_shamela_books.jsonl
        raw_shamela_narrators.jsonl
      2026-04-05/
        ...
    bukhari_podia/
      2026-03-29/
        ...
```

## Usage

All commands should be run from the **repo root**.

### List snapshots

```bash
python scripts/r2_sync/list_snapshots.py                          # all datasets
python scripts/r2_sync/list_snapshots.py --dataset bukhari_shamela # specific dataset
```

### Download (pull) a snapshot

```bash
# Download the entire latest snapshot
python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest

# Download only specific directories (faster — skip what you don't need)
python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest \
  --path extract_data_v2/playwrite/ \
  --path mongo_migration/processed_bukhari_podia/

# Download a single file
python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest \
  --path extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json

# Download by glob pattern
python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest --path '*.csv'

# Dry run — list what would be downloaded without downloading
python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest --dry-run
python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest --path extract_data_v2/playwrite/ --dry-run

# Download a specific date
python scripts/r2_sync/pull_snapshot.py --dataset full_backup --date 2026-03-29
```

Files are saved to `data_snapshots/<dataset>/<date>/` (configurable via `R2_LOCAL_DIR`).

- `--path` can be repeated to pull multiple directories/files in one command.
- Existing files with matching size are skipped (resume support).
- Failed downloads are retried up to 3 times.

### Upload (push) a snapshot

```bash
# Upload a folder as today's snapshot
python scripts/r2_sync/push_snapshot.py --dataset bukhari_shamela --source data/hadith_of_shamel/

# Upload with a specific date
python scripts/r2_sync/push_snapshot.py --dataset bukhari_shamela --source data/ --date 2026-03-29
```

Large files (>8 MB) use multipart upload automatically.

## Dataset Naming Conventions

| Dataset name | Contents |
|---|---|
| `bukhari_shamela` | Shamela pipeline data (hadiths, narrators, pages) |
| `bukhari_podia` | Podia pipeline data (hadiths, narrators, biographies) |
| `tarajm` | Tarajm.com narrator biographies |

Add new dataset names as needed — the scripts accept any string.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `R2_ENDPOINT_URL` | Yes | — | S3-compatible endpoint URL |
| `R2_BUCKET` | Yes | — | R2 bucket name |
| `R2_ACCESS_KEY_ID` | Yes | — | R2 API token access key |
| `R2_SECRET_ACCESS_KEY` | Yes | — | R2 API token secret key |
| `R2_PREFIX` | No | `snapshots/` | Key prefix in bucket |
| `R2_LOCAL_DIR` | No | `data_snapshots/` | Local download directory |

## Notes

- `data_snapshots/` is in `.gitignore` — never commit downloaded data.
- Credentials load from `.env` at runtime — never hardcode them.
