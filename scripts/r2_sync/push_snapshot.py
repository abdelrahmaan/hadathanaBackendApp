#!/usr/bin/env python3
"""Upload a local folder as a dated snapshot to Cloudflare R2.

Usage:
    python scripts/r2_sync/push_snapshot.py --dataset full_backup --source . --extensions json,jsonl,csv,xlsx,parquet
    python scripts/r2_sync/push_snapshot.py --dataset bukhari_podia --source mongo_migration/processed_bukhari_podia/
    python scripts/r2_sync/push_snapshot.py --dataset bukhari_shamela --source data/ --date 2026-03-29
"""

import argparse
import sys
from datetime import date as date_type
from pathlib import Path

from config import (
    get_s3_client,
    get_bucket,
    R2_PREFIX,
    TRANSFER_CONFIG,
)

try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# Always skip these directories and files
SKIP_DIRS = {
    "__pycache__",
    ".git",
    ".venv",
    "venv",
    "node_modules",
    ".next",
    ".idea",
    ".vscode",
}
SKIP_FILES = {".DS_Store", "Thumbs.db", ".gitkeep"}


def collect_files(
    source: Path,
    extensions: set[str] | None = None,
) -> list[Path]:
    """Recursively collect files, skipping junk dirs/files.

    If extensions is provided, only include files with those suffixes.
    """
    if not source.is_dir():
        print(f"ERROR: Source is not a directory: {source}")
        sys.exit(1)

    files = []
    for f in sorted(source.rglob("*")):
        if not f.is_file():
            continue
        # Skip junk directories
        if any(part in SKIP_DIRS for part in f.parts):
            continue
        # Skip junk files
        if f.name in SKIP_FILES:
            continue
        # Skip hidden files (dotfiles)
        if f.name.startswith("."):
            continue
        # Filter by extension if specified
        if extensions and f.suffix.lstrip(".").lower() not in extensions:
            continue
        files.append(f)

    if not files:
        print(f"ERROR: No matching files found in {source}")
        if extensions:
            print(f"  (filtered to extensions: {', '.join(sorted(extensions))})")
        sys.exit(1)

    return files


def format_size(nbytes: int) -> str:
    """Human-readable file size."""
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def upload_file(client, bucket: str, local_path: Path, key: str) -> bool:
    """Upload a single file with multipart support. Returns True on success."""
    try:
        client.upload_file(
            str(local_path),
            bucket,
            key,
            Config=TRANSFER_CONFIG,
        )
        return True
    except Exception as e:
        print(f"  FAILED: {key} — {e}")
        return False


def push_snapshot(client, bucket: str, dataset: str, date_str: str, source: Path,
                  extensions: set[str] | None = None, dry_run: bool = False):
    """Upload all files from source directory to R2."""
    files = collect_files(source, extensions)
    total_size = sum(f.stat().st_size for f in files)
    snapshot_prefix = f"{R2_PREFIX}{dataset}/{date_str}/"

    print(f"{'[DRY RUN] ' if dry_run else ''}Uploading {len(files)} files ({format_size(total_size)}) → r2://{bucket}/{snapshot_prefix}")

    if dry_run:
        for f in files:
            relative = f.relative_to(source)
            size = f.stat().st_size
            print(f"  {relative} ({format_size(size)})")
        print(f"\nTotal: {len(files)} files, {format_size(total_size)}")
        return

    failed = []
    if HAS_TQDM:
        bar = tqdm(total=total_size, unit="B", unit_scale=True, desc="Uploading")
        for f in files:
            relative = f.relative_to(source)
            key = f"{snapshot_prefix}{relative}"
            size = f.stat().st_size
            ok = upload_file(client, bucket, f, key)
            if not ok:
                failed.append(str(relative))
            bar.update(size)
        bar.close()
    else:
        for i, f in enumerate(files, 1):
            relative = f.relative_to(source)
            key = f"{snapshot_prefix}{relative}"
            size = f.stat().st_size
            print(f"  [{i}/{len(files)}] {relative} ({format_size(size)})")
            ok = upload_file(client, bucket, f, key)
            if not ok:
                failed.append(str(relative))

    if failed:
        print(f"\n{len(failed)} file(s) failed to upload:")
        for name in failed:
            print(f"  - {name}")
        sys.exit(1)
    else:
        print(f"\nDone. Snapshot uploaded to r2://{bucket}/{snapshot_prefix}")


def main():
    parser = argparse.ArgumentParser(
        description="Upload a local folder as a snapshot to Cloudflare R2.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  # Push only data files from entire repo (preserves hierarchy)\n"
               "  python scripts/r2_sync/push_snapshot.py --dataset full_backup --source . --extensions json,jsonl,csv,xlsx,parquet\n\n"
               "  # Push a specific folder\n"
               "  python scripts/r2_sync/push_snapshot.py --dataset bukhari_podia --source mongo_migration/processed_bukhari_podia/\n\n"
               "  # Dry run (list files without uploading)\n"
               "  python scripts/r2_sync/push_snapshot.py --dataset full_backup --source . --extensions json,jsonl,csv,xlsx,parquet --dry-run\n",
    )
    parser.add_argument("--dataset", required=True, help="Dataset name (used as folder in R2)")
    parser.add_argument("--source", required=True, help="Local folder to upload")
    parser.add_argument(
        "--date",
        default=str(date_type.today()),
        help="Snapshot date folder (default: today YYYY-MM-DD)",
    )
    parser.add_argument(
        "--extensions",
        help="Comma-separated file extensions to include (e.g. json,jsonl,csv,xlsx,parquet)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be uploaded without actually uploading",
    )
    args = parser.parse_args()

    source = Path(args.source)
    if not source.is_absolute():
        source = Path.cwd() / source

    extensions = None
    if args.extensions:
        extensions = {e.strip().lower().lstrip(".") for e in args.extensions.split(",")}

    client = get_s3_client()
    bucket = get_bucket()

    push_snapshot(client, bucket, args.dataset, args.date, source,
                  extensions=extensions, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
