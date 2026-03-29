#!/usr/bin/env python3
"""Download a dataset snapshot from Cloudflare R2 to local disk.

Usage:
    python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest
    python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest --path extract_data_v2/playwrite/
    python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest --path mongo_migration/processed_bukhari_podia/ --path extract_data_v2/playwrite/
"""

import argparse
import sys
import time
from fnmatch import fnmatch
from pathlib import Path

from config import (
    get_s3_client,
    get_bucket,
    local_dir,
    R2_PREFIX,
    TRANSFER_CONFIG,
)

try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def discover_latest_date(client, bucket: str, prefix: str, dataset: str) -> str | None:
    """Find the most recent date folder for a dataset."""
    full_prefix = f"{prefix}{dataset}/"
    dates: set[str] = set()
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        for obj in page.get("Contents", []):
            relative = obj["Key"][len(full_prefix):]
            parts = relative.split("/")
            if parts[0]:
                dates.add(parts[0])

    return sorted(dates)[-1] if dates else None


def list_objects(client, bucket: str, prefix: str) -> list[dict]:
    """List all objects under a prefix, returning [{Key, Size}, ...]."""
    objects = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({"Key": obj["Key"], "Size": obj.get("Size", 0)})
    return objects


def filter_objects(objects: list[dict], snapshot_prefix: str, path_filters: list[str]) -> list[dict]:
    """Filter objects by path prefixes or glob patterns.

    Each filter can be:
      - A directory prefix: "extract_data_v2/playwrite/" matches all files under it
      - A file path: "extract_data_v2/playwrite/rawi_lookup.json" matches exactly
      - A glob pattern: "*.csv" matches by filename pattern
    """
    filtered = []
    for obj in objects:
        relative = obj["Key"][len(snapshot_prefix):]
        for pf in path_filters:
            # Directory prefix match
            if pf.endswith("/") and relative.startswith(pf):
                filtered.append(obj)
                break
            # Exact file match
            elif relative == pf:
                filtered.append(obj)
                break
            # Glob pattern match (against full relative path and filename)
            elif "*" in pf or "?" in pf:
                filename = relative.rsplit("/", 1)[-1] if "/" in relative else relative
                if fnmatch(relative, pf) or fnmatch(filename, pf):
                    filtered.append(obj)
                    break
            # Prefix match without trailing slash (e.g. "mongo_migration/processed_bukhari_podia")
            elif relative.startswith(pf + "/") or relative == pf:
                filtered.append(obj)
                break
    return filtered


def download_file(client, bucket: str, key: str, dest: Path, size: int) -> bool:
    """Download a single file with retry. Returns True on success."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Resume: skip if local file exists with matching size
    if dest.exists() and dest.stat().st_size == size:
        return True

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client.download_file(
                bucket,
                key,
                str(dest),
                Config=TRANSFER_CONFIG,
            )
            return True
        except Exception as e:
            if attempt < MAX_RETRIES:
                print(f"  Retry {attempt}/{MAX_RETRIES} for {key}: {e}")
                time.sleep(RETRY_DELAY)
            else:
                print(f"  FAILED after {MAX_RETRIES} attempts: {key} — {e}")
                return False


def format_size(nbytes: int) -> str:
    """Human-readable file size."""
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def pull_snapshot(client, bucket: str, dataset: str, date: str,
                  path_filters: list[str] | None = None, dry_run: bool = False):
    """Download files for a dataset/date snapshot, optionally filtered by path."""
    snapshot_prefix = f"{R2_PREFIX}{dataset}/{date}/"
    objects = list_objects(client, bucket, snapshot_prefix)

    if not objects:
        print(f"No files found at r2://{bucket}/{snapshot_prefix}")
        sys.exit(1)

    if path_filters:
        objects = filter_objects(objects, snapshot_prefix, path_filters)
        if not objects:
            print(f"No files matched the path filter(s): {', '.join(path_filters)}")
            print(f"Total files in snapshot: use --dry-run without --path to see all files.")
            sys.exit(1)

    total_size = sum(o["Size"] for o in objects)
    dest_root = local_dir() / dataset / date

    print(f"{'[DRY RUN] ' if dry_run else ''}Downloading {len(objects)} files ({format_size(total_size)}) → {dest_root}")

    if dry_run:
        for obj in objects:
            relative = obj["Key"][len(snapshot_prefix):]
            print(f"  {relative} ({format_size(obj['Size'])})")
        print(f"\nTotal: {len(objects)} files, {format_size(total_size)}")
        return

    failed = []
    if HAS_TQDM:
        bar = tqdm(total=total_size, unit="B", unit_scale=True, desc="Downloading")
        for obj in objects:
            relative = obj["Key"][len(snapshot_prefix):]
            dest = dest_root / relative
            ok = download_file(client, bucket, obj["Key"], dest, obj["Size"])
            if not ok:
                failed.append(obj["Key"])
            bar.update(obj["Size"])
        bar.close()
    else:
        for i, obj in enumerate(objects, 1):
            relative = obj["Key"][len(snapshot_prefix):]
            dest = dest_root / relative
            print(f"  [{i}/{len(objects)}] {relative} ({format_size(obj['Size'])})")
            ok = download_file(client, bucket, obj["Key"], dest, obj["Size"])
            if not ok:
                failed.append(obj["Key"])

    if failed:
        print(f"\n{len(failed)} file(s) failed to download:")
        for f in failed:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print(f"\nDone. Snapshot saved to {dest_root}")


def main():
    parser = argparse.ArgumentParser(
        description="Download snapshots from Cloudflare R2.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  # Pull entire snapshot\n"
               "  python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest\n\n"
               "  # Pull specific directories\n"
               "  python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest \\\n"
               "    --path extract_data_v2/playwrite/ \\\n"
               "    --path mongo_migration/processed_bukhari_podia/\n\n"
               "  # Pull a single file\n"
               "  python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest \\\n"
               "    --path extract_data_v2/playwrite/bukhari_pedia_advanced_extraction_results.json\n\n"
               "  # Pull by glob pattern\n"
               "  python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest --path '*.csv'\n\n"
               "  # Dry run (list files without downloading)\n"
               "  python scripts/r2_sync/pull_snapshot.py --dataset full_backup --latest --dry-run\n",
    )
    parser.add_argument("--dataset", required=True, help="Dataset name to download")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--latest", action="store_true", help="Download the latest snapshot")
    group.add_argument("--date", help="Specific snapshot date (YYYY-MM-DD)")
    parser.add_argument(
        "--path", action="append", dest="paths",
        help="Filter to specific paths/directories (can be repeated). Supports directory prefixes, exact files, and glob patterns.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List files that would be downloaded without actually downloading",
    )
    args = parser.parse_args()

    client = get_s3_client()
    bucket = get_bucket()

    if args.latest:
        date = discover_latest_date(client, bucket, R2_PREFIX, args.dataset)
        if not date:
            print(f"No snapshots found for dataset '{args.dataset}'.")
            sys.exit(1)
        print(f"Latest snapshot: {date}")
    else:
        date = args.date

    pull_snapshot(client, bucket, args.dataset, date,
                  path_filters=args.paths, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
