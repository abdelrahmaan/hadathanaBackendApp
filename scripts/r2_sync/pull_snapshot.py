#!/usr/bin/env python3
"""Download a dataset snapshot from Cloudflare R2 to local disk.

Usage:
    python scripts/r2_sync/pull_snapshot.py --dataset bukhari_shamela --latest
    python scripts/r2_sync/pull_snapshot.py --dataset bukhari_shamela --date 2026-03-29
"""

import argparse
import sys
import time
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


def pull_snapshot(client, bucket: str, dataset: str, date: str):
    """Download all files for a dataset/date snapshot."""
    snapshot_prefix = f"{R2_PREFIX}{dataset}/{date}/"
    objects = list_objects(client, bucket, snapshot_prefix)

    if not objects:
        print(f"No files found at r2://{bucket}/{snapshot_prefix}")
        sys.exit(1)

    total_size = sum(o["Size"] for o in objects)
    dest_root = local_dir() / dataset / date

    print(f"Downloading {len(objects)} files ({format_size(total_size)}) → {dest_root}")

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
               "  python scripts/r2_sync/pull_snapshot.py --dataset bukhari_shamela --latest\n"
               "  python scripts/r2_sync/pull_snapshot.py --dataset bukhari_shamela --date 2026-03-29\n",
    )
    parser.add_argument("--dataset", required=True, help="Dataset name to download")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--latest", action="store_true", help="Download the latest snapshot")
    group.add_argument("--date", help="Specific snapshot date (YYYY-MM-DD)")
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

    pull_snapshot(client, bucket, args.dataset, date)


if __name__ == "__main__":
    main()
