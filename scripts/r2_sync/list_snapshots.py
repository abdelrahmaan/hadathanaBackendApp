#!/usr/bin/env python3
"""List available dataset snapshots in Cloudflare R2.

Usage:
    python scripts/r2_sync/list_snapshots.py                # list all datasets
    python scripts/r2_sync/list_snapshots.py --dataset foo   # list snapshots for 'foo'
"""

import argparse
from collections import defaultdict

from config import get_s3_client, get_bucket, R2_PREFIX


def list_snapshots(client, bucket: str, prefix: str) -> dict[str, list[str]]:
    """Return {dataset_name: [date1, date2, ...]} sorted by date."""
    datasets: dict[str, set[str]] = defaultdict(set)
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=""):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Expected: snapshots/<dataset>/<date>/filename
            relative = key[len(prefix):]
            parts = relative.split("/")
            if len(parts) >= 2:
                dataset_name, date_str = parts[0], parts[1]
                if date_str:  # skip empty segments
                    datasets[dataset_name].add(date_str)

    # Sort dates within each dataset
    return {k: sorted(v) for k, v in sorted(datasets.items())}


def print_snapshots(snapshots: dict[str, list[str]], filter_dataset: str | None = None):
    """Pretty-print snapshot listing."""
    if not snapshots:
        print("No snapshots found.")
        return

    if filter_dataset:
        if filter_dataset not in snapshots:
            print(f"No snapshots found for dataset '{filter_dataset}'.")
            return
        snapshots = {filter_dataset: snapshots[filter_dataset]}

    for dataset, dates in snapshots.items():
        latest = dates[-1] if dates else "—"
        print(f"\n  Dataset: {dataset}")
        print(f"  Latest:  {latest}")
        print(f"  Snapshots ({len(dates)}):")
        for d in dates:
            marker = " <-- latest" if d == latest else ""
            print(f"    - {d}{marker}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description="List available snapshots in Cloudflare R2.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  python scripts/r2_sync/list_snapshots.py\n"
               "  python scripts/r2_sync/list_snapshots.py --dataset bukhari_shamela\n",
    )
    parser.add_argument("--dataset", help="Filter to a specific dataset name")
    args = parser.parse_args()

    client = get_s3_client()
    bucket = get_bucket()

    print(f"Listing snapshots in r2://{bucket}/{R2_PREFIX} ...")
    snapshots = list_snapshots(client, bucket, R2_PREFIX)
    print_snapshots(snapshots, filter_dataset=args.dataset)


if __name__ == "__main__":
    main()
