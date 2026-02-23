"""
rescrape_missing_hadith.py

Re-scrapes pages that cover missing hadith indices from the verification report.

Logic:
1. Load missing_hadith_indices from hadith_verification_report_1681.json
2. Build a (page -> [hadith indices]) map from the existing JSONL success entries
3. For each missing hadith index, find the gap pages between the last page whose
   max index is below it and the first page whose min index is above it — those
   gap pages are the ones that were never successfully scraped (or scraped without
   narrator data) and contain the missing hadith.
4. Re-scrape all resolved gap pages using the same scrape_with_firecrawl machinery,
   overwriting old failed/missing entries in the JSONL.

pages_with_no_idx_extracted are intentionally skipped: they are already success
entries in the JSONL — their hadith blocks just didn't have a parseable index
prefix (content issue, not a scraping issue).
"""

import re
import json
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set

from shamela_firecrawl import (
    scrape_with_firecrawl,
    append_jsonl,
    remove_failed_entries,
    SkipReason,
)

# ── Config ────────────────────────────────────────────────────────────────────

API_KEYS = [
    "fc-bb3459dabca8414b8c92f647cde7ebf3",
    "fc-68d7c10c71b74bb5a52d3e7534f28730",
    "fc-ff5958295ba0497280bc8cc9ca8f5279",
    "fc-a0e6b09c69d5441293d77c29a403ae85",
]

BOOK_ID = 1681
DELAY_SECONDS = 3.0
MAX_WORKERS = 2

REPORT_PATH = Path(__file__).parent / "hadith_verification_report_1681.json"
JSONL_PATH = Path(__file__).parent / f"shamela_book_{BOOK_ID}.jsonl"
DEBUG_DIR = Path(__file__).parent / f"debug_html_{BOOK_ID}_missing"

# ── Arabic numeral helpers ────────────────────────────────────────────────────

ARABIC_TO_INT = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
IDX_RE = re.compile(r"^([٠-٩]+)\s*[-–—]")


def parse_hadith_idx(full_text: str):
    m = IDX_RE.match(full_text.strip())
    if m:
        return int(m.group(1).translate(ARABIC_TO_INT))
    return None


# ── JSONL helpers ─────────────────────────────────────────────────────────────

def load_success_pages(jsonl_path: Path) -> Set[int]:
    """Return set of page numbers with status=success."""
    pages = set()
    if not jsonl_path.exists():
        return pages
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("status") == "success":
                pages.add(obj["page_number"])
    return pages


def build_page_index_ranges(jsonl_path: Path):
    """
    Return sorted list of (page, min_idx, max_idx) for all success pages
    that have at least one parseable hadith index.
    """
    page_indices: dict[int, list] = {}
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("status") != "success":
                continue
            page = obj["page_number"]
            for block in obj.get("hadith_blocks", []):
                idx = parse_hadith_idx(block.get("full_text", ""))
                if idx is not None:
                    page_indices.setdefault(page, []).append(idx)

    ranges = [
        (page, min(idxs), max(idxs))
        for page, idxs in page_indices.items()
    ]
    ranges.sort(key=lambda x: x[1])  # sort by min hadith index
    return ranges


def resolve_pages_for_missing_indices(
    missing_indices: List[int],
    page_ranges: list,
) -> Set[int]:
    """
    For each missing hadith index, find the gap pages between the last page
    whose max_idx < missing_idx and the first page whose min_idx > missing_idx.
    Returns the union of all such gap pages.
    """
    pages_to_scrape: Set[int] = set()
    unresolved = []

    for miss_idx in missing_indices:
        prev_page = None
        next_page = None

        for (page, mn, mx) in page_ranges:
            if mx < miss_idx:
                prev_page = page
            elif mn > miss_idx and next_page is None:
                next_page = page
                break

        if prev_page is not None and next_page is not None:
            gap = list(range(prev_page + 1, next_page))
            pages_to_scrape.update(gap)
        else:
            unresolved.append(miss_idx)

    if unresolved:
        print(f"WARNING: Could not resolve pages for {len(unresolved)} indices: {unresolved[:10]}...")

    return pages_to_scrape


# ── Key rotation (thread-safe) ────────────────────────────────────────────────

_key_lock = threading.Lock()


def scrape_page(book_id: int, page_num: int, api_keys: List[str], key_state: dict,
                jsonl_path: Path, debug_dir: Path):
    """Scrape a single page with key rotation on 402. Returns outcome dict or None."""
    url = f"https://shamela.ws/book/{book_id}/{page_num}"

    with _key_lock:
        current_key = key_state["key"]

    result = scrape_with_firecrawl(url, current_key)

    # 402 quota exhausted → rotate key and retry once
    if not result.success and "402" in result.message:
        with _key_lock:
            if key_state["key"] == current_key:
                key_state["index"] += 1
                if key_state["index"] < len(api_keys):
                    key_state["key"] = api_keys[key_state["index"]]
                    print(f"\n  ** API key exhausted. Switching to key "
                          f"{key_state['index'] + 1}/{len(api_keys)} **")
                else:
                    print(f"\n  ** All {len(api_keys)} API keys exhausted! **")
                    _write_failed(book_id, page_num, url, "api_failure",
                                  "All API keys exhausted (402)", jsonl_path)
                    return None  # signal caller to stop

            new_key = key_state["key"]

        if key_state["index"] >= len(api_keys):
            _write_failed(book_id, page_num, url, "api_failure",
                          "All API keys exhausted (402)", jsonl_path)
            return None

        result = scrape_with_firecrawl(url, new_key)

    if result.success:
        obj = {
            "status": "success",
            "book_id": book_id,
            "page_number": page_num,
            **result.data,
        }
        append_jsonl(obj, jsonl_path)
        return {"success": True, "reason": None}
    else:
        _write_failed(book_id, page_num, url, result.reason.value,
                      result.message, jsonl_path)
        if debug_dir and result.raw_html_snippet:
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / f"page_{page_num}_{result.reason.value}.html").write_text(
                result.raw_html_snippet, encoding="utf-8"
            )
        return {"success": False, "reason": result.reason}


def _write_failed(book_id, page_num, url, reason, message, jsonl_path):
    append_jsonl({
        "status": "failed",
        "book_id": book_id,
        "page_number": page_num,
        "url": url,
        "reason": reason,
        "message": message,
    }, jsonl_path)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load report
    with open(REPORT_PATH, encoding="utf-8") as f:
        report = json.load(f)

    missing_indices: List[int] = report["missing_hadith_indices"]
    pages_with_no_idx: List[int] = report.get("pages_with_no_idx_extracted", [])

    print(f"Missing hadith indices: {len(missing_indices)}")
    print(f"pages_with_no_idx_extracted: {len(pages_with_no_idx)} "
          f"(all already success — skipping)")

    # Build page index ranges from existing JSONL
    print("\nBuilding page→hadith-index map from JSONL...")
    page_ranges = build_page_index_ranges(JSONL_PATH)
    print(f"Pages with parseable index data: {len(page_ranges)}")

    # Resolve gap pages for missing indices
    pages_to_scrape = resolve_pages_for_missing_indices(missing_indices, page_ranges)
    print(f"Gap pages resolved for missing indices: {len(pages_to_scrape)}")

    # Exclude pages already scraped successfully (they came back with no index — content issue)
    success_pages = load_success_pages(JSONL_PATH)
    pages_to_scrape -= success_pages
    print(f"After excluding already-success pages: {len(pages_to_scrape)}")

    if not pages_to_scrape:
        print("\nNothing to scrape. All resolved pages already have success entries.")
        return

    sorted_pages = sorted(pages_to_scrape)
    print(f"\nPages to re-scrape: {sorted_pages[:20]}{'...' if len(sorted_pages) > 20 else ''}")

    # Remove old failed entries so we can write fresh results
    remove_failed_entries(JSONL_PATH, pages_to_scrape)

    # Scrape
    key_state = {"index": 0, "key": API_KEYS[0]}
    print(f"\nUsing API key 1/{len(API_KEYS)}, workers={MAX_WORKERS}, delay={DELAY_SECONDS}s")
    print(f"{'='*60}")

    succeeded = 0
    failed = 0
    keys_exhausted = False
    total = len(sorted_pages)

    for batch_start in range(0, total, MAX_WORKERS):
        if keys_exhausted:
            break

        group = sorted_pages[batch_start:batch_start + MAX_WORKERS]
        for p in group:
            print(f"\n[{batch_start + group.index(p) + 1}/{total}] Page {p}")

        futures = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page_num in group:
                future = executor.submit(
                    scrape_page, BOOK_ID, page_num, API_KEYS, key_state,
                    JSONL_PATH, DEBUG_DIR
                )
                futures[future] = page_num

            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    outcome = future.result()
                    if outcome is None:
                        keys_exhausted = True
                    elif outcome["success"]:
                        succeeded += 1
                    else:
                        failed += 1
                except Exception as e:
                    print(f"  UNEXPECTED ERROR on page {page_num}: {e}")
                    _write_failed(BOOK_ID, page_num,
                                  f"https://shamela.ws/book/{BOOK_ID}/{page_num}",
                                  "unexpected_error", str(e), JSONL_PATH)
                    failed += 1

        if not keys_exhausted and batch_start + MAX_WORKERS < total:
            time.sleep(DELAY_SECONDS)

    print(f"\n{'='*60}")
    print(f"DONE")
    print(f"  Pages attempted : {total}")
    print(f"  Succeeded       : {succeeded}")
    print(f"  Failed          : {failed}")
    if keys_exhausted:
        print("  Stopped early: all API keys exhausted")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
