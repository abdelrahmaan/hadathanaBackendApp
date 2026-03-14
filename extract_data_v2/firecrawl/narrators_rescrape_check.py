#!/usr/bin/env python3
import argparse
import json
import time
import threading
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

START_PAGE = 10
END_PAGE = 11207

API_KEYS = [
    "fc-04554043bf914e728a96ba5cd7902d60",
    "fc-220712d1992f448795f850342e05d1bd",
]

BOOK_ID = 1681
BASE_DIR = Path(__file__).resolve().parent
MISSING_INDEX_FILE = BASE_DIR / "missing_hadith_index.json"


def sort_ids(values: Set[str]) -> list[str]:
    return sorted(values, key=lambda x: (0, int(x)) if x.isdigit() else (1, x))


def load_book_range_data(book_jsonl: Path, start_page: int, end_page: int):
    page_latest: Dict[int, dict] = {}
    narrators_from_success_pages: Set[str] = set()

    with book_jsonl.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {book_jsonl} at line {line_no}: {exc}") from exc

            page = obj.get("page_number")
            if not isinstance(page, int) or page < start_page or page > end_page:
                continue

            status = str(obj.get("status", "")).strip().lower()
            reason = str(obj.get("reason") or obj.get("message") or "").strip()
            page_latest[page] = {"status": status, "reason": reason}

            if status == "success":
                for block in obj.get("hadith_blocks", []):
                    for narrator in block.get("narrators", []):
                        nid = str(narrator.get("id", "")).strip()
                        if nid:
                            narrators_from_success_pages.add(nid)

    expected_pages = set(range(start_page, end_page + 1))
    seen_pages = set(page_latest.keys())
    missing_page_numbers = expected_pages - seen_pages

    success_pages = {p for p, v in page_latest.items() if v["status"] == "success"}
    failed_pages = {p for p, v in page_latest.items() if v["status"] != "success"}

    failed_reason_counter = Counter()
    for p in failed_pages:
        reason = page_latest[p]["reason"] or "unknown"
        failed_reason_counter[reason] += 1

    return {
        "expected_pages": len(expected_pages),
        "seen_pages": len(seen_pages),
        "missing_pages": len(missing_page_numbers),
        "success_pages": len(success_pages),
        "failed_pages": len(failed_pages),
        "failed_reason_counter": failed_reason_counter,
        "missing_page_numbers": sort_ids({str(p) for p in missing_page_numbers}),
        "narrator_ids": narrators_from_success_pages,
    }


def load_narrator_scrape_state(narrators_jsonl: Path):
    state: Dict[str, dict] = {}

    if not narrators_jsonl.exists():
        return state

    with narrators_jsonl.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {narrators_jsonl} at line {line_no}: {exc}") from exc

            nid = str(obj.get("narrator_id") or obj.get("id") or "").strip()
            if not nid:
                continue

            status = str(obj.get("status", "")).strip().lower()
            message = str(obj.get("message") or "").strip()

            # Success dominates; once success exists, keep it.
            if status == "success":
                state[nid] = {"status": "success", "message": ""}
                continue

            if nid not in state or state[nid]["status"] != "success":
                state[nid] = {"status": "failed", "message": message}

    return state


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check narrator scraping coverage from Shamela book pages in a page range."
    )
    parser.add_argument(
        "--book-jsonl",
        type=Path,
        default=BASE_DIR / "shamela_book_1681.jsonl",
        help="Path to shamela_book_1681.jsonl",
    )
    parser.add_argument(
        "--narrators-jsonl",
        type=Path,
        default=BASE_DIR / "shamela_narrators.jsonl",
        help="Path to shamela_narrators.jsonl",
    )
    parser.add_argument("--start-page", type=int, default=START_PAGE)
    parser.add_argument("--end-page", type=int, default=END_PAGE)
    parser.add_argument("--show-limit", type=int, default=20)
    parser.add_argument(
        "--rescrape-missing",
        action="store_true",
        help="Rescrape narrator IDs that are failed/not-scraped using API_KEYS in this file.",
    )
    parser.add_argument(
        "--rescrape-hadith",
        action="store_true",
        help="Rescrape missing hadith pages from missing_hadith_index.json into shamela_book_1681.jsonl.",
    )
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--delay", type=float, default=3.0)

    args = parser.parse_args()

    if args.start_page > args.end_page:
        raise ValueError("start-page must be <= end-page")

    book_stats = load_book_range_data(args.book_jsonl, args.start_page, args.end_page)
    narrator_state = load_narrator_scrape_state(args.narrators_jsonl)

    target_narrator_ids = book_stats["narrator_ids"]

    success_ids = {nid for nid in target_narrator_ids if narrator_state.get(nid, {}).get("status") == "success"}
    failed_ids = {nid for nid in target_narrator_ids if narrator_state.get(nid, {}).get("status") == "failed"}
    not_scraped_ids = target_narrator_ids - success_ids - failed_ids

    fail_reason_counter = Counter()
    for nid in failed_ids:
        reason = narrator_state.get(nid, {}).get("message") or "unknown"
        fail_reason_counter[reason] += 1

    print("=== Narrators Rescrape Check ===")
    print(f"Page range: {args.start_page}..{args.end_page}")
    print(f"Expected pages in range: {book_stats['expected_pages']}")
    print(f"Seen pages in JSONL:      {book_stats['seen_pages']}")
    print(f"Success pages:            {book_stats['success_pages']}")
    print(f"Failed pages:             {book_stats['failed_pages']}")
    print(f"Missing pages in range:   {book_stats['missing_pages']}")

    if book_stats["failed_reason_counter"]:
        print("\nFailed page reasons:")
        for reason, count in book_stats["failed_reason_counter"].most_common():
            print(f"- {reason}: {count}")

    print("\nNarrator scrape status (from narrators mentioned in successful pages):")
    print(f"Unique narrator IDs in range: {len(target_narrator_ids)}")
    print(f"Scraped success:              {len(success_ids)}")
    print(f"Failed scraped:               {len(failed_ids)}")
    print(f"Not scraped yet:              {len(not_scraped_ids)}")

    if fail_reason_counter:
        print("\nFailed narrator reasons:")
        for reason, count in fail_reason_counter.most_common():
            print(f"- {reason}: {count}")

    limit = max(args.show_limit, 0)
    if failed_ids:
        failed_sorted = sort_ids(failed_ids)
        shown = failed_sorted[:limit] if limit else failed_sorted
        print(f"\nFailed narrator IDs ({len(failed_ids)}):")
        print(", ".join(shown))
        if limit and len(failed_sorted) > limit:
            print(f"... and {len(failed_sorted) - limit} more")

    if not_scraped_ids:
        missing_sorted = sort_ids(not_scraped_ids)
        shown = missing_sorted[:limit] if limit else missing_sorted
        print(f"\nNot-scraped narrator IDs ({len(not_scraped_ids)}):")
        print(", ".join(shown))
        if limit and len(missing_sorted) > limit:
            print(f"... and {len(missing_sorted) - limit} more")

    if args.rescrape_missing:
        to_rescrape = failed_ids | not_scraped_ids
        if not to_rescrape:
            print("\nNo failed/not-scraped narrator IDs to rescrape.")
            return

        from shamela_narrator_scraper import scrape_narrators

        print(f"\nStarting rescrape for {len(to_rescrape)} narrator IDs...")
        scrape_narrators(
            narrator_ids=to_rescrape,
            api_keys=API_KEYS,
            jsonl_output=args.narrators_jsonl,
            delay=args.delay,
            max_workers=args.max_workers,
        )

    if args.rescrape_hadith:
        rescrape_missing_hadith_pages(
            book_jsonl=args.book_jsonl,
            api_keys=API_KEYS,
            max_workers=args.max_workers,
            delay=args.delay,
        )


# ── Hadith rescrape logic ────────────────────────────────────────────────────

_key_lock = threading.Lock()


def _scrape_single_page(
    book_id: int,
    page_num: int,
    api_keys: List[str],
    key_state: dict,
    jsonl_path: Path,
    debug_dir: Optional[Path],
):
    """Scrape one page with API key rotation on 402. Returns outcome dict or None."""
    from shamela_firecrawl import scrape_with_firecrawl, append_jsonl

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
                    append_jsonl({"status": "failed", "book_id": book_id,
                                  "page_number": page_num, "url": url,
                                  "reason": "api_failure",
                                  "message": "All API keys exhausted (402)"}, jsonl_path)
                    return None

            new_key = key_state["key"]

        if key_state["index"] >= len(api_keys):
            append_jsonl({"status": "failed", "book_id": book_id,
                          "page_number": page_num, "url": url,
                          "reason": "api_failure",
                          "message": "All API keys exhausted (402)"}, jsonl_path)
            return None

        result = scrape_with_firecrawl(url, new_key)

    if result.success:
        append_jsonl({"status": "success", "book_id": book_id,
                      "page_number": page_num, **result.data}, jsonl_path)
        return {"success": True, "reason": None}
    else:
        append_jsonl({"status": "failed", "book_id": book_id,
                      "page_number": page_num, "url": url,
                      "reason": result.reason.value,
                      "message": result.message}, jsonl_path)
        if debug_dir and result.raw_html_snippet:
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / f"page_{page_num}_{result.reason.value}.html").write_text(
                result.raw_html_snippet, encoding="utf-8")
        return {"success": False, "reason": result.reason.value}


def rescrape_missing_hadith_pages(
    book_jsonl: Path,
    api_keys: List[str],
    max_workers: int = 2,
    delay: float = 3.0,
):
    """Load missing_hadith_index.json and rescrape candidate pages."""
    from shamela_firecrawl import load_scraped_pages, remove_failed_entries

    if not MISSING_INDEX_FILE.exists():
        print(f"\n{MISSING_INDEX_FILE.name} not found. Run list_missing_hadith_index.py first.")
        return

    with open(MISSING_INDEX_FILE, encoding="utf-8") as f:
        report = json.load(f)

    pages_to_scrape: List[int] = report.get("pages_to_scrape", [])
    if not pages_to_scrape:
        print("\nNo pages to scrape in missing_hadith_index.json.")
        return

    # Filter out already-successful pages
    success_pages = load_scraped_pages(book_jsonl)
    pages_to_scrape = [p for p in pages_to_scrape if p not in success_pages]

    if not pages_to_scrape:
        print("\nAll candidate pages already scraped successfully.")
        return

    # Remove old failed entries for pages we'll retry
    remove_failed_entries(book_jsonl, set(pages_to_scrape))

    debug_dir = BASE_DIR / f"debug_html_{BOOK_ID}_missing"
    key_state = {"index": 0, "key": api_keys[0]}

    print(f"\n{'='*60}")
    print(f"HADITH RESCRAPE: {len(pages_to_scrape)} pages")
    print(f"Using API key 1/{len(api_keys)}, workers={max_workers}, delay={delay}s")
    print(f"Output: {book_jsonl}")
    print(f"{'='*60}")

    succeeded = 0
    failed = 0
    keys_exhausted = False
    total = len(pages_to_scrape)

    for batch_start in range(0, total, max_workers):
        if keys_exhausted:
            break

        group = pages_to_scrape[batch_start:batch_start + max_workers]
        for p in group:
            print(f"\n[{batch_start + group.index(p) + 1}/{total}] Page {p}")

        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page_num in group:
                future = executor.submit(
                    _scrape_single_page, BOOK_ID, page_num, api_keys,
                    key_state, book_jsonl, debug_dir,
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
                    failed += 1

        if not keys_exhausted and batch_start + max_workers < total:
            time.sleep(delay)

    print(f"\n{'='*60}")
    print(f"HADITH RESCRAPE DONE")
    print(f"  Pages attempted : {total}")
    print(f"  Succeeded       : {succeeded}")
    print(f"  Failed          : {failed}")
    if keys_exhausted:
        print("  Stopped early: all API keys exhausted")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
