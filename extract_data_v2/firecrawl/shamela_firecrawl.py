import re
import json
import time
import random
import requests
import threading
from pathlib import Path
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from dataclasses import dataclass
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed


class SkipReason(Enum):
    API_FAILURE = "api_failure"
    EMPTY_HTML = "empty_html"
    NO_HADITH_BLOCKS = "no_hadith_blocks"
    NO_NARRATORS = "no_narrators"
    SUCCESS = "success"


@dataclass
class ScrapeResult:
    success: bool
    data: Optional[dict] = None
    reason: SkipReason = SkipReason.SUCCESS
    message: str = ""
    raw_html_snippet: str = ""

NARRATOR_RE = re.compile(r"/narrator/(\d+)")
WS_RE = re.compile(r"\s+")

RETRYABLE_FIRECRAWL_HTTP_CODES = {"408", "409", "425", "429", "500", "502", "503", "504"}
RETRYABLE_TARGET_CODES = {
    "408", "409", "423", "425", "429",
    "500", "502", "503", "504",
    "520", "521", "522", "523", "524", "525", "526", "530",
}
CLOUDFLARE_ERROR_MARKERS = [
    "cf-browser-verification",
    "challenge-platform",
    "just a moment",
    "checking your browser",
    "id=\"cf-wrapper\"",
    "id=\"cf-error-details\"",
    "error code 520",
    "error code 522",
    "cloudflare",
]


def _normalize_status_code(value) -> str:
    """Normalize status code to string for consistent comparisons."""
    if value is None:
        return ""
    return str(value).strip()


def _compute_backoff_seconds(attempt: int, base: float = 5.0, cap: float = 45.0) -> float:
    """Linear backoff with small jitter to avoid synchronized retries."""
    linear = base * (attempt + 1)
    jitter = random.uniform(0.0, 1.5)
    return min(cap, linear + jitter)


def _is_cloudflare_error_page(html: str) -> bool:
    """Detect Cloudflare challenge/error pages returned instead of target content."""
    lowered = (html or "").lower()
    return any(marker in lowered for marker in CLOUDFLARE_ERROR_MARKERS)

def norm(text: str) -> str:
    return WS_RE.sub(" ", (text or "")).strip()

def extract_special_num(soup: BeautifulSoup) -> Optional[int]:
    """
    Extract the hadith serial number from the page's specialNum input field.
    Shamela renders: <input id="fld_specialNum_top" value="62" ...>
    Returns the integer value, or None if not found.
    """
    el = soup.find("input", id="fld_specialNum_top")
    if el:
        try:
            return int(el.get("value", "").strip())
        except (ValueError, AttributeError):
            pass
    return None

def extract_breadcrumb(soup: BeautifulSoup) -> list:
    """
    Extract breadcrumb links: list of {text, href}
    """
    # 1) Find the exact label string on the page
    label_node = soup.find(string=lambda s: isinstance(s, str) and "مسار الصفحة الحالية" in s)
    if label_node:
        container = label_node.parent
        for _ in range(3):
            links = container.find_all("a", href=True)
            if links:
                return [{"text": norm(a.get_text(" ", strip=True)), "href": a["href"]} for a in links]
            container = container.parent

    # 2) Fallback: common breadcrumb classes/ids
    bc = soup.select_one(".breadcrumb, .breadcrumbs, #breadcrumb, .path, .navpath")
    if bc:
        links = bc.find_all("a", href=True)
        return [{"text": norm(a.get_text(" ", strip=True)), "href": a["href"]} for a in links]

    return []

def extract_hadith_and_narrators(soup: BeautifulSoup) -> list:
    """
    Extract hadith blocks from the page.
    Returns list of hadith block dicts.
    """
    blocks = soup.select("div.nass.margin-top-10, div.nass")
    results = []

    for block in blocks:
        # Narrators (IDs + names)
        narrators = []
        for a in block.select("a[href*='/narrator/']"):
            href = a.get("href", "")
            m = NARRATOR_RE.search(href)
            if not m:
                continue
            narrators.append({
                "id": m.group(1),
                "name": norm(a.get_text(" ", strip=True)),
                "url": href
            })

        # Matn often inside span.c2
        matn_el = block.select_one("span.c2")
        matn = norm(matn_el.get_text(" ", strip=True)) if matn_el else ""

        # Clean full text: remove UI garbage (copy button, icons, anchors)
        block_clean = BeautifulSoup(str(block), "html.parser")
        for el in block_clean.select("a.btn_tag, span.fa, span.anchor"):
            el.decompose()

        full_text = norm(block_clean.get_text(" ", strip=True))

        results.append({
            "full_text": full_text,
            "matn": matn,
            "narrators": narrators
        })

    return results

def has_narrator_data(hadith_blocks: list) -> bool:
    """Check if at least one hadith block has narrators."""
    for block in hadith_blocks:
        if block.get("narrators") and len(block["narrators"]) > 0:
            return True
    return False

def scrape_with_firecrawl(url: str, api_key: str, max_retries: int = 3) -> ScrapeResult:
    """
    Scrape a URL using Firecrawl API to bypass Cloudflare protection.
    Returns ScrapeResult with success/failure details for every call.
    """
    firecrawl_url = "https://api.firecrawl.dev/v2/scrape"

    payload = {
        "url": url,
        "onlyMainContent": False,
        "formats": ["html"]
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    html = ""
    special_num = None

    for attempt in range(max_retries):
        try:
            print(f"  Firecrawl request: {url} (attempt {attempt + 1}/{max_retries})")
            response = requests.post(firecrawl_url, json=payload, headers=headers, timeout=120)

            # Handle HTTP-level errors from Firecrawl API itself
            if response.status_code != 200:
                http_code = response.status_code
                print(f"  Firecrawl HTTP {http_code}")
                http_code_str = _normalize_status_code(http_code)
                if http_code_str in RETRYABLE_FIRECRAWL_HTTP_CODES and attempt < max_retries - 1:
                    wait_time = _compute_backoff_seconds(
                        attempt, base=15.0 if http_code_str == "429" else 5.0
                    )
                    print(f"  Retryable Firecrawl HTTP {http_code}, waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue
                return ScrapeResult(
                    success=False,
                    reason=SkipReason.API_FAILURE,
                    message=f"Firecrawl HTTP {http_code}",
                )

            result = response.json()
            firecrawl_success = result.get("success", False)
            metadata = result.get("data", {}).get("metadata", {})
            target_status = _normalize_status_code(metadata.get("statusCode"))
            html = result.get("data", {}).get("html", "")

            print(f"  Firecrawl response: success={firecrawl_success}, target_statusCode={target_status}")

            # Check if Firecrawl reported failure
            if not firecrawl_success:
                error_msg = result.get("error", "Unknown error")
                print(f"  FAILED: {error_msg}")

                error_str = str(error_msg).lower()
                is_retryable = ("timeout" in error_str or
                                any(code in str(error_msg) for code in RETRYABLE_TARGET_CODES))

                if is_retryable and attempt < max_retries - 1:
                    wait_time = _compute_backoff_seconds(
                        attempt, base=15.0 if "429" in str(error_msg) else 5.0
                    )
                    print(f"  Retryable Firecrawl error, waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue

                return ScrapeResult(
                    success=False,
                    reason=SkipReason.API_FAILURE,
                    message=f"Firecrawl failed: {error_msg}",
                )

            # Target responded with retryable upstream status (e.g., 520/522/524)
            if target_status in RETRYABLE_TARGET_CODES:
                if attempt < max_retries - 1:
                    wait_time = _compute_backoff_seconds(
                        attempt, base=15.0 if target_status == "429" else 6.0
                    )
                    print(f"  Target returned {target_status}; retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue
                return ScrapeResult(
                    success=False,
                    reason=SkipReason.API_FAILURE,
                    message=f"Target status {target_status} after {max_retries} attempts",
                    raw_html_snippet=html[:500],
                )

            if not html or len(html) < 100:
                print(f"  Empty or near-empty HTML ({len(html)} chars)")
                return ScrapeResult(
                    success=False,
                    reason=SkipReason.EMPTY_HTML,
                    message=f"Empty HTML ({len(html)} chars)",
                    raw_html_snippet=html[:500],
                )

            # Check for Cloudflare challenge/error page
            if _is_cloudflare_error_page(html):
                if attempt < max_retries - 1:
                    wait_time = _compute_backoff_seconds(attempt, base=10.0)
                    print(f"  Cloudflare error/challenge page detected, retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue
                return ScrapeResult(
                    success=False,
                    reason=SkipReason.API_FAILURE,
                    message="Cloudflare error/challenge page received",
                    raw_html_snippet=html[:500],
                )

            print(f"  HTML retrieved: {len(html)} chars")

            # Parse and check if page content actually loaded
            soup = BeautifulSoup(html, "html.parser")
            hadith_blocks = extract_hadith_and_narrators(soup)

            if not hadith_blocks:
                # Page likely didn't finish loading — retry
                any_nass = soup.select("div.nass")
                any_narrator_links = soup.select("a[href*='/narrator/']")

                # Avoid misclassifying Cloudflare error pages as selector/content failures
                if _is_cloudflare_error_page(html):
                    if attempt < max_retries - 1:
                        wait_time = _compute_backoff_seconds(attempt, base=10.0)
                        print(f"  Cloudflare page (no content), retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    return ScrapeResult(
                        success=False,
                        reason=SkipReason.API_FAILURE,
                        message=f"Cloudflare page after {max_retries} attempts",
                        raw_html_snippet=html[:500],
                    )

                if len(any_nass) == 0 and len(any_narrator_links) == 0:
                    if attempt < max_retries - 1:
                        wait_time = _compute_backoff_seconds(attempt, base=5.0)
                        print(f"  Page content not loaded (0 div.nass), retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    # All retries exhausted
                    msg = (f"No hadith blocks after {max_retries} attempts. "
                           f"div.nass count: 0, narrator links: 0")
                    print(f"  {msg}")
                    return ScrapeResult(
                        success=False,
                        reason=SkipReason.NO_HADITH_BLOCKS,
                        message=msg,
                        raw_html_snippet=html[:500],
                    )
                else:
                    # div.nass exists but no hadith extracted — legit empty or selector issue
                    msg = (f"No hadith blocks. div.nass count: {len(any_nass)}, "
                           f"narrator links in page: {len(any_narrator_links)}")
                    print(f"  {msg}")
                    return ScrapeResult(
                        success=False,
                        reason=SkipReason.NO_HADITH_BLOCKS,
                        message=msg,
                        raw_html_snippet=html[:500],
                    )

            # Got hadith blocks — exit retry loop
            breadcrumb_links = extract_breadcrumb(soup)
            special_num = extract_special_num(soup)
            break

        except requests.exceptions.Timeout:
            print(f"  Request timeout")
            if attempt < max_retries - 1:
                wait_time = _compute_backoff_seconds(attempt, base=5.0)
                print(f"  Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            return ScrapeResult(
                success=False,
                reason=SkipReason.API_FAILURE,
                message=f"Request timeout after {max_retries} attempts",
            )

        except requests.exceptions.RequestException as e:
            print(f"  Connection error: {e}")
            if attempt < max_retries - 1:
                wait_time = _compute_backoff_seconds(attempt, base=5.0)
                print(f"  Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            return ScrapeResult(
                success=False,
                reason=SkipReason.API_FAILURE,
                message=f"Connection error after {max_retries} attempts: {e}",
            )

    else:
        return ScrapeResult(
            success=False,
            reason=SkipReason.API_FAILURE,
            message=f"Failed after {max_retries} attempts",
        )

    # Blocks found but no narrators
    if not has_narrator_data(hadith_blocks):
        sample_text = hadith_blocks[0].get("full_text", "")[:100] if hadith_blocks else ""
        msg = f"{len(hadith_blocks)} hadith blocks but no narrator links"
        print(f"  {msg}. Sample: {sample_text}...")
        return ScrapeResult(
            success=False,
            reason=SkipReason.NO_NARRATORS,
            message=msg,
            raw_html_snippet=html[:500],
        )

    # Success
    narrator_count = sum(len(b.get("narrators", [])) for b in hadith_blocks)
    print(f"  OK: {len(hadith_blocks)} hadith blocks, {narrator_count} narrators")

    return ScrapeResult(
        success=True,
        data={
            "url": url,
            "breadcrumb_links": breadcrumb_links,
            "hadith_number": special_num,
            "hadith_blocks": hadith_blocks,
        },
    )

def load_scraped_pages(jsonl_path: Path) -> set:
    """
    Load only SUCCESS page numbers from JSONL file.
    All failed pages (including no_narrators) will be retried — some no_narrators
    results are Cloudflare partial loads misclassified as content-without-narrators.
    """
    scraped = set()
    if not jsonl_path.exists():
        return scraped
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            status = obj.get("status")
            if status == "success":
                scraped.add(obj["page_number"])
    return scraped

def load_tracker(tracker_path: Path) -> Dict[int, dict]:
    """Load failed_pages_tracker JSON. Keys are int page numbers."""
    if not tracker_path.exists():
        return {}
    with open(tracker_path, encoding="utf-8") as f:
        raw = json.load(f)
    return {int(k): v for k, v in raw.items()}

def save_tracker(tracker: Dict[int, dict], tracker_path: Path):
    """Save tracker back to disk (thread-unsafe — call only from main thread)."""
    with open(tracker_path, "w", encoding="utf-8") as f:
        json.dump({str(k): v for k, v in tracker.items()}, f, ensure_ascii=False, indent=2)

def get_pages_from_tracker(tracker: Dict[int, dict], max_rerun: int) -> List[int]:
    """
    Return pages to scrape this run, sorted by n_rerun ascending (fewest first).
    - n_rerun=0  → never tried → always included
    - 0 < n_rerun < max_rerun → retry
    - n_rerun >= max_rerun → skip (gave up)
    - reason='api_failure' with HTTP 401 → treated like permanent failure (skip after max_rerun)
    """
    retryable = []
    for page, info in tracker.items():
        n = info.get("n_rerun", 0)
        if n < max_rerun:
            retryable.append((page, n))
    retryable.sort(key=lambda x: x[1])
    return [page for page, _ in retryable]

def update_tracker_after_run(tracker: Dict[int, dict], page: int,
                              success: bool, reason: str = "", message: str = ""):
    """Update a single page entry in the tracker after a scrape attempt."""
    if success:
        # Remove from tracker — page is done
        tracker.pop(page, None)
    else:
        prev = tracker.get(page, {})
        tracker[page] = {
            "n_rerun": prev.get("n_rerun", 0) + 1,
            "reason": reason,
            "message": message,
        }

def remove_failed_entries(jsonl_path: Path, pages_to_retry: set) -> Dict[int, int]:
    """Remove old failed entries for pages that will be retried."""
    if not jsonl_path.exists() or not pages_to_retry:
        return {}
    kept_lines = []
    removed = 0
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            obj = json.loads(stripped)
            page = obj.get("page_number")
            if page in pages_to_retry and obj.get("status") == "failed":
                removed += 1
            else:
                kept_lines.append(line.rstrip('\n'))
    if removed > 0:
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for line in kept_lines:
                f.write(line + '\n')
        print(f"Cleaned {removed} old failed entries from JSONL")

# Lock for thread-safe file writes and key rotation
_file_lock = threading.Lock()
_key_lock = threading.Lock()

def append_jsonl(obj: dict, jsonl_path: Path):
    """Append a single JSON object as a line to a JSONL file (thread-safe)."""
    with _file_lock:
        with open(jsonl_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')

def _scrape_page(book_id: int, page_num: int, api_keys: List[str], key_state: dict,
                 jsonl_output: Path, debug_dir: Optional[Path]) -> Optional[dict]:
    """
    Scrape a single page. Handles 402 key rotation (thread-safe).
    Returns {"success": bool, "reason": str, "message": str} or None if all keys exhausted.
    key_state = {"index": int, "key": str} — mutated in place on rotation.
    """
    url = f"https://shamela.ws/book/{book_id}/{page_num}"

    # Get current key (thread-safe read)
    with _key_lock:
        current_key = key_state["key"]

    result = scrape_with_firecrawl(url, current_key)

    # 402 = quota exhausted -> rotate API key and retry same page
    if not result.success and "402" in result.message:
        with _key_lock:
            # Check if another thread already rotated the key
            if key_state["key"] == current_key:
                key_state["index"] += 1
                if key_state["index"] < len(api_keys):
                    key_state["key"] = api_keys[key_state["index"]]
                    print(f"\n  ** API key quota exhausted. Switching to key {key_state['index'] + 1}/{len(api_keys)} **")
                else:
                    print(f"\n  ** All {len(api_keys)} API keys exhausted! Stopping. **")
                    msg = "All API keys exhausted (402)"
                    append_jsonl({"status": "failed", "book_id": book_id,
                                  "page_number": page_num, "url": url,
                                  "reason": "api_failure", "message": msg}, jsonl_output)
                    return None  # Signal to stop

            # Retry with new key (could be rotated by this thread or another)
            new_key = key_state["key"]

        if key_state["index"] >= len(api_keys):
            msg = "All API keys exhausted (402)"
            append_jsonl({"status": "failed", "book_id": book_id,
                          "page_number": page_num, "url": url,
                          "reason": "api_failure", "message": msg}, jsonl_output)
            return None

        result = scrape_with_firecrawl(url, new_key)

    if result.success:
        append_jsonl({"status": "success", "book_id": book_id,
                      "page_number": page_num, **result.data}, jsonl_output)
        return {"success": True, "reason": "", "message": ""}
    else:
        append_jsonl({"status": "failed", "book_id": book_id,
                      "page_number": page_num, "url": url,
                      "reason": result.reason.value, "message": result.message}, jsonl_output)

        if debug_dir and result.raw_html_snippet:
            debug_file = debug_dir / f"page_{page_num}_{result.reason.value}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(result.raw_html_snippet)

        return {"success": False, "reason": result.reason.value, "message": result.message}

def _categorize_failure(outcome: dict, api_failures, empty_pages, no_block_pages,
                        no_narrator_pages, page_num: int):
    """Add a failure entry to the appropriate category list."""
    reason = outcome["reason"]
    entry = {"page": page_num, "reason": reason}
    if reason == SkipReason.API_FAILURE.value:
        api_failures.append(entry)
    elif reason == SkipReason.EMPTY_HTML.value:
        empty_pages.append(entry)
    elif reason == SkipReason.NO_HADITH_BLOCKS.value:
        no_block_pages.append(entry)
    elif reason == SkipReason.NO_NARRATORS.value:
        no_narrator_pages.append(entry)

def _process_batch(batch: List[int], book_id: int, api_keys: List[str], key_state: dict,
                   jsonl_output: Path, debug_dir: Optional[Path], max_workers: int,
                   delay: float, label: str, start_idx: int, total: int,
                   api_failures, empty_pages, no_block_pages, no_narrator_pages, error_pages,
                   tracker: Optional[Dict[int, dict]] = None):
    """
    Process a list of page numbers using concurrent workers.
    Returns (newly_scraped, keys_exhausted).
    tracker: the shared failed_pages_tracker dict — updated in place after each result.
    """
    newly_scraped = 0
    keys_exhausted = False
    if tracker is None:
        tracker = {}

    # Process pages in groups of max_workers
    for batch_start in range(0, len(batch), max_workers):
        if keys_exhausted:
            break

        group = batch[batch_start:batch_start + max_workers]
        group_idx = start_idx + batch_start

        for p in group:
            n = tracker.get(p, {}).get("n_rerun", 0)
            print(f"\n[{label} {group_idx + group.index(p) + 1}/{total}] Page {p} (n_rerun={n}→{n+1})")

        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page_num in group:
                future = executor.submit(
                    _scrape_page, book_id, page_num, api_keys, key_state,
                    jsonl_output, debug_dir
                )
                futures[future] = page_num

            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    outcome = future.result()
                    if outcome is None:
                        keys_exhausted = True
                        update_tracker_after_run(tracker, page_num, False,
                                                 "api_failure", "All API keys exhausted")
                    elif outcome["success"]:
                        newly_scraped += 1
                        update_tracker_after_run(tracker, page_num, True)
                    else:
                        update_tracker_after_run(tracker, page_num, False,
                                                 outcome["reason"], outcome["message"])
                        _categorize_failure(outcome, api_failures, empty_pages,
                                          no_block_pages, no_narrator_pages, page_num)
                except Exception as e:
                    print(f"  UNEXPECTED ERROR on page {page_num}: {e}")
                    append_jsonl({"status": "failed", "book_id": book_id,
                                  "page_number": page_num,
                                  "url": f"https://shamela.ws/book/{book_id}/{page_num}",
                                  "reason": "unexpected_error", "message": str(e)}, jsonl_output)
                    update_tracker_after_run(tracker, page_num, False, "unexpected_error", str(e))
                    error_pages.append({"page": page_num, "reason": str(e)})

        # Delay between batches (not between individual requests within a batch)
        if not keys_exhausted and batch_start + max_workers < len(batch):
            time.sleep(delay)

    return newly_scraped, keys_exhausted

def scrape_book_pages(book_id: int, start_page: int, end_page: int, api_keys: List[str],
                      jsonl_output: Path, delay: float = 1.0,
                      debug_dir: Optional[Path] = None,
                      max_workers: int = 2,
                      max_rerun: int = 3) -> int:
    """
    Scrape multiple pages from a Shamela book with concurrent requests.
    1) First retries all previously failed pages (removes old entries, re-scrapes).
    2) Then continues with new pages from where it left off.
    On 402 (quota exhausted), rotates to the next API key.
    max_workers controls concurrency (default 2 for Firecrawl free tier).
    """
    if debug_dir:
        debug_dir.mkdir(parents=True, exist_ok=True)
        print(f"Debug mode: saving problematic HTML to {debug_dir}")

    # API key rotation state (shared across retry + new pages)
    key_state = {"index": 0, "key": api_keys[0]}
    print(f"Using API key {key_state['index'] + 1}/{len(api_keys)}")
    print(f"Concurrent workers: {max_workers}")
    print(f"Max reruns per page: {max_rerun}")

    # ── Load tracker ──
    tracker_path = jsonl_output.parent / f"failed_pages_tracker_{book_id}.json"
    tracker = load_tracker(tracker_path)
    print(f"Tracker loaded: {len(tracker)} pages pending")

    # Categorized failure tracking
    api_failures = []
    empty_pages = []
    no_block_pages = []
    no_narrator_pages = []
    error_pages = []
    newly_scraped = 0
    keys_exhausted = False

    # ── Get pages to scrape this run (sorted by n_rerun ascending) ──
    pages_to_run = get_pages_from_tracker(tracker, max_rerun=max_rerun)
    skipped = len(tracker) - len(pages_to_run)

    print(f"Pages to scrape this run: {len(pages_to_run)} "
          f"(skipped {skipped} that reached max_rerun={max_rerun})")

    if pages_to_run:
        # Remove ALL old failed JSONL entries for every page we're about to scrape
        remove_failed_entries(jsonl_output, set(pages_to_run))

        n0 = sum(1 for p in pages_to_run if tracker.get(p, {}).get("n_rerun", 0) == 0)
        n_retry = len(pages_to_run) - n0
        print(f"\n{'='*60}")
        print(f"Scraping: {n0} new pages + {n_retry} retries (sorted fewest-attempts-first)")
        print(f"{'='*60}")

        newly_scraped, keys_exhausted = _process_batch(
            pages_to_run, book_id, api_keys, key_state, jsonl_output, debug_dir,
            max_workers, delay, "", 0, len(pages_to_run),
            api_failures, empty_pages, no_block_pages, no_narrator_pages, error_pages,
            tracker=tracker,
        )
    else:
        print("\nNothing to scrape — all pages either done or gave up after max_rerun.")

    # ── Save tracker after run ──
    save_tracker(tracker, tracker_path)
    print(f"Tracker saved: {len(tracker)} pages still pending")

    # Print detailed summary
    print(f"\n{'='*60}")
    print(f"SCRAPING SUMMARY")
    print(f"{'='*60}")
    total_pages = end_page - start_page + 1
    final_scraped = load_scraped_pages(jsonl_output)
    print(f"Total pages in range:       {total_pages}")
    print(f"Total done (success/skip):  {len(final_scraped)}")
    print(f"Successfully scraped:       {newly_scraped}")
    print(f"{'='*60}")
    print(f"FAILURES:")
    print(f"  API failures:             {len(api_failures)}")
    print(f"  Empty/blocked pages:      {len(empty_pages)}")
    print(f"  No hadith blocks:         {len(no_block_pages)}")
    print(f"  No narrator links:        {len(no_narrator_pages)}")
    print(f"  Unexpected errors:        {len(error_pages)}")
    print(f"{'='*60}")

    if api_failures:
        print(f"\nAPI failures (first 5):")
        for f in api_failures[:5]:
            print(f"  Page {f['page']}: {f['reason']}")

    if empty_pages:
        print(f"\nEmpty/blocked pages (first 5):")
        for f in empty_pages[:5]:
            print(f"  Page {f['page']}: {f['reason']}")

    if no_block_pages:
        print(f"\nNo hadith blocks (first 5) -- possible CSS selector issue:")
        for f in no_block_pages[:5]:
            print(f"  Page {f['page']}: {f['reason']}")

    if no_narrator_pages:
        print(f"\nHadith blocks but no narrators (first 5):")
        for f in no_narrator_pages[:5]:
            print(f"  Page {f['page']}: {f['reason']}")

    if error_pages:
        print(f"\nUnexpected errors:")
        for f in error_pages:
            print(f"  Page {f['page']}: {f['reason']}")

    # Save failure report
    failure_report = {
        "scrape_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "book_id": book_id,
        "page_range": f"{start_page}-{end_page}",
        "api_failures": api_failures,
        "empty_pages": empty_pages,
        "no_block_pages": no_block_pages,
        "no_narrator_pages": no_narrator_pages,
        "error_pages": error_pages,
    }
    report_path = jsonl_output.parent / f"failure_report_{book_id}.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(failure_report, f, ensure_ascii=False, indent=2)
    print(f"\nFailure report saved to: {report_path}")

    return newly_scraped

def _run_phase0_manual_rescrape(report_path: Path, jsonl_output: Path,
                                 api_keys: List[str], delay: float, max_workers: int):
    """
    Phase 0: Read manual_rescrape_pages from the verification report,
    skip pages already successful in the hadith JSONL, and scrape the rest.
    Returns the count of newly scraped pages.
    """
    if not report_path.exists():
        print(f"Phase 0: Report not found ({report_path}) — skipping.")
        return 0

    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    manual_pages: List[int] = report.get("manual_rescrape_pages", [])

    print(f"\n{'='*60}")
    print(f"PHASE 0: Manual hadith page rescrape")
    print(f"{'='*60}")

    if not manual_pages:
        print("  manual_rescrape_pages is empty ✅ — nothing to do.")
        return 0

    # Find which pages are already success in JSONL
    success_pages = load_scraped_pages(jsonl_output)
    to_scrape = [p for p in manual_pages if p not in success_pages]

    print(f"  Total manual pages:    {len(manual_pages)}")
    print(f"  Already success:       {len(manual_pages) - len(to_scrape)}")
    print(f"  To scrape this run:    {len(to_scrape)}")

    if not to_scrape:
        print("  All manual pages already scraped ✅")
        return 0

    book_id = report.get("book_id", 1681)
    remove_failed_entries(jsonl_output, set(to_scrape))

    key_state = {"index": 0, "key": api_keys[0]}
    newly_scraped = 0
    keys_exhausted = False

    for batch_start in range(0, len(to_scrape), max_workers):
        if keys_exhausted:
            break

        group = to_scrape[batch_start:batch_start + max_workers]
        for p in group:
            print(f"\n[Phase0 {batch_start + group.index(p) + 1}/{len(to_scrape)}] Page {p}")

        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page_num in group:
                future = executor.submit(
                    _scrape_page, book_id, page_num, api_keys, key_state,
                    jsonl_output, None
                )
                futures[future] = page_num

            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    outcome = future.result()
                    if outcome is None:
                        keys_exhausted = True
                        print(f"  ** All API keys exhausted — stopping Phase 0 **")
                    elif outcome["success"]:
                        newly_scraped += 1
                    else:
                        print(f"  Page {page_num} failed: {outcome['message']}")
                except Exception as e:
                    print(f"  UNEXPECTED ERROR page {page_num}: {e}")

        if not keys_exhausted and batch_start + max_workers < len(to_scrape):
            time.sleep(delay)

    print(f"\nPhase 0 done: {newly_scraped}/{len(to_scrape)} pages scraped")
    return newly_scraped


if __name__ == "__main__":
    import subprocess
    import sys

    # Firecrawl API keys (rotates to next on 402 quota exhausted)
    API_KEYS = [
        "fc-9dfc24f3a5314143b9ff520dde949d30",
        "fc-3586edaed4b4435581f85bbd525a8099",
        "fc-9c2bdb32b1b24e6db10405e4056238ad",
        "fc-4bb4e11e032e485b8116279e68cc2142",

    ]

    # Configuration
    BOOK_ID = 1681
    START_PAGE = 10
    END_PAGE = 11208

    # Delay between batches (in seconds)
    DELAY_SECONDS = 3.0

    # Concurrent requests (Firecrawl free tier allows 2)
    MAX_WORKERS = 2

    # Max times to retry a failed page before giving up (per reason, except api_failure)
    MAX_RERUN = 3

    # Debug directory for saving HTML of failed pages (set to None to disable)
    DEBUG_DIR = Path(__file__).parent / f"debug_html_{BOOK_ID}"

    # Output paths
    jsonl_output = Path(__file__).parent / f"shamela_book_{BOOK_ID}.jsonl"
    REPORT_FILE  = Path(__file__).parent / f"hadith_verification_report_{BOOK_ID}.json"
    NARRATOR_SCRAPER = Path(__file__).parent / "shamela_narrator_scraper.py"

    print(f"Starting scrape for book {BOOK_ID}, pages {START_PAGE} to {END_PAGE}")
    print(f"Delay between batches: {DELAY_SECONDS}s")
    print(f"Concurrent workers: {MAX_WORKERS}")
    print(f"API keys available: {len(API_KEYS)}")

    # ── Phase 0: Rescrape manually identified hadith pages ──────────────────────
    try:
        _run_phase0_manual_rescrape(
            report_path=REPORT_FILE,
            jsonl_output=jsonl_output,
            api_keys=API_KEYS,
            delay=DELAY_SECONDS,
            max_workers=MAX_WORKERS,
        )
    except KeyboardInterrupt:
        print("\n\nPhase 0 interrupted by user!")
        print("Data has been saved up to the last successful page.")
        sys.exit(0)

    # ── Main scrape ──────────────────────────────────────────────────────────────
    try:
        newly_scraped = scrape_book_pages(
            book_id=BOOK_ID,
            start_page=START_PAGE,
            end_page=END_PAGE,
            api_keys=API_KEYS,
            jsonl_output=jsonl_output,
            delay=DELAY_SECONDS,
            debug_dir=DEBUG_DIR,
            max_workers=MAX_WORKERS,
            max_rerun=MAX_RERUN,
        )

        print(f"\nData saved to: {jsonl_output}")

    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user!")
        print("Data has been saved up to the last successful page.")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # ── Auto-run narrator scraper ────────────────────────────────────────────────
    if NARRATOR_SCRAPER.exists():
        print(f"\n{'='*60}")
        print(f"Launching narrator scraper: {NARRATOR_SCRAPER.name}")
        print(f"{'='*60}")
        try:
            subprocess.run([sys.executable, str(NARRATOR_SCRAPER)], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Narrator scraper exited with error code {e.returncode}")
        except KeyboardInterrupt:
            print("\n\nNarrator scraper interrupted by user!")
    else:
        print(f"\nNarrator scraper not found at {NARRATOR_SCRAPER} — skipping.")
