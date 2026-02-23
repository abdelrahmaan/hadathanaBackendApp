"""
Verify completeness of the Shamela book scrape.

Reads shamela_book_1681.jsonl and produces:
  hadith_verification_report_1681.json

The report contains:
  - Page-level: which pages succeeded, failed, or are missing entirely
  - pages_to_rescrape: sorted list of page numbers to feed back to the scraper
  - Hadith-index-level: which indices (1–7563) are present, missing, or duplicated

Usage:
    python extract_data_v2/firecrawl/verify_hadith_coverage.py

Configuration: edit the constants below.
"""

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Optional

# ── Configuration ──────────────────────────────────────────────────────────────
JSONL_PATH       = Path(__file__).parent / "shamela_book_1681.jsonl"
BOOK_ID          = 1681
PAGE_START       = 10
PAGE_END         = 11208
HADITH_IDX_START = 1
HADITH_IDX_END   = 7563

# Pages manually identified as containing missing hadith indices.
# When this list is empty it means all missing hadiths have been resolved.
# Add page numbers here whenever you manually identify a page that needs re-scraping.
MANUAL_RESCRAPE_PAGES = [
    81, 228, 301, 311, 415, 601, 707, 817, 1069, 1184,
    1244, 1678, 1729, 1946, 2087, 2375, 2409, 2442, 2586, 2664,
    2740, 2817, 3284, 3770, 3814, 4064, 4115, 4326, 4369, 4385,
    4445, 4467, 4679, 4685, 4797, 4804, 4862, 5187, 5315, 5349,
    5387, 5595, 5707, 6042, 6226, 6262, 6324, 6470, 6494, 6585,
    7023, 7040, 7070, 7608, 7634, 7700, 7864, 7903, 8338, 8521,
    8685, 8774, 8846, 8955, 9031, 9103, 9334, 9378, 9829, 9901,
    10286, 10376, 10513, 10734, 11119, 11179,
]
# ───────────────────────────────────────────────────────────────────────────────

_ARABIC_TO_INT = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
# Matches an Arabic or Western numeral sequence followed by optional matn marker and a dash
# Searches anywhere in text to handle cases where a chapter header precedes the hadith number
_IDX_RE = re.compile(r"(?<![٠-٩\d])([\u0660-\u0669\u0030-\u0039]+)\s*(?:\([^)]*\)\s*)?[-–—]")


def _extract_all_indices(text: str, hadith_number: Optional[int] = None) -> list:
    """
    Extract ALL hadith serial numbers from a block's full_text.
    A single block can contain multiple hadiths (e.g. '٢٧٢ - ... ٢٧٣ - ...').

    Priority for first index:
    1. hadith_number from fld_specialNum_top (page-level, most reliable)
    2. All Arabic/Western numerals before a dash found anywhere in full_text
    """
    if not text:
        return [hadith_number] if hadith_number is not None else []

    results = []
    for m in _IDX_RE.finditer(text.strip()):
        try:
            n = int(m.group(1).translate(_ARABIC_TO_INT))
            if HADITH_IDX_START <= n <= HADITH_IDX_END:
                results.append(n)
        except ValueError:
            pass

    # If regex found nothing but we have the page-level hadith_number, use it
    if not results and hadith_number is not None:
        return [hadith_number]

    return results


def main():
    if not JSONL_PATH.exists():
        print(f"ERROR: file not found: {JSONL_PATH}")
        return

    # ── Scan ────────────────────────────────────────────────────────────────────
    # Track the final (last-seen) status and reason per page number.
    # This correctly handles pages that appear multiple times in the file
    # (e.g. failed once, then re-scraped successfully, or vice-versa).
    page_final_status: dict[int, str] = {}   # page → "success" | "failed"
    page_final_reason: dict[int, str] = {}   # page → reason string (for failed pages)

    # idx_int → list of page numbers where it appeared
    idx_to_pages: dict[int, list[int]] = defaultdict(list)

    total_blocks = 0
    blocks_without_idx = 0
    pages_with_no_idx: list[int] = []

    print(f"Scanning {JSONL_PATH} …")

    with open(JSONL_PATH, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"  [WARN] line {lineno}: JSON parse error — {e}")
                continue

            page = obj.get("page_number")
            status = obj.get("status")
            reason = obj.get("reason", "")

            # Always update to the latest entry seen for this page
            page_final_status[page] = status
            if status == "failed":
                page_final_reason[page] = reason
            elif status == "success":
                # Remove stale failed reason if this page eventually succeeded
                page_final_reason.pop(page, None)

            if status == "success":
                blocks = obj.get("hadith_blocks") or []
                page_hadith_number = obj.get("hadith_number")  # from fld_specialNum_top
                page_has_any_idx = False
                for block in blocks:
                    total_blocks += 1
                    indices = _extract_all_indices(block.get("full_text") or "", page_hadith_number)
                    if not indices:
                        blocks_without_idx += 1
                    else:
                        page_has_any_idx = True
                        for idx in indices:
                            idx_to_pages[idx].append(page)

                if blocks and not page_has_any_idx:
                    pages_with_no_idx.append(page)

    # ── Page analysis ───────────────────────────────────────────────────────────
    pages_ok = {p for p, s in page_final_status.items() if s == "success"}
    pages_failed_all = {p for p, s in page_final_status.items() if s == "failed"}

    # Split failed pages by reason
    pages_failed_no_narrators = sorted(
        p for p in pages_failed_all
        if page_final_reason.get(p) == "no_narrators"
    )
    pages_failed_other = sorted(
        p for p in pages_failed_all
        if page_final_reason.get(p) != "no_narrators"
    )

    all_pages_in_range = set(range(PAGE_START, PAGE_END + 1))
    pages_missing_entirely = all_pages_in_range - set(page_final_status.keys())

    # Only re-scrape truly failed pages (not no_narrators) + missing pages
    pages_to_rescrape = sorted(set(pages_failed_other) | pages_missing_entirely)

    # ── Hadith index analysis ───────────────────────────────────────────────────
    found_indices = set(idx_to_pages.keys())
    expected_indices = set(range(HADITH_IDX_START, HADITH_IDX_END + 1))
    missing_indices = sorted(expected_indices - found_indices)

    duplicate_indices = {
        str(idx): pages
        for idx, pages in sorted(idx_to_pages.items())
        if len(pages) > 1
    }

    # ── Manual rescrape pages: filter out already-success pages ─────────────────
    # Pages still pending (not yet success in JSONL)
    manual_rescrape_pending = sorted(
        p for p in set(MANUAL_RESCRAPE_PAGES) if p not in pages_ok
    )
    # Empty list = all manually identified pages have been successfully scraped ✅

    # ── Build report ────────────────────────────────────────────────────────────
    report = {
        "book_id": BOOK_ID,
        "page_range": f"{PAGE_START}-{PAGE_END}",
        "hadith_range": f"{HADITH_IDX_START}-{HADITH_IDX_END}",
        "total_pages_in_range": len(all_pages_in_range),
        "pages_scraped_ok": len(pages_ok),
        "pages_failed_count": len(pages_failed_all),
        "pages_failed_no_narrators_count": len(pages_failed_no_narrators),
        "pages_failed_other_count": len(pages_failed_other),
        "pages_missing_entirely_count": len(pages_missing_entirely),
        # pages_to_rescrape = only truly failed (non-no_narrators) + missing
        "pages_to_rescrape": pages_to_rescrape,
        # no_narrators pages: content existed but had no narrator links — legitimate skip
        "pages_to_rescrape_no_narrators": pages_failed_no_narrators,
        "total_hadith_blocks": total_blocks,
        "blocks_with_idx": total_blocks - blocks_without_idx,
        "blocks_without_idx": blocks_without_idx,
        "pages_with_no_idx_extracted": sorted(pages_with_no_idx),
        "missing_hadith_indices": missing_indices,
        "duplicate_hadith_indices": duplicate_indices,
        # Manually identified pages that contain missing hadith indices.
        # Empty = all resolved ✅. Add new pages to MANUAL_RESCRAPE_PAGES above.
        "manual_rescrape_pages": manual_rescrape_pending,
    }

    report_path = JSONL_PATH.parent / f"hadith_verification_report_{BOOK_ID}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # ── Console summary ─────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("COVERAGE REPORT")
    print("=" * 60)
    print(f"Pages in range [{PAGE_START}–{PAGE_END}]:  {len(all_pages_in_range)}")
    print(f"  Scraped OK:               {len(pages_ok)}")
    print(f"  Failed (total):           {len(pages_failed_all)}")
    print(f"    ├─ no_narrators (skip): {len(pages_failed_no_narrators)}")
    print(f"    └─ other failures:      {len(pages_failed_other)}")
    print(f"  Missing entirely:         {len(pages_missing_entirely)}")
    print(f"  → Pages to re-scrape:     {len(pages_to_rescrape)}  (other failures + missing)")
    print()
    print(f"Hadith indices [{HADITH_IDX_START}–{HADITH_IDX_END}]:")
    print(f"  Found:                    {len(found_indices)}")
    print(f"  Missing:                  {len(missing_indices)}")
    print(f"  Duplicated:               {len(duplicate_indices)}")
    print()
    print(f"Hadith blocks total:        {total_blocks}")
    print(f"  Without index extracted:  {blocks_without_idx}")
    print(f"  Pages with no index:      {len(pages_with_no_idx)}")
    print()
    if manual_rescrape_pending:
        print(f"Manual rescrape pages:      {len(manual_rescrape_pending)} still pending")
        print(f"  {manual_rescrape_pending[:20]}{'...' if len(manual_rescrape_pending) > 20 else ''}")
    else:
        print("Manual rescrape pages:      ✅ all resolved (empty)")
    print()

    if missing_indices:
        preview = missing_indices[:20]
        suffix = f" … (+{len(missing_indices) - 20} more)" if len(missing_indices) > 20 else ""
        print(f"Missing indices (first 20): {preview}{suffix}")

    if pages_to_rescrape:
        preview = pages_to_rescrape[:20]
        suffix = f" … (+{len(pages_to_rescrape) - 20} more)" if len(pages_to_rescrape) > 20 else ""
        print(f"Pages to re-scrape (first 20): {preview}{suffix}")

    print()
    print(f"Full report saved → {report_path}")


if __name__ == "__main__":
    main()
