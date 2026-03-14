import json
import re
from bisect import bisect_left

INPUT_HADITH_FILE = "shamela_book_1681.jsonl"
OUTPUT_FILE = "missing_hadith_index.json"
BOOK_ID = 1681

ARABIC_INDIC = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# Arabic tashkeel Unicode ranges (strip before searching for index patterns)
TASHKEEL = re.compile(
    "[\u0610-\u061A"
    "\u064B-\u065F"
    "\u0670"
    "\u06D6-\u06DC"
    "\u06DF-\u06E4"
    "\u06E7\u06E8"
    "\u06EA-\u06ED"
    "]"
)

def to_int(s):
    return int(s.translate(ARABIC_INDIC))

def parse_indices(full_text):
    """
    Extract ALL hadith indices from full_text by scanning the entire text.
    A hadith index is an Arabic/ASCII number followed by a dash, e.g.:
      '٤٠٨ - ٤٠٩ - حَدَّثَنَا'               -> [408, 409]
      'بَابُ حُسْنِ... ٤١ - قَالَ مَالِكٌ'  -> [41]
      '٨٩٦ - ... text ... ٨٩٧ - ... ٨٩٨ -'   -> [896, 897, 898]
    Scans the entire full_text for all 'number -' patterns (embedded indices).
    """
    plain = TASHKEEL.sub("", full_text)
    parts = re.findall(r"[٠-٩\d]+(?=\s*[-–—])", plain)
    if not parts:
        return []
    return [to_int(p) for p in parts]

# --- Parse hadith blocks: collect found indices and build idx->page map ---
found_indices = set()
idx_to_page = {}   # hadith_index -> page_number
all_pages = {}     # page_number -> status ("success" / "failed")

with open(INPUT_HADITH_FILE, encoding="utf-8") as f:
    for line in f:
        rec = json.loads(line)
        page = rec.get("page_number")
        status = rec.get("status", "")
        all_pages[page] = status

        for block in rec.get("hadith_blocks", []):
            full_text = block.get("full_text", "")
            for idx in parse_indices(full_text):
                found_indices.add(idx)
                if status == "success":
                    idx_to_page[idx] = page

# --- Compute gaps ---
all_indices = sorted(found_indices)
sorted_existing = sorted(idx_to_page.keys())
min_idx, max_idx = all_indices[0], all_indices[-1]
missing_indices = sorted(set(range(min_idx, max_idx + 1)) - found_indices)

print(f"Range   : {min_idx} – {max_idx}")
print(f"Found   : {len(all_indices)}")
print(f"Missing : {len(missing_indices)}")

# --- For each missing index, find candidate pages to scrape ---
pages_to_scrape = set()
missing_records = []
resolvable = 0
unresolved = 0

for idx in missing_indices:
    pos = bisect_left(sorted_existing, idx)
    if pos == 0 or pos >= len(sorted_existing):
        missing_records.append({
            "hadith_index": idx,
            "candidate_pages": [],
            "status": "unresolved",
        })
        unresolved += 1
        continue

    prev_idx = sorted_existing[pos - 1]
    next_idx = sorted_existing[pos]
    prev_page = idx_to_page[prev_idx]
    next_page = idx_to_page[next_idx]

    lo = min(prev_page, next_page)
    hi = max(prev_page, next_page)

    # Find failed or never-scraped pages in [lo-1, hi+1]
    candidates = []
    for p in range(lo - 1, hi + 2):
        if p not in all_pages:          # never scraped
            candidates.append(p)
        elif all_pages[p] == "failed":  # failed scrape
            candidates.append(p)

    if candidates:
        pages_to_scrape.update(candidates)
        missing_records.append({
            "hadith_index": idx,
            "candidate_pages": sorted(candidates),
            "status": "rescrape",
        })
        resolvable += 1
    else:
        missing_records.append({
            "hadith_index": idx,
            "candidate_pages": [],
            "status": "parsing_issue",
        })
        unresolved += 1

print(f"Resolvable by rescrape : {resolvable}")
print(f"Unresolved (parsing)   : {unresolved}")
print(f"Total pages to scrape  : {len(pages_to_scrape)}")

# --- Build output ---
output = {
    "range": {"min": min_idx, "max": max_idx},
    "total_found": len(all_indices),
    "total_missing": len(missing_indices),
    "resolvable_by_rescrape": resolvable,
    "unresolved_parsing_issue": unresolved,
    "pages_to_scrape": sorted(pages_to_scrape),
    "hadiths": missing_records,
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"Written to {OUTPUT_FILE}")
