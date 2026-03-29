"""
match_bukhari_to_shamela.py
===========================
Match every Bukhari hadith to its counterpart in the Shamela scrape,
using normalized matn text.

Both sides are normalized identically:
  - Strip tashkeel (Shamela is fully diacritized; Bukhari has none)
  - Strip Arabic ligatures (رضي الله عنه ⦗⦘ … * etc.) that survive tashkeel removal
  - Normalize hamza/ta-marbuta/alef-maqsura variants
  - Remove punctuation, numbers, special symbols

Passes (first hit wins):
  1. Exact matn       — normalize(bukhari matn) == normalize(shamela matn)
  2. Full-text exact  — normalize(bukhari full_text) == normalize(shamela full_text)
  3. Substring fwd    — normalize(bukhari matn) is contained in normalize(shamela full_text)
                        Catches: LLM drops intro clause but the matn body is identical.
  4. Substring rev    — normalize(shamela matn) is contained in normalize(bukhari full_text)
                        Catches: Shamela matn is longer/fuller; Bukhari has the same text
                        embedded inside the full hadith_text.
  5. Word-overlap     — Jaccard similarity of word sets >= JACCARD_THRESHOLD
                        Catches: LLM drops reporting verbs like "قال / فقال" mid-matn.
                        Only fires when bukhari matn has >= MIN_WORDS_FOR_JACCARD words.
  6. Prefix           — first PREFIX_LEN normalized chars agree

Output: match_results.json  — one entry per Bukhari hadith
"""

import json
import re
import unicodedata
from pathlib import Path

# ── paths ────────────────────────────────────────────────────────────────────
BASE         = Path(__file__).parent
BUKHARI_PATH = BASE / "Bukhari_Without_Tashkel_results_advanced_with_matn.json"
SHAMELA_PATH = BASE.parent / "firecrawl" / "shamela_book_1681_plain.jsonl"
OUTPUT_PATH  = BASE / "match_results.json"

# ── tuning knobs ──────────────────────────────────────────────────────────────
MIN_SUBSTR_LEN        = 60    # min chars for substring pass
PREFIX_LEN            = 80    # chars for prefix pass
JACCARD_THRESHOLD     = 0.72  # word-set Jaccard required for overlap pass
MIN_WORDS_FOR_JACCARD = 8     # bukhari matn must have at least this many words


# ── normalization ─────────────────────────────────────────────────────────────
_TASHKEEL_RE = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)
_SYMBOLS_RE  = re.compile(r"[«»\(\)\[\]٠-٩0-9﵁ﷺ﴿﴾\u06DD\uFDFA\uFDFB]+")
_LIGATURE_RE = re.compile(r"[\u2997\u2998\u2026\u002A\u2013\uFD40-\uFD4F\uFDFF\uFDFD]+")
_PUNCT_RE    = re.compile(r"[.,،؛;:?!\-_/\\\"'`«»]+")

def normalize(text: str) -> str:
    if not text:
        return ""
    text = _TASHKEEL_RE.sub("", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = _SYMBOLS_RE.sub("", text)
    text = _LIGATURE_RE.sub("", text)
    text = _PUNCT_RE.sub("", text)
    text = text.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    text = text.replace("ئ", "ي").replace("ؤ", "و")
    text = text.replace("ة", "ه")
    text = text.replace("ى", "ي")
    text = re.sub(r"\s+", " ", text).strip()
    return text

# Arabic-indic digits (٠١٢٣٤٥٦٧٨٩) and western digits at start of full_text
_HADITH_NUM_RE = re.compile(r"^([٠-٩\d]+)\s*[-–]")
_ARABIC_INDIC  = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

def extract_shamela_hadith_number(full_text: str) -> int | None:
    """Extract the sequential hadith number from the start of a Shamela full_text."""
    m = _HADITH_NUM_RE.match(full_text.strip())
    if m:
        return int(m.group(1).translate(_ARABIC_INDIC))
    return None

def word_set(norm_text: str) -> frozenset:
    return frozenset(norm_text.split())

def jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / (len(a) + len(b) - inter)


# ── load data ─────────────────────────────────────────────────────────────────
print("Loading Bukhari …")
with open(BUKHARI_PATH, encoding="utf-8") as f:
    bukhari_hadiths = json.load(f)

print("Loading Shamela (tashkeel is removed via normalize at index time) …")
shamela_pages = []
with open(SHAMELA_PATH, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            shamela_pages.append(json.loads(line))


# ── build Shamela index ───────────────────────────────────────────────────────
print("Building Shamela index …")

shamela_blocks   = []    # flat list of all block dicts (raw, with tashkeel)
block_norm_matn  = []    # normalize(matn)      — parallel to shamela_blocks
block_norm_full  = []    # normalize(full_text) — parallel to shamela_blocks
block_word_sets  = []    # word_set(norm_full)  — parallel to shamela_blocks

matn_index      = {}     # norm_matn             → [idx, …]
full_text_index = {}     # norm_full             → [idx, …]
prefix_index    = {}     # norm_matn[:PREFIX_LEN]→ [idx, …]

for page in shamela_pages:
    page_number = page.get("page_number")
    url         = page.get("url", "")
    for block in page.get("hadith_blocks", []):
        idx = len(shamela_blocks)

        raw_full = block.get("full_text", "")
        # Use name_plain (pre-stripped of tashkeel by the scraper) when available,
        # falling back to name. This avoids normalizing tashkeel ourselves.
        narrators_clean = [
            {**n, "name": n.get("name_plain") or n.get("name", "")}
            for n in block.get("narrators", [])
        ]
        entry = {
            "page_number":           page_number,
            "url":                   url,
            "shamela_hadith_number": extract_shamela_hadith_number(raw_full),
            "full_text":             raw_full,
            "matn":                  block.get("matn", ""),
            "narrators":             narrators_clean,
        }
        shamela_blocks.append(entry)

        nm = normalize(entry["matn"])
        nf = normalize(entry["full_text"])

        block_norm_matn.append(nm)
        block_norm_full.append(nf)
        block_word_sets.append(word_set(nf))

        if nm:
            matn_index.setdefault(nm, []).append(idx)
            prefix_index.setdefault(nm[:PREFIX_LEN], []).append(idx)
        if nf:
            full_text_index.setdefault(nf, []).append(idx)

print(f"  Shamela blocks:        {len(shamela_blocks)}")
print(f"  Unique matn keys:      {len(matn_index)}")
print(f"  Unique full_text keys: {len(full_text_index)}")
print(f"  Unique prefix keys:    {len(prefix_index)}")


# ── matching ──────────────────────────────────────────────────────────────────
print("\nMatching Bukhari → Shamela …")

results = []
stats = {
    "matn_exact":       0,
    "full_text_exact":  0,
    "substring_fwd":    0,
    "substring_rev":    0,
    "word_overlap":     0,
    "prefix":           0,
    "no_match":         0,
}

for hadith in bukhari_hadiths:
    hid           = hadith["hadith_index"]
    matn_segments = hadith.get("matn_segments", [])
    hadith_text   = hadith.get("hadith_text", "")

    bukhari_matn      = matn_segments[0] if matn_segments else ""
    norm_bukhari_matn = normalize(bukhari_matn)
    norm_bukhari_full = normalize(hadith_text)
    bukhari_words     = word_set(norm_bukhari_matn)
    bukhari_full_words = word_set(norm_bukhari_full)

    match_method   = None
    matched_blocks = []

    # ── Pass 1: exact matn match ──────────────────────────────────────────────
    if norm_bukhari_matn and norm_bukhari_matn in matn_index:
        matched_blocks = [shamela_blocks[i] for i in matn_index[norm_bukhari_matn]]
        match_method   = "matn_exact"

    # ── Pass 2: full-text exact match ─────────────────────────────────────────
    if not matched_blocks and norm_bukhari_full and norm_bukhari_full in full_text_index:
        matched_blocks = [shamela_blocks[i] for i in full_text_index[norm_bukhari_full]]
        match_method   = "full_text_exact"

    # ── Pass 3: substring fwd — bukhari matn inside shamela full_text ──────────
    if not matched_blocks and len(norm_bukhari_matn) >= MIN_SUBSTR_LEN:
        hits = [i for i, nf in enumerate(block_norm_full) if norm_bukhari_matn in nf]
        if hits:
            matched_blocks = [shamela_blocks[i] for i in hits]
            match_method   = "substring_fwd"

    # ── Pass 4: substring rev — shamela matn inside bukhari full_text ──────────
    # Catches cases where Shamela's matn is the full statement and Bukhari's
    # hadith_text contains it verbatim even though the extracted matn_segment differs.
    if not matched_blocks and norm_bukhari_full:
        hits = [
            i for i, nm in enumerate(block_norm_matn)
            if nm and len(nm) >= MIN_SUBSTR_LEN and nm in norm_bukhari_full
        ]
        if hits:
            matched_blocks = [shamela_blocks[i] for i in hits]
            match_method   = "substring_rev"

    # ── Pass 5: word-overlap (Jaccard on word sets) ───────────────────────────
    # The LLM drops reporting verbs ("قال / فقال") mid-text, breaking substring.
    # Word-overlap ignores word order and catches most-word overlap.
    if not matched_blocks and len(bukhari_words) >= MIN_WORDS_FOR_JACCARD:
        best_score  = JACCARD_THRESHOLD - 0.001   # must strictly exceed threshold
        best_hits   = []
        for i, sws in enumerate(block_word_sets):
            score = jaccard(bukhari_words, sws)
            if score > best_score:
                best_score = score
                best_hits  = [i]
            elif score == best_score and best_hits:
                best_hits.append(i)
        if best_hits:
            matched_blocks = [shamela_blocks[i] for i in best_hits]
            match_method   = "word_overlap"

    # ── Pass 6: prefix match ──────────────────────────────────────────────────
    if not matched_blocks and norm_bukhari_matn:
        prefix = norm_bukhari_matn[:PREFIX_LEN]
        if prefix and prefix in prefix_index:
            matched_blocks = [shamela_blocks[i] for i in prefix_index[prefix]]
            match_method   = "prefix"

    # ── record result ─────────────────────────────────────────────────────────
    if matched_blocks:
        stats[match_method] += 1
        unique = len(matched_blocks) == 1
        result = {
            "hadith_index":    hid,
            "match_method":    match_method,
            "unique_match":    unique,
            "match_count":     len(matched_blocks),
            "shamela_matches": [
                {
                    "shamela_hadith_number": b["shamela_hadith_number"],
                    "page_number":           b["page_number"],
                    "url":                   b["url"],
                    "narrators":             b["narrators"],
                    "matn":                  b["matn"],
                }
                for b in matched_blocks
            ],
        }
    else:
        stats["no_match"] += 1
        result = {
            "hadith_index":    hid,
            "match_method":    None,
            "unique_match":    False,
            "match_count":     0,
            "shamela_matches": [],
        }

    results.append(result)


# ── summary ───────────────────────────────────────────────────────────────────
total = len(bukhari_hadiths)
matched_total   = total - stats["no_match"]
unique_count    = sum(1 for r in results if r["unique_match"])
collision_count = sum(1 for r in results if r["match_count"] > 1)

print(f"\n{'─'*52}")
print(f"Total Bukhari hadiths    : {total}")
print(f"  matn_exact             : {stats['matn_exact']:>5}  ({stats['matn_exact']/total*100:.1f}%)")
print(f"  full_text_exact        : {stats['full_text_exact']:>5}  ({stats['full_text_exact']/total*100:.1f}%)")
print(f"  substring_fwd          : {stats['substring_fwd']:>5}  ({stats['substring_fwd']/total*100:.1f}%)")
print(f"  substring_rev          : {stats['substring_rev']:>5}  ({stats['substring_rev']/total*100:.1f}%)")
print(f"  word_overlap (Jaccard) : {stats['word_overlap']:>5}  ({stats['word_overlap']/total*100:.1f}%)")
print(f"  prefix                 : {stats['prefix']:>5}  ({stats['prefix']/total*100:.1f}%)")
print(f"  no match               : {stats['no_match']:>5}  ({stats['no_match']/total*100:.1f}%)")
print(f"  TOTAL MATCHED          : {matched_total:>5}  ({matched_total/total*100:.1f}%)")
print(f"  of which unique (1:1)  : {unique_count:>5}  ({unique_count/total*100:.1f}%)")
print(f"  collisions (>1 hit)    : {collision_count:>5}")
print(f"{'─'*52}")


# ── write output ──────────────────────────────────────────────────────────────
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"\nOutput written to: {OUTPUT_PATH}")
