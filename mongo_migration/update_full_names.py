"""
Update full_name (and full_name_plain) in narrators_tarajem.jsonl
using rawi_lookup.json as the source of truth.

Only updates narrators whose rawi_id appears in narrators_tabaqat.json
(the manually cleaned list).

Source of full_name : extract_data_v2/playwrite/rawi_lookup.json
Filter (which IDs)  : extract_data_v2/playwrite/narrators_tabaqat.json
Target file         : mongo_migration/processed_bukhari_podia/narrators_tarajem.jsonl

After running this script, re-upload the collection:
    python mongo_migration/upload.py
"""

import json
import pathlib
import re
import unicodedata

ROOT = pathlib.Path(__file__).parent.parent

TABAQAT_FILE  = ROOT / "extract_data_v2/playwrite/narrators_tabaqat.json"
LOOKUP_FILE   = ROOT / "extract_data_v2/playwrite/rawi_lookup.json"
TARAJEM_FILE  = ROOT / "mongo_migration/processed_bukhari_podia/narrators_tarajem.jsonl"


# ---------------------------------------------------------------------------
# Arabic tashkeel stripping (same logic used in preprocess.py)
# ---------------------------------------------------------------------------

ARABIC_DIACRITICS = re.compile(
    "[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)

def strip_tashkeel(text: str) -> str:
    if not text:
        return text
    return ARABIC_DIACRITICS.sub("", text)


# ---------------------------------------------------------------------------
# Load mappings
# ---------------------------------------------------------------------------

tabaqat = json.loads(TABAQAT_FILE.read_text(encoding="utf-8"))
tabaqat_ids = {entry["rawi_id"] for entry in tabaqat}
print(f"Loaded {len(tabaqat_ids)} rawi_ids from narrators_tabaqat.json")

rawi_lookup: dict = json.loads(LOOKUP_FILE.read_text(encoding="utf-8"))
# keys are strings in rawi_lookup
print(f"Loaded {len(rawi_lookup)} entries from rawi_lookup.json")

# Build: rawi_id (int) -> full_name
full_name_map: dict[int, str] = {}
for entry in tabaqat:
    rid = entry["rawi_id"]
    lookup_entry = rawi_lookup.get(str(rid))
    if lookup_entry and lookup_entry.get("full_name"):
        full_name_map[rid] = lookup_entry["full_name"]

print(f"full_name found for {len(full_name_map)} / {len(tabaqat_ids)} narrators")


# ---------------------------------------------------------------------------
# Update narrators_tarajem.jsonl
# ---------------------------------------------------------------------------

updated = 0
skipped_no_map = 0
lines_out = []

with open(TARAJEM_FILE, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        doc = json.loads(line)
        rid = doc.get("rawi_id")

        if rid in full_name_map:
            new_full_name = full_name_map[rid]
            new_full_name_plain = strip_tashkeel(new_full_name)

            old_full_name = doc.get("full_name", "")
            doc["full_name"] = new_full_name
            doc["full_name_plain"] = new_full_name_plain

            if old_full_name != new_full_name:
                updated += 1
        else:
            skipped_no_map += 1

        lines_out.append(json.dumps(doc, ensure_ascii=False))

TARAJEM_FILE.write_text("\n".join(lines_out) + "\n", encoding="utf-8")

print(f"\nResults:")
print(f"  Updated (full_name changed) : {updated}")
print(f"  Skipped (not in tabaqat)    : {skipped_no_map}")
print(f"  Total lines written         : {len(lines_out)}")
print(f"\nFile written: {TARAJEM_FILE}")
print("\nNext step: re-upload the collection:")
print("  python mongo_migration/upload.py")
