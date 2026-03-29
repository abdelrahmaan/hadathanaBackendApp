"""
export_null_narrators_csv.py
============================
Convert null_narrators_review.json → null_narrators_review.csv

One row per narrator (wide format).
Columns:
    narrator_name         — narrator name (Arabic)
    role                  — empty (for manual review)
    total_occurrences     — how many times this narrator appears with null ID
    hadith_1_text         — first sample hadith text preview
    hadith_2_text         — second sample hadith text preview
    hadith_3_text         — third sample hadith text preview
    narrator_id           — empty (for manual review)
"""

import csv
import json
from pathlib import Path

BASE        = Path(__file__).parent
INPUT_PATH  = BASE / "null_narrators_review.json"
OUTPUT_PATH = BASE / "null_narrators_review.csv"

with open(INPUT_PATH, encoding="utf-8") as f:
    narrators = json.load(f)

rows = []
for entry in narrators:
    samples = entry.get("sample_hadiths", [])
    rows.append({
        "narrator_name":     entry["name"],
        "role":              "",
        "total_occurrences": entry["total_mentions"],
        "hadith_1_text":     samples[0]["hadith_text_preview"] if len(samples) > 0 else "",
        "hadith_2_text":     samples[1]["hadith_text_preview"] if len(samples) > 1 else "",
        "hadith_3_text":     samples[2]["hadith_text_preview"] if len(samples) > 2 else "",
        "narrator_id":       "",
    })

fieldnames = [
    "narrator_name",
    "role",
    "total_occurrences",
    "hadith_1_text",
    "hadith_2_text",
    "hadith_3_text",
    "narrator_id",
]

with open(OUTPUT_PATH, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Written {len(rows)} rows → {OUTPUT_PATH}")
