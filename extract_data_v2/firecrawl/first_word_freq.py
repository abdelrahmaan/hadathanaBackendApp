import json
import re
from collections import Counter
from pathlib import Path

INPUT_FILE = Path(__file__).parent / "shamela_book_1681.jsonl"
OUTPUT_FILE = Path(__file__).parent / "first_word_freq.json"

# Arabic tashkeel (diacritics) unicode range
TASHKEEL = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670]")
# Leading hadith number pattern e.g. "١ - " or "١- " (Arabic-Indic or Western digits)
HADITH_NUM = re.compile(r"^[\u0660-\u0669\d]+\s*-\s*")
# Pure number token (Arabic-Indic or Western digits only)
PURE_NUMBER = re.compile(r"^[\u0660-\u0669\d]+$")


def remove_tashkeel(text: str) -> str:
    return TASHKEEL.sub("", text)


first_words = []
total_blocks = 0

with open(INPUT_FILE, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        for block in record.get("hadith_blocks", []):
            full_text = block.get("full_text", "").strip()
            if not full_text:
                continue
            total_blocks += 1
            # 1. Remove tashkeel
            clean = remove_tashkeel(full_text)
            # 2. Strip leading hadith number (e.g. "١ - ")
            clean = HADITH_NUM.sub("", clean).strip()
            # 3. Get first word, skip if it's a bare number
            if clean:
                word = clean.split()[0]
                if not PURE_NUMBER.match(word):
                    first_words.append(word)

freq = Counter(first_words)
result = [{"word": word, "freq": count} for word, count in freq.most_common()]

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print(f"Done. {len(result)} unique first words from {total_blocks} hadith blocks.")
print(f"Output: {OUTPUT_FILE}")
