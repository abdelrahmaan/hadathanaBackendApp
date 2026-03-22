"""
Clean Arabic text fields in playwrite_bukhari_hadith_podia.jsonl:
  - Remove tashkeel (harakat)
  - Remove parenthesized numbers like (1), (2)
  - Remove leading/trailing numbers
  - Remove punctuation: , . ♠ and similar
  - Remove underscore wrappers used for italics
  - Normalize whitespace
"""

import json
import re
import unicodedata
from pathlib import Path

# Arabic tashkeel Unicode range: U+064B – U+065F and U+0670
TASHKEEL = re.compile(r"[\u064B-\u065F\u0670]")

# Parenthesized numbers: (1), (2), etc. — anywhere in the string
PAREN_NUMBERS = re.compile(r"\(\d+\)")

# Leading/trailing digits (e.g. "1يحيى" or "يحيى1")
LEADING_TRAILING_DIGITS = re.compile(r"^\d+|\d+$")

# Punctuation to strip: Arabic comma، Arabic full stop، Latin , . and special chars like ♠
UNWANTED_CHARS = re.compile(r"[،؛,\.،؟!♠♦♣♥_\[\]{}\"']")

# Newline-based tooltip label prefix (e.g. "\nالرتبة: ")
TOOLTIP_RANK_PREFIX = re.compile(r"\nالرتبة:\s*")


def clean_arabic(text: str) -> str:
    if not text:
        return text
    # 1. Remove tashkeel
    text = TASHKEEL.sub("", text)
    # 2. Remove parenthesized numbers
    text = PAREN_NUMBERS.sub("", text)
    # 3. Remove unwanted punctuation and special chars
    text = UNWANTED_CHARS.sub("", text)
    # 4. Normalize whitespace (collapse multiple spaces, strip edges)
    text = re.sub(r"\s+", " ", text).strip()
    # 5. Remove any remaining leading/trailing digits after cleanup
    text = LEADING_TRAILING_DIGITS.sub("", text).strip()
    return text


def clean_tooltip(text: str) -> str:
    """Clean tooltip which contains full_name + newline + 'الرتبة: ' + rank."""
    if not text:
        return text
    # Split on the rank label line and clean each part separately
    parts = TOOLTIP_RANK_PREFIX.split(text, maxsplit=1)
    cleaned = "\n".join(clean_arabic(p) for p in parts)
    return cleaned.strip()


def clean_record(record: dict) -> dict:
    return {
        "rawi_id": record["rawi_id"],
        "name_in_chain": clean_arabic(record.get("name_in_chain", "")),
        "full_name": clean_arabic(record.get("full_name", "")),
        "rank": clean_arabic(record.get("rank", "")),
        "full_tooltip_info": clean_tooltip(record.get("full_tooltip_info", "")),
    }


def main():
    input_path = Path(__file__).parent / "playwrite_bukhari_hadith_podia.jsonl"
    output_path = input_path.parent / (input_path.stem + "_clean.jsonl")

    records = []
    with input_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    cleaned = [clean_record(r) for r in records]

    with output_path.open("w", encoding="utf-8") as f:
        for record in cleaned:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Done. {len(cleaned)} records written to: {output_path}")


if __name__ == "__main__":
    main()
