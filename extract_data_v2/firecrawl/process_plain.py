import json
import re
import unicodedata

# Arabic tashkeel Unicode ranges
TASHKEEL = re.compile(
    "[\u0610-\u061A"   # Arabic Extended-A
    "\u064B-\u065F"   # Arabic diacritics (fathah, kasrah, dammah, shadda, sukun, etc.)
    "\u0670"          # Arabic letter superscript alef
    "\u06D6-\u06DC"   # Arabic small high letters
    "\u06DF-\u06E4"   # Arabic small letters
    "\u06E7\u06E8"    # Arabic small high yeh / noon
    "\u06EA-\u06ED"   # Arabic small low letters
    "]"
)

# Other non-letter chars to strip from narrator names (punctuation, trailing/leading noise)
NAME_NOISE = re.compile(r"[،,،\.\u060C\u061B\u061F\u00AB\u00BB\u2018-\u201F؟!؛:]+")


def remove_tashkeel(text: str) -> str:
    if not text:
        return text
    return TASHKEEL.sub("", text)


def clean_narrator_name(name: str) -> str:
    """Remove tashkeel and clean punctuation from narrator names."""
    name = remove_tashkeel(name)
    # Strip any leading/trailing punctuation or whitespace
    name = NAME_NOISE.sub(" ", name)
    # Collapse multiple spaces
    name = re.sub(r" {2,}", " ", name).strip()
    return name


ARABIC_INDIC = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def parse_hadith_indices(full_text: str) -> list[int]:
    """
    Extract ALL hadith indices from full_text by scanning the entire text.
    A hadith index is an Arabic/ASCII number followed by a dash, e.g.:
      '٤٠٨ - ٤٠٩ - حَدَّثَنَا'               -> [408, 409]
      'بَابُ حُسْنِ... ٤١ - قَالَ مَالِكٌ'  -> [41]
      '٨٩٦ - ... text ... ٨٩٧ - ... ٨٩٨ -'   -> [896, 897, 898]
    Scans the entire full_text for all 'number -' patterns (embedded indices).
    """
    # Strip tashkeel first so number-dash patterns aren't interrupted by diacritics
    plain = TASHKEEL.sub("", full_text)
    parts = re.findall(r"[٠-٩\d]+(?=\s*[-–—])", plain)
    if not parts:
        return []
    return [int(p.translate(ARABIC_INDIC)) for p in parts]


input_path = "shamela_book_1681.jsonl"
output_path = "shamela_book_1681_plain.jsonl"

processed = 0
with open(input_path, "r", encoding="utf-8") as fin, \
     open(output_path, "w", encoding="utf-8") as fout:
    for line in fin:
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)

        # Collect all hadith indices across all blocks on this page
        page_indices = []
        for block in record.get("hadith_blocks", []):
            page_indices.extend(parse_hadith_indices(block.get("full_text", "")))

        for block in record.get("hadith_blocks", []):
            # Add plain versions of full_text and matn
            block["full_text_plain"] = remove_tashkeel(block.get("full_text", ""))
            block["matn_plain"] = remove_tashkeel(block.get("matn", ""))

            # Clean narrator names and add name_plain
            for narrator in block.get("narrators", []):
                narrator["name_plain"] = clean_narrator_name(narrator.get("name", ""))

        # Rebuild record with hadith_indices right after url
        ordered = {
            "status": record.get("status"),
            "book_id": record.get("book_id"),
            "page_number": record.get("page_number"),
            "url": record.get("url"),
            "hadith_indices": page_indices,
        }
        for k, v in record.items():
            if k not in ordered:
                ordered[k] = v

        fout.write(json.dumps(ordered, ensure_ascii=False) + "\n")
        processed += 1

print(f"Done. Processed {processed} records -> {output_path}")
