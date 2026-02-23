import json
from collections import Counter
from pathlib import Path

INPUT_FILE = Path(__file__).parent / "Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn.json"
OUTPUT_FILE = Path(__file__).parent / "first_word_freq.json"

with open(INPUT_FILE, encoding="utf-8") as f:
    hadiths = json.load(f)

first_words = []
for entry in hadiths:
    text = entry.get("hadith_text", "").strip()
    if text:
        first_word = text.split()[0]
        first_words.append(first_word)

freq = Counter(first_words)
result = [{"word": word, "freq": count} for word, count in freq.most_common()]

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print(f"Done. {len(result)} unique first words from {len(first_words)} hadiths.")
print(f"Output: {OUTPUT_FILE}")
