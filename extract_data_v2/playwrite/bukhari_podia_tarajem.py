import json
import re
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

BASE_URL = "https://www.bukhari-pedia.net/rwah/"
INPUT_FILE = "narrators_bukhari_pedia_playwrite.jsonl"
OUTPUT_FILE = "bukhari_narrators_tarajem.jsonl"

# --- Load input records keyed by rawi_id ---
input_records = {}
with open(INPUT_FILE, encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)
        rawi_id = record["rawi_id"]
        input_records[rawi_id] = record

# --- Load already-scraped rawi_ids from output file (resume support) ---
scraped_ids = set()
try:
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                scraped_ids.add(json.loads(line)["rawi_id"])
            except (json.JSONDecodeError, KeyError):
                pass
except FileNotFoundError:
    pass

remaining = [rid for rid in input_records if rid not in scraped_ids]
print(f"Total: {len(input_records)} | Already done: {len(scraped_ids)} | Remaining: {len(remaining)}")


def scrape_narrator(page, rawi_id):
    url = BASE_URL + str(rawi_id)
    page.goto(url, wait_until="networkidle")
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")

    # narrator stats — only divs with numeric content
    narrator_info = []
    for div in soup.find_all("div", class_="filter_item"):
        text = div.get_text(strip=True)
        action = div.get("action")
        if action and any(ch.isdigit() for ch in text):
            narrator_info.append({"action": action, "text": text})

    # biographies from embedded JS var info = {...}
    tarajim = []
    for script in soup.find_all("script"):
        content = script.string or ""
        match = re.search(r"var\s+info\s*=\s*(\{.*?\});", content, re.DOTALL)
        if match:
            info_dict = json.loads(match.group(1))
            tarajim = [{"source": src, "tarjama": txt} for src, txt in info_dict.items()]
            break

    return {
        "url": url,
        "narrator_info": narrator_info,
        "tarajim": tarajim,
    }


with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()

    with open(OUTPUT_FILE, "a", encoding="utf-8") as out_f:
        for i, rawi_id in enumerate(remaining, 1):
            try:
                scraped = scrape_narrator(page, rawi_id)
                record = {**input_records[rawi_id], **scraped}
                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_f.flush()
                print(f"[{i}/{len(remaining)}] ✓ {rawi_id}")
            except Exception as e:
                print(f"[{i}/{len(remaining)}] ✗ {rawi_id} — {e}")

    browser.close()

print("Done.")
