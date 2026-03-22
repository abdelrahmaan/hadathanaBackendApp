import re
import asyncio
import json
import html as html_lib
from pathlib import Path
from collections import OrderedDict
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Output files
HADITHS_FILE  = Path("hadith_narrators_bukhari_pedia_playwrite.jsonl")   # one hadith per line
NARRATORS_FILE = Path("narrators_bukhari_pedia_playwrite.jsonl")          # one narrator per line (deduplicated)

BASE_URL = "https://www.bukhari-pedia.net/book/matn_bukhari"
TOTAL_HADITHS = 11141


# ── Helpers ────────────────────────────────────────────────────────────────────

def normalize_text(s: str) -> str:
    s = s.replace("\ufeff", "").strip()
    return re.sub(r"\s+", " ", s).strip()


def extract_rank_and_full_info(mark_tag) -> tuple:
    tooltip_encoded = mark_tag.get("data-original-title") or ""
    if not tooltip_encoded:
        return None, None, None
    tooltip_html = html_lib.unescape(tooltip_encoded)
    tooltip_text = BeautifulSoup(tooltip_html, "lxml").get_text("\n", strip=True)
    rank_match = re.search(r"الرتبة:\s*([^\n]+)", tooltip_text)
    rank = rank_match.group(1).strip() if rank_match else None
    full_name = tooltip_text.split("\nالرتبة:")[0].strip()
    return rank, full_name, tooltip_text


def parse_narrators_from_hadith_html(container_html: str):
    """Returns list of narrator dicts, in order of appearance (unique by rawi_id)."""
    rawis = OrderedDict()
    soup = BeautifulSoup(container_html, "lxml")
    for m in soup.select("mark.mark_rawi[rawi_id]"):
        rawi_id = int(m.get("rawi_id"))
        if rawi_id not in rawis:
            rank, full_name, tooltip_text = extract_rank_and_full_info(m)
            rawis[rawi_id] = {
                "rawi_id": rawi_id,
                "name_in_chain": m.get_text(strip=True),
                "full_name": full_name,
                "rank": rank,
                "full_tooltip_info": tooltip_text,
            }
    return list(rawis.values())


def parse_hadith_text(container_html: str) -> str:
    soup = BeautifulSoup(container_html, "lxml")
    for m in soup.select("mark.mark_rawi"):
        m.unwrap()
    return normalize_text(soup.get_text(" ", strip=True))


# ── Resume / State loading ─────────────────────────────────────────────────────

def load_scraped_urls() -> set:
    """URLs already saved in HADITHS_FILE (successful or error)."""
    if not HADITHS_FILE.exists():
        return set()
    scraped = set()
    with open(HADITHS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
                if "hadith_url" in row:
                    scraped.add(row["hadith_url"])
            except json.JSONDecodeError:
                pass
    return scraped


def load_known_rawi_ids() -> set:
    """rawi_ids already saved in NARRATORS_FILE — skip re-saving these."""
    if not NARRATORS_FILE.exists():
        return set()
    known = set()
    with open(NARRATORS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
                if "rawi_id" in row:
                    known.add(row["rawi_id"])
            except json.JSONDecodeError:
                pass
    return known


# ── Saving ─────────────────────────────────────────────────────────────────────

def save_hadith(result: dict):
    with open(HADITHS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(result, ensure_ascii=False) + "\n")


def save_narrator(narrator: dict):
    with open(NARRATORS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(narrator, ensure_ascii=False) + "\n")


# ── Playwright helpers ─────────────────────────────────────────────────────────

async def dismiss_modal(page):
    try:
        btn = page.locator(".bootbox.modal .btn").first
        if await btn.count() > 0:
            await btn.wait_for(state="visible", timeout=5000)
            await btn.click()
            await page.wait_for_selector(".modal-backdrop", state="hidden", timeout=8000)
    except PlaywrightTimeoutError:
        pass


async def scrape_hadith(page, url: str, retries: int = 2) -> dict:
    for attempt in range(1, retries + 2):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await dismiss_modal(page)

            rwah_btn = page.locator("#a_rwah")
            if await rwah_btn.count() == 0:
                return {
                    "hadith_url": url,
                    "hadith_text": None,
                    "narrators": [],
                    "note": "no_narrator_button"
                }

            await rwah_btn.click()

            try:
                await page.wait_for_selector("mark.mark_rawi[rawi_id]", timeout=15000)
            except PlaywrightTimeoutError:
                container_html = await page.locator("#div_book_content_txt").inner_html()
                return {
                    "hadith_url": url,
                    "hadith_text": parse_hadith_text(container_html),
                    "narrators": [],
                    "note": "no_narrator_marks"
                }

            container_html = await page.locator("#div_book_content_txt").inner_html()
            narrators = parse_narrators_from_hadith_html(container_html)

            return {
                "hadith_url": url,
                "hadith_text": parse_hadith_text(container_html),
                "narrators": narrators,
            }

        except PlaywrightTimeoutError as e:
            if attempt <= retries:
                print(f"  → Timeout on attempt {attempt}, retrying...")
                await asyncio.sleep(2)
            else:
                raise e


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    urls = [f"{BASE_URL}/{i}" for i in range(1, TOTAL_HADITHS + 1)]

    scraped_urls  = load_scraped_urls()
    known_rawi_ids = load_known_rawi_ids()
    remaining     = [u for u in urls if u not in scraped_urls]

    print(f"Total hadiths : {len(urls)}")
    print(f"Already done  : {len(scraped_urls)}")
    print(f"Remaining     : {len(remaining)}")
    print(f"Known narrators: {len(known_rawi_ids)}")
    print()

    if not remaining:
        print("All hadiths already scraped.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="ar")
        page   = await context.new_page()

        for i, url in enumerate(remaining, 1):
            print(f"[{i}/{len(remaining)}] {url}")
            try:
                result = await scrape_hadith(page, url)

                # Save new narrators to narrators.jsonl (deduplicated globally)
                new_narrators = 0
                for narrator in result.get("narrators", []):
                    if narrator["rawi_id"] not in known_rawi_ids:
                        save_narrator(narrator)
                        known_rawi_ids.add(narrator["rawi_id"])
                        new_narrators += 1

                save_hadith(result)

                note = f" ({result['note']})" if "note" in result else ""
                print(f"  → {len(result['narrators'])} narrators "
                      f"({new_narrators} new){note} | saved")

            except Exception as e:
                print(f"  → FAILED: {e}")
                save_hadith({"hadith_url": url, "error": str(e)})

        await browser.close()

    print(f"\nDone.")
    print(f"  Hadiths   → {HADITHS_FILE.resolve()}")
    print(f"  Narrators → {NARRATORS_FILE.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
