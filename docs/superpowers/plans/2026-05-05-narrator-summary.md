# Narrator Summary Field Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate and store a structured Arabic `summary` field on every document in `processed_podia_narrator_biographies` using an LLM for the biographical extraction (kunya, era, location, notes) and pure Python for the structured fields (hadith counts, transmission stats, top teachers/students, tarajim sources).

**Architecture:** A standalone offline script `scripts/generate_narrator_summaries.py` reads from two MongoDB collections, calls `google/gemini-3-flash-preview` via OpenRouter once per narrator with a Pydantic structured-output chain, then bulk-writes the result back with `$set` — never replacing existing fields. The script is resumable: it skips narrators that already have a `summary` field unless `--force` is passed.

**Tech Stack:** Python 3.12, PyMongo (sync, for the script), LangChain `ChatOpenAI` + `with_structured_output`, Pydantic v2, `python-dotenv`, `tqdm`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `scripts/generate_narrator_summaries.py` | **Create** | Main script: CLI, data fetch, LLM call, bulk write |
| `tasks.md` | **Modify** | Add task entry under v1.1 |

---

## Task 1: Define the Pydantic summary models inside the script

**Files:**
- Create: `scripts/generate_narrator_summaries.py`

- [ ] **Step 1: Create the script file with models only**

```python
#!/usr/bin/env python3
"""
Generate structured Arabic summaries for Podia narrator biographies.

Reads from MongoDB collections:
  - processed_podia_narrator_biographies  (tarajim, narrator_info, rank, full_name)
  - analytics_narrator_stats_podia        (teachers, students, hadith_count)

Calls google/gemini-3-flash-preview via OpenRouter to extract:
  kunya, era, location, notes

Writes { summary: {...} } back to processed_podia_narrator_biographies via $set.
Also appends each result to mongo_migration/processed_bukhari_podia/narrator_summaries.jsonl.

USAGE
-----
    python scripts/generate_narrator_summaries.py              # run all pending
    python scripts/generate_narrator_summaries.py --force      # re-generate all
    python scripts/generate_narrator_summaries.py --limit 5    # smoke test
    python scripts/generate_narrator_summaries.py --dry-run    # count pending, exit
    python scripts/generate_narrator_summaries.py --db HadithData --uri mongodb://localhost:27018/

ENV VARS REQUIRED (in .env)
----------------------------
    OPENROUTER_API_KEY
"""

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm

_REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = _REPO_ROOT / "mongo_migration/processed_bukhari_podia/narrator_summaries.jsonl"

LLM_MODEL = "google/gemini-3-flash-preview"
MAX_RETRIES_LLM = 3
RETRY_BASE_DELAY = 2.0

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TransmissionStats(BaseModel):
    total: int = Field(description="إجمالي الروايات")
    connected: int = Field(description="الروايات الموصولة")
    suspended: int = Field(description="الروايات المعلقة")
    disputed: int = Field(description="الروايات المختلف فيها بين رواة الصحيح")


class NarratorRelation(BaseModel):
    rawi_id: int
    name: str
    freq: int = Field(description="عدد الأحاديث المشتركة")


class LLMExtracted(BaseModel):
    kunya: str | None = Field(default=None, description="الكنية مثل أبو محمد — None إذا لم تُذكر")
    era: str | None = Field(default=None, description="العصر مثل القرن 1 هـ — None إذا لم يُذكر")
    location: str | None = Field(default=None, description="المدينة أو البلد — None إذا لم يُذكر")
    notes: str | None = Field(default=None, description="فقرة موجزة 3-5 جمل عن أبرز آراء العلماء في هذا الراوي")


class NarratorSummary(BaseModel):
    full_name: str
    kunya: str | None
    era: str | None
    location: str | None
    classification: str
    hadith_count: int
    transmission_stats: TransmissionStats
    top_teachers: list[NarratorRelation]
    top_students: list[NarratorRelation]
    tarajim_sources: list[str]
    notes: str | None
```

- [ ] **Step 2: Verify the file parses without errors**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "import scripts.generate_narrator_summaries"
```

Expected: no output (clean import).

> Note: If you get `ModuleNotFoundError` for `langchain_openai` or `pymongo`, they are already in the venv — verify with `/home/abdo_kamar/Projects/.venv/bin/pip show langchain-openai pymongo`.

- [ ] **Step 3: Commit**

```bash
git add scripts/generate_narrator_summaries.py
git commit -m "feat: add narrator summary script skeleton with Pydantic models"
```

---

## Task 2: Add structured-field parsers (no LLM, no DB)

These functions turn raw MongoDB documents into the structured parts of `NarratorSummary`.

**Files:**
- Modify: `scripts/generate_narrator_summaries.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_narrator_summary_parsers.py`:

```python
"""Tests for the pure-Python structured-field parsers in generate_narrator_summaries."""
import pytest
from scripts.generate_narrator_summaries import (
    parse_hadith_count,
    parse_transmission_stats,
    build_top_relations,
    TransmissionStats,
    NarratorRelation,
)

NARRATOR_INFO_520 = [
    {"action": "get_matn_entries",       "text": "عدد أحاديثه المرقَّمة في الصحيح: 214"},
    {"action": "get_esnad_entries",      "text": "عدد رواياته الكلي: 281"},
    {"action": "get_esnad_mt_entries",   "text": "الروايات الموصولة: 226"},
    {"action": "get_esnad_kht_entries",  "text": "الروايات المعلقة: 49"},
    {"action": "get_esnad_mt_kht_entries","text": "الروايات المُختَلَف في وصْلِها وتعليقها: 0"},
    {"action": "get_esnad_shk_entries",  "text": "الروايات المُختَلَف فيها بين رواة الصحيح أو شرَّاحه: 3"},
    {"action": "get_esnad_mqron_entries","text": "الأسانيد التي قْرٍن فيها بغيره: 68"},
]

TEACHERS_520 = [
    {"rawi_id": 1794, "name": "أَبا هُرَيْرَةَ", "freq": 133},
    {"rawi_id": 1818, "name": "عايِشَةَ",         "freq": 14},
    {"rawi_id": 487,  "name": "سَعْدًا",           "freq": 7},
    {"rawi_id": 484,  "name": "أَبِي سَعِيدٍ",    "freq": 5},
    {"rawi_id": 325,  "name": "حَكِيمِ بْنِ حِزامٍ", "freq": 4},
    {"rawi_id": 212,  "name": "جُبَيْرَ بْنَ مُطْعِمٍ", "freq": 3},
]


def test_parse_hadith_count():
    assert parse_hadith_count(NARRATOR_INFO_520) == 214


def test_parse_hadith_count_missing():
    assert parse_hadith_count([]) == 0


def test_parse_transmission_stats():
    stats = parse_transmission_stats(NARRATOR_INFO_520)
    assert stats == TransmissionStats(total=281, connected=226, suspended=49, disputed=3)


def test_parse_transmission_stats_missing():
    stats = parse_transmission_stats([])
    assert stats == TransmissionStats(total=0, connected=0, suspended=0, disputed=0)


def test_build_top_relations_top5():
    top = build_top_relations(TEACHERS_520, n=5)
    assert len(top) == 5
    assert top[0] == NarratorRelation(rawi_id=1794, name="أَبا هُرَيْرَةَ", freq=133)


def test_build_top_relations_fewer_than_n():
    top = build_top_relations(TEACHERS_520[:2], n=5)
    assert len(top) == 2


def test_build_top_relations_empty():
    assert build_top_relations([], n=5) == []
```

- [ ] **Step 2: Run tests — confirm they FAIL**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_narrator_summary_parsers.py -v
```

Expected: `ImportError` or `AttributeError` — the functions don't exist yet.

- [ ] **Step 3: Implement the parsers in the script**

Add these functions after the Pydantic models in `scripts/generate_narrator_summaries.py`:

```python
# ---------------------------------------------------------------------------
# Structured-field parsers (no LLM, no DB)
# ---------------------------------------------------------------------------

import re as _re

def _extract_number(text: str) -> int:
    m = _re.search(r"\d+", text)
    return int(m.group()) if m else 0


def parse_hadith_count(narrator_info: list[dict]) -> int:
    for entry in narrator_info:
        if entry.get("action") == "get_matn_entries":
            return _extract_number(entry.get("text", ""))
    return 0


def parse_transmission_stats(narrator_info: list[dict]) -> TransmissionStats:
    mapping = {
        "get_esnad_entries":       "total",
        "get_esnad_mt_entries":    "connected",
        "get_esnad_kht_entries":   "suspended",
        "get_esnad_shk_entries":   "disputed",
    }
    values: dict[str, int] = {k: 0 for k in mapping.values()}
    for entry in narrator_info:
        key = mapping.get(entry.get("action", ""))
        if key:
            values[key] = _extract_number(entry.get("text", ""))
    return TransmissionStats(**values)


def build_top_relations(relations: list[dict], n: int = 5) -> list[NarratorRelation]:
    sorted_rel = sorted(relations, key=lambda r: r.get("freq", 0), reverse=True)
    return [
        NarratorRelation(rawi_id=r["rawi_id"], name=r["name"], freq=r["freq"])
        for r in sorted_rel[:n]
    ]
```

- [ ] **Step 4: Run tests — confirm they PASS**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_narrator_summary_parsers.py -v
```

Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/generate_narrator_summaries.py tests/test_narrator_summary_parsers.py
git commit -m "feat: add structured-field parsers for narrator summary"
```

---

## Task 3: Add the LLM chain

**Files:**
- Modify: `scripts/generate_narrator_summaries.py`

- [ ] **Step 1: Add the LLM chain builder and call function**

Append after the parsers section in `scripts/generate_narrator_summaries.py`:

```python
# ---------------------------------------------------------------------------
# LLM chain
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "أنت باحث متخصص في علم الرجال وعلم الحديث النبوي. "
    "مهمتك استخراج معلومات دقيقة عن رواة صحيح البخاري من نصوص التراجم المقدَّمة إليك."
)

_HUMAN_TEMPLATE = """\
فيما يلي معلومات عن راوٍ من رواة صحيح البخاري.

الاسم: {full_name}
الرتبة: {rank}

التراجم:
{tarajim_text}

استخرج المعلومات التالية باللغة العربية الفصحى:
- kunya: الكنية (مثل: أبو محمد) — اترك الحقل فارغاً إذا لم تُذكر
- era: العصر (مثل: القرن 1 هـ) — اترك الحقل فارغاً إذا لم يُذكر
- location: المدينة أو البلد (مثل: المدينة المنورة) — اترك الحقل فارغاً إذا لم يُذكر
- notes: فقرة موجزة (3-5 جمل) تلخّص أبرز ما قاله العلماء في هذا الراوي من توثيق أو تجريح أو مزايا علمية.\
  إذا لم تتوفر تراجم فاترك الحقل فارغاً.
"""


def build_llm_chain(openrouter_key: str):
    llm = ChatOpenAI(
        model=LLM_MODEL,
        base_url="https://openrouter.ai/api/v1",
        api_key=openrouter_key,
        temperature=0.0,
    )
    prompt = ChatPromptTemplate.from_messages([
        ("system", _SYSTEM_PROMPT),
        ("human", _HUMAN_TEMPLATE),
    ])
    return prompt | llm.with_structured_output(LLMExtracted)


def call_llm(
    chain,
    full_name: str,
    rank: str,
    tarajim: list[dict],
    retries: int = MAX_RETRIES_LLM,
) -> LLMExtracted | None:
    tarajim_text = "\n\n".join(
        t.get("tarjama_plain") or t.get("tarjama", "") for t in tarajim
    ).strip()

    if not tarajim_text:
        return LLMExtracted()  # all None — no text to extract from

    for attempt in range(retries):
        try:
            return chain.invoke({
                "full_name": full_name,
                "rank": rank,
                "tarajim_text": tarajim_text,
            })
        except Exception as exc:
            if attempt == retries - 1:
                tqdm.write(f"  [LLM ERROR] giving up after {retries} attempts: {exc}")
                return None
            delay = RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            tqdm.write(f"  [LLM] retry {attempt + 1}/{retries} in {delay:.1f}s — {exc}")
            time.sleep(delay)
    return None
```

- [ ] **Step 2: Verify script still parses cleanly**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "import scripts.generate_narrator_summaries"
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add scripts/generate_narrator_summaries.py
git commit -m "feat: add LLM chain for narrator summary extraction"
```

---

## Task 4: Add the assembly function

This combines LLM-extracted fields with the structured fields into a final `NarratorSummary`.

**Files:**
- Modify: `scripts/generate_narrator_summaries.py`
- Test: `tests/test_narrator_summary_parsers.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_narrator_summary_parsers.py`:

```python
from scripts.generate_narrator_summaries import (
    assemble_summary,
    LLMExtracted,
    NarratorSummary,
    TransmissionStats,
    NarratorRelation,
)

BIO_DOC_520 = {
    "rawi_id": 520,
    "full_name": "سَعِيد بن المُسَيَّب",
    "rank": "أحدُ العلماء الأثبات الفقهاء الكبار",
    "narrator_info": NARRATOR_INFO_520,
    "tarajim": [
        {"source": "الجرح والتعديل", "tarjama": "...", "tarjama_plain": "..."},
    ],
}

STATS_DOC_520 = {
    "rawi_id": 520,
    "hadith_count": 199,
    "teachers": TEACHERS_520,
    "students": [
        {"rawi_id": 1398, "name": "الزُّهْرِيِّ", "freq": 164},
        {"rawi_id": 46,   "name": "الزُّهْرِيِّ", "freq": 6},
    ],
}

LLM_RESULT_520 = LLMExtracted(
    kunya="أبو محمد",
    era="القرن 1 هـ",
    location="المدينة المنورة",
    notes="وثّقه الأئمة وعدّوه من أفقه التابعين.",
)


def test_assemble_summary_full():
    summary = assemble_summary(BIO_DOC_520, STATS_DOC_520, LLM_RESULT_520)
    assert isinstance(summary, NarratorSummary)
    assert summary.full_name == "سَعِيد بن المُسَيَّب"
    assert summary.kunya == "أبو محمد"
    assert summary.era == "القرن 1 هـ"
    assert summary.location == "المدينة المنورة"
    assert summary.classification == "أحدُ العلماء الأثبات الفقهاء الكبار"
    assert summary.hadith_count == 214          # from narrator_info, not stats
    assert summary.transmission_stats.total == 281
    assert summary.transmission_stats.connected == 226
    assert summary.transmission_stats.suspended == 49
    assert summary.transmission_stats.disputed == 3
    assert len(summary.top_teachers) == 5
    assert summary.top_teachers[0].rawi_id == 1794
    assert len(summary.top_students) == 2       # only 2 students available
    assert summary.tarajim_sources == ["الجرح والتعديل"]
    assert summary.notes == "وثّقه الأئمة وعدّوه من أفقه التابعين."


def test_assemble_summary_no_stats():
    summary = assemble_summary(BIO_DOC_520, None, LLM_RESULT_520)
    assert summary.top_teachers == []
    assert summary.top_students == []
    assert summary.hadith_count == 214  # falls back to narrator_info


def test_assemble_summary_llm_none():
    summary = assemble_summary(BIO_DOC_520, STATS_DOC_520, None)
    assert summary is None
```

- [ ] **Step 2: Run tests — confirm they FAIL**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_narrator_summary_parsers.py -v -k "assemble"
```

Expected: `ImportError` — `assemble_summary` not defined yet.

- [ ] **Step 3: Implement `assemble_summary`**

Append after the LLM section in `scripts/generate_narrator_summaries.py`:

```python
# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def assemble_summary(
    bio_doc: dict,
    stats_doc: dict | None,
    llm_result: LLMExtracted | None,
) -> NarratorSummary | None:
    if llm_result is None:
        return None

    narrator_info = bio_doc.get("narrator_info", [])
    tarajim = bio_doc.get("tarajim", [])

    teachers = stats_doc.get("teachers", []) if stats_doc else []
    students = stats_doc.get("students", []) if stats_doc else []

    return NarratorSummary(
        full_name=bio_doc.get("full_name", ""),
        kunya=llm_result.kunya,
        era=llm_result.era,
        location=llm_result.location,
        classification=bio_doc.get("rank", ""),
        hadith_count=parse_hadith_count(narrator_info),
        transmission_stats=parse_transmission_stats(narrator_info),
        top_teachers=build_top_relations(teachers, n=5),
        top_students=build_top_relations(students, n=5),
        tarajim_sources=[t["source"] for t in tarajim if t.get("source")],
        notes=llm_result.notes,
    )
```

- [ ] **Step 4: Run tests — confirm they PASS**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_narrator_summary_parsers.py -v
```

Expected: all 10 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/generate_narrator_summaries.py tests/test_narrator_summary_parsers.py
git commit -m "feat: add assemble_summary and full test coverage for parsers"
```

---

## Task 5: Add the CLI, main loop, and bulk-write

**Files:**
- Modify: `scripts/generate_narrator_summaries.py`

- [ ] **Step 1: Append CLI, main loop, and DB write to the script**

Append to `scripts/generate_narrator_summaries.py`:

```python
# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Generate structured Arabic summaries for narrator biographies.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--force", action="store_true",
                   help="Re-generate summaries even if summary field already exists.")
    p.add_argument("--dry-run", action="store_true",
                   help="Count pending narrators and exit without writing.")
    p.add_argument("--limit", type=int, default=0,
                   help="Process at most N narrators (0 = all). Useful for smoke tests.")
    p.add_argument("--db", type=str, default="HadithDataDev",
                   help="MongoDB database name (default: HadithDataDev).")
    p.add_argument("--uri", type=str, default="mongodb://localhost:27017/",
                   help="MongoDB URI (default: mongodb://localhost:27017/).")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    load_dotenv(_REPO_ROOT / ".env")
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not openrouter_key and not args.dry_run:
        sys.exit("ERROR: OPENROUTER_API_KEY not set in .env")

    client = MongoClient(args.uri)
    db = client[args.db]
    bio_col = db["processed_podia_narrator_biographies"]
    stats_col = db["analytics_narrator_stats_podia"]

    print(f"DB     : {args.db} @ {args.uri}")
    print(f"Output : {OUTPUT_PATH}")

    # Build lookup: rawi_id -> stats doc
    stats_by_id: dict[int, dict] = {}
    for doc in stats_col.find({}, {"_id": 0}):
        rid = doc.get("rawi_id")
        if rid is not None:
            stats_by_id[int(rid)] = doc

    # Build pending list
    query = {} if args.force else {"summary": {"$exists": False}}
    total_docs = bio_col.count_documents({})
    pending_docs = list(bio_col.find(query, {"_id": 0}))

    if args.limit > 0:
        pending_docs = pending_docs[: args.limit]

    print(f"Total narrators : {total_docs:,}")
    print(f"Pending         : {len(pending_docs):,}")

    if args.dry_run:
        print("\n[dry-run] No changes made.")
        client.close()
        return

    if not pending_docs:
        print("All narrators already have summaries.")
        client.close()
        return

    llm_chain = build_llm_chain(openrouter_key)

    n_ok = 0
    n_skipped = 0
    n_failed = 0
    ops: list[UpdateOne] = []

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_file = open(OUTPUT_PATH, "a", encoding="utf-8")

    try:
        for bio_doc in tqdm(pending_docs, unit="narrator", desc="Generating summaries"):
            rawi_id = bio_doc.get("rawi_id")
            if rawi_id is None:
                n_skipped += 1
                continue

            stats_doc = stats_by_id.get(int(rawi_id))

            llm_result = call_llm(
                llm_chain,
                full_name=bio_doc.get("full_name", ""),
                rank=bio_doc.get("rank", ""),
                tarajim=bio_doc.get("tarajim", []),
            )

            summary = assemble_summary(bio_doc, stats_doc, llm_result)
            if summary is None:
                tqdm.write(f"  [SKIP] rawi_id={rawi_id}: LLM failed")
                n_failed += 1
                continue

            summary_dict = summary.model_dump()
            ops.append(UpdateOne({"rawi_id": rawi_id}, {"$set": {"summary": summary_dict}}))

            row = {"rawi_id": rawi_id, "summary": summary_dict}
            out_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            out_file.flush()

            n_ok += 1

            # Flush bulk write every 50 to avoid large batches
            if len(ops) >= 50:
                bio_col.bulk_write(ops, ordered=False)
                ops = []

        if ops:
            bio_col.bulk_write(ops, ordered=False)

    finally:
        out_file.close()
        client.close()

    print(f"\n{'='*60}")
    print(f"  Generated : {n_ok:,}")
    print(f"  Skipped   : {n_skipped:,}  (missing rawi_id)")
    print(f"  Failed    : {n_failed:,}  (LLM error — re-run to retry)")
    print(f"  Output    : {OUTPUT_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run a dry-run to confirm the script connects and counts correctly**

```bash
/home/abdo_kamar/Projects/.venv/bin/python scripts/generate_narrator_summaries.py --dry-run
```

Expected output (approximate):
```
DB     : HadithDataDev @ mongodb://localhost:27017/
Output : .../narrator_summaries.jsonl
Total narrators : 1,780
Pending         : 1,780

[dry-run] No changes made.
```

- [ ] **Step 3: Run a smoke test on 2 narrators (requires OPENROUTER_API_KEY in .env)**

```bash
/home/abdo_kamar/Projects/.venv/bin/python scripts/generate_narrator_summaries.py --limit 2
```

Expected: progress bar completes for 2 narrators, summary written to `narrator_summaries.jsonl`, no errors.

- [ ] **Step 4: Verify the summary was written to MongoDB**

```bash
/home/abdo_kamar/Projects/.venv/bin/python << 'EOF'
from pymongo import MongoClient
import json
client = MongoClient("mongodb://localhost:27017/")
doc = client["HadithDataDev"]["processed_podia_narrator_biographies"].find_one(
    {"summary": {"$exists": True}}, {"summary": 1, "rawi_id": 1, "_id": 0}
)
print(json.dumps(doc, ensure_ascii=False, indent=2, default=str))
EOF
```

Expected: a document with a populated `summary` object containing `full_name`, `classification`, `hadith_count`, `transmission_stats`, `top_teachers`, `top_students`, `tarajim_sources`, and at least some of `kunya`/`era`/`location`/`notes`.

- [ ] **Step 5: Commit**

```bash
git add scripts/generate_narrator_summaries.py
git commit -m "feat: add CLI and main loop for narrator summary generation"
```

---

## Task 6: Update tasks.md

**Files:**
- Modify: `tasks.md`

- [ ] **Step 1: Add entry to tasks.md**

Find the `## v1.1` section (or the nearest appropriate section) and add:

```markdown
- [x] Generate structured Arabic summaries for narrator biographies
  - Script: `scripts/generate_narrator_summaries.py`
  - Adds `summary` field to `processed_podia_narrator_biographies` (1,780 narrators)
  - LLM: `google/gemini-3-flash-preview` via OpenRouter (kunya, era, location, notes)
  - Structured fields computed from `narrator_info` + `analytics_narrator_stats_podia`
  - Output: `mongo_migration/processed_bukhari_podia/narrator_summaries.jsonl`
```

- [ ] **Step 2: Run full test suite to confirm nothing is broken**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS including the new `test_narrator_summary_parsers.py`.

- [ ] **Step 3: Final commit**

```bash
git add tasks.md
git commit -m "docs: add narrator summary task to tasks.md"
```

---

## Self-Review

**Spec coverage:**
- ✓ Summary stored on `processed_podia_narrator_biographies` — Task 5 bulk-writes with `$set`
- ✓ Arabic structured output — Pydantic models in Task 1, Arabic prompt in Task 3
- ✓ LLM fields: kunya, era, location, notes — `LLMExtracted` in Task 1, `call_llm` in Task 3
- ✓ Structured fields: hadith_count, transmission_stats, top teachers/students, tarajim_sources — parsers in Task 2, assembly in Task 4
- ✓ `title_model` / `google/gemini-3-flash-preview` via OpenRouter — `build_llm_chain` in Task 3
- ✓ Resumable (skips existing unless `--force`) — `query` in Task 5
- ✓ Edge case: no tarajim → LLM returns empty `LLMExtracted()` — `call_llm` handles it in Task 3
- ✓ Edge case: no stats doc → teachers/students default to `[]` — `assemble_summary` handles it in Task 4
- ✓ Edge case: LLM fails → `assemble_summary` returns None, narrator skipped — Task 5 loop
- ✓ JSONL output to `mongo_migration/processed_bukhari_podia/narrator_summaries.jsonl` — Task 5
- ✓ CLI: `--force`, `--dry-run`, `--limit`, `--db`, `--uri` — Task 5
- ✓ `tasks.md` updated — Task 6

**Placeholder scan:** No TBDs or incomplete steps.

**Type consistency:**
- `LLMExtracted` defined in Task 1, used in Tasks 3 and 4 ✓
- `NarratorSummary` defined in Task 1, used in Tasks 4 and 5 ✓
- `TransmissionStats`, `NarratorRelation` defined in Task 1, used in Tasks 2 and 4 ✓
- `parse_hadith_count`, `parse_transmission_stats`, `build_top_relations` defined in Task 2, used in Task 4 ✓
- `assemble_summary` defined in Task 4, used in Task 5 ✓
- `call_llm`, `build_llm_chain` defined in Task 3, used in Task 5 ✓
