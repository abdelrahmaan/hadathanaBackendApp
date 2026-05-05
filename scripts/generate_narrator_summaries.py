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

LLM_MODEL = os.getenv("NARRATOR_SUMMARY_MODEL", "google/gemini-3-flash-preview")
MAX_LLM_ATTEMPTS = 3
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


# ---------------------------------------------------------------------------
# Structured-field parsers (no LLM, no DB)
# ---------------------------------------------------------------------------

import re as _re

_ARABIC_INDIC_TRANS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _extract_number(text: str) -> int:
    m = _re.search(r"\d+", text.translate(_ARABIC_INDIC_TRANS))
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
        NarratorRelation(rawi_id=r["rawi_id"], name=r["name"], freq=r.get("freq", 0))
        for r in sorted_rel[:n]
        if "rawi_id" in r and "name" in r
    ]


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
    max_attempts: int = MAX_LLM_ATTEMPTS,
) -> LLMExtracted | None:
    tarajim_text = "\n\n".join(
        t.get("tarjama_plain") or t.get("tarjama", "") for t in tarajim
    ).strip()

    if not tarajim_text:
        return LLMExtracted()  # all None — no text to extract from

    last_exc = None
    for attempt in range(max_attempts):
        try:
            return chain.invoke({
                "full_name": full_name,
                "rank": rank,
                "tarajim_text": tarajim_text,
            })
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                tqdm.write(f"  [LLM] retry {attempt + 1}/{max_attempts} in {delay:.1f}s — {exc}")
                time.sleep(delay)

    tqdm.write(f"  [LLM ERROR] giving up after {max_attempts} attempts: {last_exc}")
    return None


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
