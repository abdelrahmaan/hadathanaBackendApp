# Narrator Summary Field — Design Spec

**Date:** 2026-05-05  
**Branch:** feat-enhance_chatbot  
**Status:** Approved

---

## Goal

Add a `summary` field to every document in `processed_podia_narrator_biographies`. The summary is a structured Arabic object combining LLM-extracted biographical details (kunya, era, location, scholarly notes) with programmatically-assembled structured data (hadith counts, transmission stats, top teachers/students, tarajim sources).

---

## Architecture

A standalone offline script `scripts/generate_narrator_summaries.py`, following the same pattern as `scripts/tag_topics_jsonl.py`:

- Reads source data from MongoDB (`processed_podia_narrator_biographies` + `analytics_narrator_stats_podia`)
- Calls LLM once per narrator (structured output via Pydantic)
- Writes results back to `processed_podia_narrator_biographies` via `bulk_write` with `$set` — never replaces the document
- Resumable: skips narrators where `summary` already exists unless `--force` is passed
- Logs progress and final `matched / modified` counts

---

## Data Flow

For each narrator (1,780 total):

1. **Fetch** biography doc from `processed_podia_narrator_biographies` (`tarajim`, `narrator_info`, `rank`, `full_name`)
2. **Fetch** analytics doc from `analytics_narrator_stats_podia` by `rawi_id` (may be absent for ~93 narrators)
3. **Assemble structured fields** (no LLM):
   - `hadith_count` — parsed from `narrator_info[action=get_matn_entries].text`
   - `transmission_stats` — parsed from `narrator_info` entries (total, connected, suspended, disputed)
   - `top_teachers` — top 5 by `freq` from analytics `teachers[]`
   - `top_students` — top 5 by `freq` from analytics `students[]`
   - `tarajim_sources` — list of `source` strings from `tarajim[]`
   - `classification` — taken directly from `rank` field
4. **LLM call** (structured output, Arabic): feed `full_name`, `rank`, and concatenated `tarajim[].tarjama_plain` text; extract `kunya`, `era`, `location`, `notes`
5. **Merge** LLM output + structured fields into final `NarratorSummary`
6. **Write** via `UpdateOne({"rawi_id": ...}, {"$set": {"summary": summary.model_dump()}}, upsert=False)`

---

## Summary Schema

```python
class TransmissionStats(BaseModel):
    total: int
    connected: int       # الروايات الموصولة
    suspended: int       # الروايات المعلقة
    disputed: int        # الروايات المختلف فيها بين رواة الصحيح

class NarratorRelation(BaseModel):
    rawi_id: int
    name: str
    freq: int            # distinct hadiths where this pair co-appears

class NarratorSummary(BaseModel):
    full_name: str                         # from document directly
    kunya: str | None                      # LLM-extracted (e.g. "أبو محمد")
    era: str | None                        # LLM-extracted (e.g. "القرن 1 هـ")
    location: str | None                   # LLM-extracted (e.g. "المدينة المنورة")
    classification: str                    # from rank field directly
    hadith_count: int                      # parsed from narrator_info
    transmission_stats: TransmissionStats  # parsed from narrator_info
    top_teachers: list[NarratorRelation]   # top 5 by freq, from analytics
    top_students: list[NarratorRelation]   # top 5 by freq, from analytics
    tarajim_sources: list[str]             # source names only, from tarajim[]
    notes: str | None                      # LLM-generated Arabic summary of scholarly opinions
```

The Pydantic model is used with LangChain's `with_structured_output()` on the `title_model` (`google/gemini-3-flash-preview` via OpenRouter).

---

## LLM Prompt (Arabic)

The LLM is given only the fields it needs to infer — structured fields are not included in the prompt:

```
أنت باحث في علم الحديث. فيما يلي معلومات عن راوٍ من رواة صحيح البخاري.

الاسم: {full_name}
الرتبة: {rank}

التراجم:
{tarajim_text}

استخرج المعلومات التالية باللغة العربية الفصحى:
- kunya: الكنية (مثل: أبو محمد) — إذا لم تُذكر فاترك الحقل فارغاً
- era: العصر (مثل: القرن 1 هـ) — إذا لم يُذكر فاترك الحقل فارغاً
- location: المدينة أو البلد (مثل: المدينة المنورة) — إذا لم يُذكر فاترك الحقل فارغاً
- notes: فقرة موجزة (3-5 جمل) تلخّص أبرز ما قاله العلماء في هذا الراوي من توثيق أو تجريح أو مزايا علمية
```

---

## Edge Cases

| Case | Count | Handling |
|---|---|---|
| No `tarajim` entries | ~45 narrators | LLM fields default to `None`; structured fields still populated |
| No analytics stats doc | ~93 narrators | `top_teachers`/`top_students` = `[]`; `hadith_count` from `narrator_info` |
| Both missing | Rare | Summary still written with whatever is available |
| LLM returns invalid structure | Possible | Catch `ValidationError`, log warning, skip narrator (retry on next run) |

---

## Storage

- **Collection:** `processed_podia_narrator_biographies`
- **Field added:** `summary` (nested object, all Arabic content)
- **Write method:** `bulk_write` with `UpdateOne` + `$set` — safe merge, never destroys existing fields
- **JSONL output:** `narrator_summaries.jsonl` written to `mongo_migration/processed_bukhari_podia/` alongside other processed files, for R2 backup and audit trail. Follows the same data file pattern (gitignored by `.gitignore`, registered for change tracking via `git add --intent-to-add --force`)

---

## Script CLI

```bash
# Run against HadithDataDev (default)
/home/abdo_kamar/Projects/.venv/bin/python scripts/generate_narrator_summaries.py

# Force re-generate all (even if summary exists)
/home/abdo_kamar/Projects/.venv/bin/python scripts/generate_narrator_summaries.py --force

# Custom DB
/home/abdo_kamar/Projects/.venv/bin/python scripts/generate_narrator_summaries.py --db HadithData --uri mongodb://localhost:27018/
```

---

## Model

`settings.title_model` (`google/gemini-3-flash-preview`) via OpenRouter — same model used for chat thread title generation. Accessed via `init_chat_model(settings.title_model, base_url="https://openrouter.ai/api/v1", api_key=settings.openrouter_api_key)`.

---

## Out of Scope

- No API endpoint changes in this spec (the existing `GET /api/v2/narrators/{rawi_id}/tarajem` already returns the full biography doc; `summary` will appear automatically once added)
- No Qdrant re-sync needed (summary is not embedded)
- No JSONL preprocessing changes

---

## Files Touched

| File | Change |
|---|---|
| `scripts/generate_narrator_summaries.py` | New script |
| `mongo_migration/processed_bukhari_podia/narrator_summaries.jsonl` | New output file (gitignored, R2 backup) |
| `tasks.md` | Add task entry |
