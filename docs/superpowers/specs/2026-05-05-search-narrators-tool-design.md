# Design: search_narrators Tool for Chatbot Agent

**Date:** 2026-05-05
**Branch:** feat-enhance_chatbot
**Status:** Approved for implementation

---

## Context

The chatbot agent currently has one tool — `search_hadiths` — which retrieves hadith passages from Qdrant via hybrid semantic search. Users are now asking questions about narrators (رواة) — their reliability, biography, teachers/students — and the agent has no tool to answer them. Narrator data already exists in MongoDB (`processed_podia_narrators` + `processed_podia_narrator_biographies`) and is served via the REST API, but the agent cannot access it. This feature adds a second tool to bridge that gap.

**Pre-computed `summary` field is the primary data source.** The narrator summary feature (already shipped — see [docs/superpowers/specs/2026-05-05-narrator-summary-design.md](docs/superpowers/specs/2026-05-05-narrator-summary-design.md)) added a structured `summary` field to every document in `processed_podia_narrator_biographies` (100% coverage, 1,780 narrators). The summary contains: `kunya`, `era`, `location`, `classification`, `hadith_count`, `notes` (scholarly opinion synthesis), `transmission_stats`, `top_teachers`, `top_students`, `tarajim_sources`. The chatbot tool uses this compact summary instead of shipping the raw `tarajim[]` array (10×–50× smaller, already LLM-synthesized).

---

## Narrator Context State (follow-up questions)

When a user clicks a disambiguation card, the frontend sends `rawi_id` to the agent. The agent fetches the full narrator profile + biography and returns it. For follow-up questions about the **same narrator** in the same session (e.g. "ما آراء العلماء فيه؟"), re-fetching from MongoDB every turn is wasteful.

**Solution: store `current_rawi_id` on the `ChatSession` MongoDB document.** This fits the existing stateless-agent pattern — the router reads `current_rawi_id` from the session and injects it as context into the agent's input at the start of each turn, exactly like history messages. No LangGraph custom state, no checkpointer changes needed.

**Flow:**
```
User clicks card → frontend sends rawi_id=1398
    ↓
Router calls search_narrators(rawi_id=1398)
    ↓
Tool fetches narrator + bio, returns formatted result
    ↓
Router: session.current_rawi_id = 1398  (stored in Mongo via $set)
    ↓
Next user turn: "ما رأي ابن حجر فيه؟"
    ↓
Router injects into history: "الراوي الحالي: rawi_id=1398 — محمد بن مسلم الزهري"
    ↓
Agent reads context, calls search_narrators(rawi_id=1398) to get full bio again
    ↓
Answers from biography text
```

`current_rawi_id` is reset to `None` when the user asks a non-narrator question (hadith question or small talk).

---

## Architecture

The tool lives in `app/chatbot/agent.py` alongside `search_hadiths`. It queries MongoDB directly — no Qdrant, no embeddings, no new infrastructure. The `rawi_id` is the authoritative identifier that bridges the two narrator collections.

```
User question: "من هو الزهري؟"  (or kunya: "من هو أبو حفص؟")
    ↓
Agent calls: search_narrators(query="الزهري")
    ↓
normalize_for_search(query)               # app/normalization.py — strips tashkeel, unifies hamza, etc.
    ↓
TWO PARALLEL MongoDB queries (asyncio.gather):
  A. processed_podia_narrators — $or regex on:
       - name_in_chain_plain
       - full_name_plain
  B. processed_podia_narrator_biographies — regex on:
       - summary.kunya
    ↓
Merge + dedupe by rawi_id (names hits win, kunya-only hits fetched as full narrator docs)
Cap at 6 unique narrators
    ↓
For each merged hit: fetch summary from processed_podia_narrator_biographies (projection: summary, tarajim)
    ↓
If 1 match  → return full profile from summary
If 2-6 matches → return disambiguation list (name + kunya + classification + rawi_id + URL)
    ↓
Formatted string returned to LLM
```

**Direct ID lookup path** (when frontend sends `rawi_id` from a card click):
```
Agent calls: search_narrators(rawi_id=1398)
    ↓
Direct MongoDB lookup by rawi_id in processed_podia_narrators
    ↓
Fetch biography from processed_podia_narrator_biographies by rawi_id
    ↓
Return full profile + biography (no disambiguation needed)
```

---

## Tool Signature

```python
@tool
async def search_narrators(
    query: str | None = None,
    rawi_id: int | None = None,
    config: RunnableConfig = None,
) -> str:
    """
    Search Sahih al-Bukhari narrators by name, nickname, or ID.
    Use when the user asks about a narrator, transmitter, their reliability grade,
    biography, or chain of transmission.

    If rawi_id is provided, fetch that narrator directly.
    If query is provided, search by name or nickname across both name fields.

    IMPORTANT — when extracting `query` from the user's question, pass ONLY
    the narrator's name or nickname. Strip honorifics and filler words such as:
      - الشيخ، الإمام، الحافظ، العلامة، السيد، الصحابي، التابعي
      - اعطني، اخبرني، معلومات عن، من هو، أريد أن أعرف
    Examples:
      User: "اعطني معلومات عن الشيخ الزهري"  → query="الزهري"
      User: "من هو الإمام مالك؟"            → query="مالك"
      User: "أخبرني عن أبي هريرة"           → query="أبو هريرة"
    """
```

**Rules:**
- `rawi_id` takes priority over `query` if both are provided
- Both `query` and `rawi_id` being None → return an error string asking the user to provide a name
- Normalize `query` via `normalize_for_search()` before regex (same pattern as all other search in the codebase)
- Regex is case-insensitive substring match (`$options: "i"`)

---

## Disambiguation Flow

### Single match (or rawi_id lookup)
Return formatted narrator profile built **from the `summary` field** (compact, pre-synthesized — never ships the raw `tarajim[]` array):
```
الاسم: محمد بن مسلم بن عبيد الله الزهري
الكنية: أبو بكر
العصر: القرن الثاني الهجري
المكان: المدينة المنورة
الدرجة: ثقة حافظ متقن
رقم الراوي: 1398
الصفحة: https://www.hadathana.app/narrator?nid=1398

عدد الأحاديث: 1234
إحصائيات الرواية: متصلة 1100 / معلقة 100 / مختلف فيها 34 (المجموع: 1234)

أبرز شيوخه: سعيد بن المسيب (٢١٠)، عروة بن الزبير (١٨٠)، ...
أبرز تلاميذه: مالك بن أنس (٤٠٠)، سفيان بن عيينة (٣٠٠)، ...

رأي العلماء (notes):
ثقة حافظ متقن، إمام في الحديث والفقه. اتفق العلماء على توثيقه...

مصادر التراجم: تهذيب الكمال، الكاشف، سير أعلام النبلاء
```

**Fallback:** If `summary` is null on a narrator (small percentage), fall back to: name fields from `processed_podia_narrators` + first `tarjama_plain` from `tarajim[0]` (truncated to 800 chars).

### Multiple matches (2–6)
Return disambiguation list only — uses summary fields (`classification`, `kunya`, `era`) for richer cards:
```
وجدت عدة رواة بهذا الاسم، أيهم تقصد؟

[١] عبدالله بن عمر بن الخطاب — أبو عبدالرحمن — ثقة — رقم الراوي: 521
    الصفحة: https://www.hadathana.app/narrator?nid=521

[٢] عبدالله بن عمر بن حفص — أبو عثمان — ضعيف — رقم الراوي: 530
    الصفحة: https://www.hadathana.app/narrator?nid=530

[٣] عبدالله بن عمر العمري — — ضعيف — رقم الراوي: 544
    الصفحة: https://www.hadathana.app/narrator?nid=544

إذا لم يكن أيٌّ منهم، يمكنك طلب "عرض المزيد".
```

The frontend renders each `[N]` entry as a clickable card. Clicking sends back `rawi_id=<int>` to the agent, triggering the direct ID lookup path.

**Pagination:** The tool fetches up to 6 results internally. First 3 shown as cards. If the user clicks "عرض المزيد", the frontend sends offset=3 (or a re-query) and the agent calls `search_narrators(query=..., offset=3)` — implementation detail deferred to frontend/future iteration. For V1, show first 3 and the "عرض المزيد" option as a text prompt.

---

## Prompt Update (`prompts.py`)

`ARABIC_SYSTEM_PROMPT` gets two targeted changes:

1. **Rule 1 updated** — distinguish tool usage:
   - `search_hadiths` → questions about hadith content, topics, rulings, or hadith text
   - `search_narrators` → questions about a narrator, their name, reliability grade (ثقة/ضعيف), biography, teachers, students, or chain membership

2. **New rule for disambiguation** — when `search_narrators` returns multiple results, present them as a numbered list and ask the user to confirm which narrator they mean before giving a full answer.

3. **No REFS for narrator answers** — the `REFS:[N,N,...]` citation format is hadith-only. Narrator answers cite the `rawi_id` inline in the formatted output.

---

## Files to Change

| File | What changes |
|---|---|
| `app/chatbot/agent.py` | Add `search_narrators` tool inside `build_agent()`; add to `create_agent([search_hadiths, search_narrators])` |
| `app/chatbot/prompts.py` | Update Rule 1 to distinguish tool selection; add disambiguation rule |
| `app/chatbot/models.py` | Add `current_rawi_id: int | None = None` field to `ChatSession` |
| `app/chatbot/session.py` | Add `update_current_narrator(db, session_id, rawi_id)` function |
| `app/chatbot/router.py` | Inject `current_rawi_id` context into history; call `update_current_narrator` after narrator turns |
| `app/database.py` | Add `get_podia_narrators_collection(db)` and `get_podia_narrator_biographies_collection(db)` accessors |

**Files NOT changed:** `retriever.py`, `indexer.py`, `qdrant.py` — narrator answers do not go through the Qdrant/citation pipeline.

---

## Database Queries

**Name + kunya search — two parallel queries deduped by `rawi_id`:**

Queries run via `asyncio.gather`:

1. Names query on `processed_podia_narrators`:
```python
narrators_cursor = narrators_collection.find(
    {"$or": [
        {"name_in_chain_plain": {"$regex": pattern, "$options": "i"}},
        {"full_name_plain":     {"$regex": pattern, "$options": "i"}},
    ]},
    limit=6,
)
```

2. Kunya query on `processed_podia_narrator_biographies`:
```python
kunya_cursor = bio_collection.find(
    {"summary.kunya": {"$regex": pattern, "$options": "i"}},
    {"rawi_id": 1},   # only need rawi_id to fetch full narrator doc
    limit=6,
)
```

**Why two collections?** `processed_podia_narrators` has no kunya field. The kunya only exists at `processed_podia_narrator_biographies.summary.kunya` (LLM-extracted during summary generation). Measured coverage: **35.9% of narrators have a kunya that is NOT embedded in `full_name_plain`** — including عمر بن الخطاب (أبو حفص) and عائشة (أم عبد الله). Searching only the names collection would miss them entirely.

**Merge logic:**
```python
narrators_hits = await narrators_cursor.to_list(length=6)
kunya_hits     = await kunya_cursor.to_list(length=6)

# Dedupe by rawi_id, names-collection results win priority
seen = set()
merged = []
for doc in narrators_hits:
    if doc["rawi_id"] not in seen:
        seen.add(doc["rawi_id"])
        merged.append(doc)
for hit in kunya_hits:
    if hit["rawi_id"] not in seen and len(merged) < 6:
        # fetch full narrator doc by rawi_id
        nar = await narrators_collection.find_one({"rawi_id": hit["rawi_id"]})
        if nar:
            seen.add(hit["rawi_id"])
            merged.append(nar)
# merged has up to 6 unique narrator docs
```

`re.escape(normalize_for_search(query))` is applied once and used for both queries.

**Direct ID lookup** (`processed_podia_narrators`):
```python
doc = await collection.find_one({"rawi_id": rawi_id})
```

**Summary fetch** (`processed_podia_narrator_biographies`) — projection to keep payload small:
```python
bio = await bio_collection.find_one(
    {"rawi_id": doc["rawi_id"]},
    {"summary": 1, "tarajim": 1},   # tarajim only used as fallback
)
summary = bio.get("summary") if bio else None
# summary fields: kunya, era, location, classification, hadith_count, notes,
#                 transmission_stats, top_teachers, top_students, tarajim_sources
```

The tool formats the response **from `summary`**. Only if `summary` is null does it fall back to `tarajim[0].tarjama_plain[:800]`.

---

## Error Handling

| Case | Response |
|---|---|
| No query and no rawi_id | `"يرجى ذكر اسم الراوي أو رقمه."` |
| Name search → 0 results | `"لم يُعثر على راوٍ بهذا الاسم في المصادر المُتاحة."` |
| rawi_id not found | `"لم يُعثر على راوٍ بالرقم المطلوب."` |
| Biography not found for rawi_id | Return profile only, note biography unavailable |
| MongoDB error | Log + return `"حدث خطأ أثناء البحث. يرجى المحاولة مرة أخرى."` |

---

## Verification

1. **Unit test** — `tests/test_chatbot_narrator_tool.py`:
   - Mock `processed_podia_narrators` + `processed_podia_narrator_biographies` collections
   - Test: single match returns full profile + biography
   - Test: multiple matches returns disambiguation list with rawi_ids
   - Test: rawi_id lookup returns exact narrator
   - Test: unknown name returns Arabic no-results message
   - Test: nickname search (e.g. "الزهري") matches via `full_name_plain` substring

2. **Integration smoke test** (dev stack running):
   ```bash
   curl -X POST http://localhost:8001/api/v2/chat \
     -H "Content-Type: application/json" \
     -d '{"question": "من هو الزهري؟"}'
   # Expect: disambiguation or full biography in SSE stream
   
   curl -X POST http://localhost:8001/api/v2/chat \
     -H "Content-Type: application/json" \
     -d '{"question": "ابن عمر"}'
   # Expect: disambiguation list with multiple narrators
   ```

3. **Prompt routing check** — ask a hadith question, confirm agent calls `search_hadiths` not `search_narrators`.

4. **Direct ID lookup** — send `rawi_id=1398`, confirm Zuhri's full biography is returned without disambiguation.
