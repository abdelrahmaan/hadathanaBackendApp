# Narrator Comparison Support & rawi_id Hallucination Guardrail — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two prompt rules to prevent rawi_id hallucination and to force cited hadith counts on comparison questions.

**Architecture:** Prompt-only changes — two new rules appended to `ARABIC_SYSTEM_PROMPT` in `app/chatbot/prompts.py`. No new tools, no schema migrations, no SSE contract changes. Tests are regression guards (prompt-shape assertions + `_build_disambiguation` unit test).

**Tech Stack:** Python, pytest, LangChain `@tool`, Motor (mocked in tests)

---

## File Map

| File | Action | What changes |
|---|---|---|
| `app/chatbot/prompts.py` | Modify | Append Rule 9 and Rule 10 to `ARABIC_SYSTEM_PROMPT` |
| `tests/test_chatbot_narrator_tool.py` | Modify | Add 3 tests at the end of the file |
| `tasks.md` | Modify | Mark task in_progress → done |

No changes to `agent.py`, `router.py`, `session.py`, `models.py`, `database.py`.

---

### Task 1: Write the 3 failing tests

**Files:**
- Modify: `tests/test_chatbot_narrator_tool.py`

- [ ] **Step 1.1 — Add the three test functions at the bottom of the file**

Open `tests/test_chatbot_narrator_tool.py` and append these three tests after the last existing test (`test_search_narrators_mongo_error_returns_arabic_message`):

```python
# ── Prompt-rule regression guards ─────────────────────────────────────────────


def test_prompt_rule_9_present():
    """ARABIC_SYSTEM_PROMPT must contain Rule 9 forbidding fabricated rawi_id values."""
    from app.chatbot.prompts import ARABIC_SYSTEM_PROMPT

    # Rule 9 anchors the LLM to tool results; any edit removing these tokens is a regression.
    assert "NEVER write" in ARABIC_SYSTEM_PROMPT or "لا تكتب" in ARABIC_SYSTEM_PROMPT, (
        "Rule 9 (no fabricated rawi_id) is missing from ARABIC_SYSTEM_PROMPT"
    )
    assert "رقم الراوي" in ARABIC_SYSTEM_PROMPT, (
        "Rule 9 must mention 'رقم الراوي' to be effective"
    )


def test_prompt_rule_10_present():
    """ARABIC_SYSTEM_PROMPT must contain Rule 10 requiring two tool calls + cited counts."""
    from app.chatbot.prompts import ARABIC_SYSTEM_PROMPT

    assert "search_narrators" in ARABIC_SYSTEM_PROMPT, (
        "Rule 10 must reference 'search_narrators' to direct the LLM"
    )
    assert "عدد الأحاديث" in ARABIC_SYSTEM_PROMPT, (
        "Rule 10 must mention 'عدد الأحاديث' so the LLM knows what to cite"
    )


def test_disambiguation_text_contains_only_returned_rawi_ids():
    """
    _build_disambiguation must only emit rawi_id values from the input docs.
    This is a regression guard against the bug where the LLM cited rawi_id 1070
    (علي بن عبد الله بن عباس) instead of 1066 (علي بن أبي طالب).
    The function itself can't fabricate IDs — this test locks that invariant.
    """
    from app.chatbot.agent import _build_disambiguation

    narrators = [
        {
            "rawi_id": 1066,
            "full_name_plain": "علي بن أبي طالب بن عبد المطلب الهاشمي",
            "rank_plain": "صحابي",
        },
        {
            "rawi_id": 271,
            "full_name_plain": "الحسن بن الحسن بن علي بن أبي طالب الهاشمي المدني",
            "rank_plain": "صدوق",
        },
        {
            "rawi_id": 289,
            "full_name_plain": "الحسن بن محمد بن علي بن أبي طالب الهاشمي",
            "rank_plain": "ثقة",
        },
    ]
    biographies = [
        {
            "rawi_id": 1066,
            "summary": {"full_name": "علي بن أبي طالب", "kunya": "أبو الحسن", "classification": "صحابي"},
        },
        {
            "rawi_id": 271,
            "summary": {"full_name": "الحسن بن الحسن", "kunya": "أبو محمد", "classification": "صدوق"},
        },
        {
            "rawi_id": 289,
            "summary": {"full_name": "الحسن بن محمد", "kunya": "أبو محمد", "classification": "ثقة"},
        },
    ]
    bio_map = {b["rawi_id"]: b for b in biographies}
    pairs = [(n, bio_map.get(n["rawi_id"])) for n in narrators]

    _entries, text = _build_disambiguation(pairs)

    # Must contain the IDs that were passed in
    assert "1066" in text
    assert "271" in text
    assert "289" in text

    # Must NOT contain rawi_id 1070 (the hallucinated ID from the reported bug)
    assert "1070" not in text, (
        "_build_disambiguation must not emit rawi_id 1070 — it was not in the input"
    )
```

- [ ] **Step 1.2 — Run tests to verify they fail (RED)**

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m pytest tests/test_chatbot_narrator_tool.py::test_prompt_rule_9_present \
  tests/test_chatbot_narrator_tool.py::test_prompt_rule_10_present \
  tests/test_chatbot_narrator_tool.py::test_disambiguation_text_contains_only_returned_rawi_ids \
  -v
```

Expected: `test_prompt_rule_9_present` and `test_prompt_rule_10_present` FAIL with `AssertionError: Rule 9 (no fabricated rawi_id) is missing` (the rules don't exist yet). `test_disambiguation_text_contains_only_returned_rawi_ids` should PASS immediately (the function already only emits input IDs — this test is a regression lock, not a behavior change). If it fails, `_build_disambiguation` has a bug to fix first.

---

### Task 2: Add Rules 9 and 10 to the system prompt

**Files:**
- Modify: `app/chatbot/prompts.py`

- [ ] **Step 2.1 — Extend `ARABIC_SYSTEM_PROMPT` with the two new rules**

`app/chatbot/prompts.py` currently ends with Rule 8 as the last line inside the triple-quoted string:

```python
8. When search_narrators returns multiple narrators (a disambiguation list), present them to the user as-is and ask which one they mean. Do NOT pick one yourself, do NOT fabricate biography details — wait for the user to confirm by clicking a card or naming the narrator."""
```

Replace the closing `"""` of `ARABIC_SYSTEM_PROMPT` with the two new rules appended. The full updated string ending is:

```python
8. When search_narrators returns multiple narrators (a disambiguation list), present them to the user as-is and ask which one they mean. Do NOT pick one yourself, do NOT fabricate biography details — wait for the user to confirm by clicking a card or naming the narrator.
9. NEVER write a "رقم الراوي" value unless it appears verbatim in a tool result returned during THIS turn, OR it matches the session's current narrator set by a previous turn's tool result. Do NOT guess, infer, or pattern-match a rawi_id from memory or from names mentioned elsewhere in the conversation. If you do not have a confirmed rawi_id from a tool call, omit the number entirely.
10. For comparison questions ("أيهما أكثر رواية", "من أكثر روايةً X أم Y", "قارن بين X وY", etc.):
    a. Call search_narrators ONCE for each narrator mentioned — two separate calls.
    b. If either side returns a disambiguation list, present BOTH lists and ask the user which narrator they mean for each side. Do NOT proceed with the comparison until both sides are resolved to a single narrator.
    c. Once you have a confirmed single profile for each side, your answer MUST include the exact "عدد الأحاديث" from each tool result before stating which narrator has more. Use this format:
       "روى [اسم أ] (رقم الراوي: N) ما مجموعه [A] حديثاً في صحيح البخاري، بينما روى [اسم ب] (رقم الراوي: M) ما مجموعه [B] حديثاً، فـ[الأكثر رواية] هو [اسم الأكثر]."
    d. NEVER compare narrators using prior knowledge — only cite the numbers the tool returned."""
```

The full updated `app/chatbot/prompts.py` should be:

```python
ARABIC_SYSTEM_PROMPT = """You are a specialist assistant for Sahih al-Bukhari hadiths and narrators.

Rules:
0. For greetings, farewells, or non-Islamic small talk (e.g. "مرحبا", "شكراً", "كيف حالك", "hi", "hello"), respond naturally and briefly in Arabic — do NOT call any tool.
1. Tool selection:
   - For questions about hadith content, topics, rulings, or hadith text → call search_hadiths.
   - For questions about a narrator, their name, kunya, reliability grade (ثقة/ضعيف), biography, teachers, students, or chain membership → call search_narrators.
2. Answer ONLY from the results returned by the tool — never from prior knowledge. Each passage has a relevance score (0.0–1.0). Only cite passages that directly answer the user's question — do not cite tangentially related passages.
3. If the tool returns no relevant results, say explicitly:
   "لا يوجد في المصادر المُتاحة ما يجيب على سؤالك."
   (There is nothing in the available sources that answers your question.)
4. Each hadith passage is numbered [١], [٢], etc. with a relevance score. When referencing a passage, cite its Arabic numeral, e.g. [١] or [٢]. Do NOT cite a passage just because it shares a keyword with the question — it must provide direct evidence for your answer.
5. Never fabricate hadiths, narrator biographies, or any content from outside the tool results.
6. Respond in formal Arabic (فصحى).
7. At the very end of HADITH answers (only when search_hadiths was called), after all Arabic text, on a new line write exactly:
   REFS:[N,N,...]
   where N is the Western digit of each passage you cited (e.g. REFS:[1,3]).
   If you cited none, write REFS:[].
   Do NOT add a REFS line for narrator answers — narrator results are cited inline by رقم الراوي.
8. When search_narrators returns multiple narrators (a disambiguation list), present them to the user as-is and ask which one they mean. Do NOT pick one yourself, do NOT fabricate biography details — wait for the user to confirm by clicking a card or naming the narrator.
9. NEVER write a "رقم الراوي" value unless it appears verbatim in a tool result returned during THIS turn, OR it matches the session's current narrator set by a previous turn's tool result. Do NOT guess, infer, or pattern-match a rawi_id from memory or from names mentioned elsewhere in the conversation. If you do not have a confirmed rawi_id from a tool call, omit the number entirely.
10. For comparison questions ("أيهما أكثر رواية", "من أكثر روايةً X أم Y", "قارن بين X وY", etc.):
    a. Call search_narrators ONCE for each narrator mentioned — two separate calls.
    b. If either side returns a disambiguation list, present BOTH lists and ask the user which narrator they mean for each side. Do NOT proceed with the comparison until both sides are resolved to a single narrator.
    c. Once you have a confirmed single profile for each side, your answer MUST include the exact "عدد الأحاديث" from each tool result before stating which narrator has more. Use this format:
       "روى [اسم أ] (رقم الراوي: N) ما مجموعه [A] حديثاً في صحيح البخاري، بينما روى [اسم ب] (رقم الراوي: M) ما مجموعه [B] حديثاً، فـ[الأكثر رواية] هو [اسم الأكثر]."
    d. NEVER compare narrators using prior knowledge — only cite the numbers the tool returned."""

THREAD_RENAME_PROMPT = """Suggest a short title (5-7 Arabic words) that describes the following question.
Return the title only — no explanation, no punctuation.
Question: {question}"""
```

- [ ] **Step 2.2 — Run all three tests to verify they pass (GREEN)**

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m pytest tests/test_chatbot_narrator_tool.py::test_prompt_rule_9_present \
  tests/test_chatbot_narrator_tool.py::test_prompt_rule_10_present \
  tests/test_chatbot_narrator_tool.py::test_disambiguation_text_contains_only_returned_rawi_ids \
  -v
```

Expected: all 3 PASS.

- [ ] **Step 2.3 — Run the full test suite to confirm no regressions**

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m pytest tests/ -v
```

Expected: all previously passing tests still pass. If `test_chat_returns_429_when_daily_quota_exceeded` fails with `-1` — that is a pre-existing unrelated issue, not caused by this change.

- [ ] **Step 2.4 — Run ruff to confirm no lint errors**

```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m ruff check app/chatbot/prompts.py tests/test_chatbot_narrator_tool.py
```

Expected: no output (clean).

---

### Task 3: Restart dev container and manual verification

**Files:** none (runtime verification only)

- [ ] **Step 3.1 — Restart the dev API to pick up the prompt change**

```bash
docker compose restart api
```

Wait ~10 seconds, then verify it came up:

```bash
curl -s http://localhost:8001/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['status'], d['mongodb'])"
```

Expected output: `ok connected`

- [ ] **Step 3.2 — Test comparison question cites both hadith counts**

```bash
curl -s -N -X POST http://localhost:8001/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "أيهما روى أكثر علي بن أبي طالب أم عبد الله بن عباس؟"}'
```

Expected: SSE stream contains `103` (Ali's hadith count) and `739` (Ibn Abbas's hadith count). The `assistant_message_complete` event's `content` field should include both numbers before the conclusion sentence.

- [ ] **Step 3.3 — Test direct narrator lookup returns correct rawi_id**

```bash
curl -s -N -X POST http://localhost:8001/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "من هو علي بن أبي طالب؟"}'
```

Expected: `rawi_id` 1066 appears in the response. `1070` must NOT appear.

- [ ] **Step 3.4 — Test disambiguated comparison question**

```bash
curl -s -N -X POST http://localhost:8001/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "أيهما روى أكثر أبو هريرة أم عمر بن الخطاب؟"}'
```

Expected: The agent calls `search_narrators` twice. For Abu Hurayra it likely returns a single profile directly; for Umar it may show disambiguation if there are multiple matches. Response must include actual hadith counts from tool results, not from model memory.

---

### Task 4: Update tasks.md

**Files:**
- Modify: `tasks.md`

- [ ] **Step 4.1 — Mark the task done in tasks.md**

Find the entry for narrator comparison / rawi_id guardrail in `tasks.md` (add one if it doesn't exist yet under the current version). Update status to `done` and add touched files:

```
- [x] Narrator comparison support & rawi_id hallucination guardrail
      Files: app/chatbot/prompts.py, tests/test_chatbot_narrator_tool.py
      Added Rules 9 (no fabricated rawi_id) and 10 (comparison requires two tool calls + cited counts).
```

---

## Self-review

**Spec coverage:**
- Rule 9 (no fabricated rawi_id) → Task 2, Step 2.1 ✓
- Rule 10 (comparison two-calls + numbers) → Task 2, Step 2.1 ✓
- Test 1 `test_prompt_rule_9_present` → Task 1, Step 1.1 ✓
- Test 2 `test_prompt_rule_10_present` → Task 1, Step 1.1 ✓
- Test 3 `test_disambiguation_text_contains_only_returned_rawi_ids` → Task 1, Step 1.1 ✓
- Manual e2e verification → Task 3 ✓
- `tasks.md` update → Task 4 ✓

**Placeholder scan:** None found.

**Type consistency:** `_build_disambiguation` is imported directly in Task 1 test — matches actual function name in `agent.py`. No type mismatches.
