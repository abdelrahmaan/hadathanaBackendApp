# Spec: Narrator Comparison Support & rawi_id Hallucination Guardrail

**Date:** 2026-05-08
**Status:** Approved for implementation
**Scope:** Prompt-only fixes — no new tools, no SSE contract changes, no schema migrations

---

## Context

Two bugs surfaced in production chatbot testing:

1. **rawi_id hallucination** — when the LLM answered a comparison question that followed a disambiguation turn for "علي بن أبي طالب", it cited `rawi_id: 1070` (علي بن عبد الله بن عباس — a different narrator) instead of `1066` (علي بن أبي طالب). The disambiguation list returned by the tool was correct; the error was the LLM pattern-matching against nearby text rather than reading the tool result verbatim.

2. **Comparison questions return no numbers** — "أيهما روى أكثر علي بن أبي طالب أم عبد الله بن عباس؟" received a qualitatively correct answer ("ابن عباس أكثر") but cited no hadith counts. The data exists (`summary.hadith_count`: 103 for Ali, 739 for Ibn Abbas) but the agent never fetched both profiles, so the LLM had nothing to cite.

Both bugs are prompt failures — the data is already returned by `search_narrators`; the LLM just wasn't told how to use it.

---

## Design

### Approach: Prompt-only (two new rules in `ARABIC_SYSTEM_PROMPT`)

No new tools, no new stash keys, no SSE `kind` changes, no router changes, no data migrations. The existing `search_narrators` tool already returns `hadith_count` and `transmission_stats` in its formatted text output — it just needs prompt rules to direct the LLM to use them correctly.

Risk: the LLM may occasionally forget the rules. If >10% of real comparison questions on production skip citing numbers, the follow-up escalation is a dedicated `compare_narrators` tool (out of scope for this spec).

---

## Rule 9 — No fabricated rawi_id

Add as Rule 9 to `ARABIC_SYSTEM_PROMPT`:

```
9. NEVER write a "رقم الراوي" value unless it appears verbatim in a tool result returned
   during THIS turn, OR it matches the session's current narrator set by a previous turn's
   tool result. Do NOT guess, infer, or pattern-match a rawi_id from memory or from names
   mentioned elsewhere in the conversation. If you do not have a confirmed rawi_id from a
   tool call, omit the number entirely.
```

**Why this works:** the disambiguation list already contains the correct `rawi_id` values (e.g. `رقم الراوي: 1066`). The rule anchors the LLM to read that literal text rather than inferring from surrounding context.

---

## Rule 10 — Comparison questions require two tool calls + cited numbers

Add as Rule 10 to `ARABIC_SYSTEM_PROMPT`:

```
10. For comparison questions ("أيهما أكثر رواية", "من أكثر روايةً X أم Y", "قارن بين X وY", etc.):
    a. Call search_narrators ONCE for each narrator mentioned — two separate calls.
    b. If either side returns a disambiguation list, present BOTH lists and ask the user
       which narrator they mean for each side. Do NOT proceed with the comparison until
       both sides are resolved to a single narrator.
    c. Once you have a confirmed single profile for each side, your answer MUST include
       the exact "عدد الأحاديث" from each tool result before stating which narrator
       has more. Use this format:
         "روى [اسم أ] (رقم الراوي: N) ما مجموعه [A] حديثاً في صحيح البخاري،
          بينما روى [اسم ب] (رقم الراوي: M) ما مجموعه [B] حديثاً،
          فـ[الأكثر رواية] هو [اسم الأكثر]."
    d. NEVER compare narrators using prior knowledge — only cite the numbers the tool returned.
```

**Why the format template:** the previous failure mode was the LLM skipping the numbers entirely and jumping to a qualitative conclusion. A required sentence template forces it to surface `hadith_count` before drawing a conclusion.

---

## Files to modify

| File | Change |
|---|---|
| `app/chatbot/prompts.py` | Add Rule 9 and Rule 10 to `ARABIC_SYSTEM_PROMPT` |
| `tests/test_chatbot_narrator_tool.py` | Add 3 tests (see Testing section) |
| `tasks.md` | Mark task in_progress → done, log touched files |
| `CLAUDE.md` | One-line note in chatbot prompt section about Rules 9 & 10 |

**No changes to:** `agent.py`, `router.py`, `session.py`, `models.py`, `database.py`, any migration scripts, `.env.example`, SSE contract.

---

## Testing

### TDD approach

Tests are prompt-shape assertions + one behavioral smoke test. Full LLM-behavior tests require API keys and are marked `@pytest.mark.skipif(not api_key, reason="needs OPENROUTER_API_KEY")`.

**Test 1 — `test_prompt_rule_9_present`**
Assert `ARABIC_SYSTEM_PROMPT` contains the string `"رقم الراوي"` near the word `"NEVER"` (or the Arabic equivalent). Simple presence check — prevents accidental removal of the rule.

**Test 2 — `test_prompt_rule_10_present`**
Assert `ARABIC_SYSTEM_PROMPT` contains `"search_narrators ONCE"` or equivalent Arabic indicator of the two-call requirement, and contains `"عدد الأحاديث"`.

**Test 3 — `test_disambiguation_text_contains_only_returned_rawi_ids`**
Given a fixture of narrator docs with `rawi_id` values `[1066, 271, 289]`, call `_build_disambiguation` and assert the returned text contains `"1066"`, `"271"`, `"289"` and does NOT contain `"1070"`. This is a regression guard: `_build_disambiguation` must only emit IDs that came from the tool, not invent them.

### Manual end-to-end verification

```bash
# Comparison — must cite both numbers
curl -s -X POST http://localhost:8001/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "أيهما روى أكثر علي بن أبي طالب أم عبد الله بن عباس؟"}' | \
  grep -E "103|739"
# Expected: output contains both 103 (Ali) and 739 (Ibn Abbas)

# rawi_id accuracy — must return 1066, NOT 1070
curl -s -X POST http://localhost:8001/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "من هو علي بن أبي طالب؟"}' | grep "1066"
# Expected: 1066 appears, 1070 does NOT

# Comparison after disambiguation — both IDs must be correct
curl -s -X POST http://localhost:8001/api/v2/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "أيهما روى أكثر أبو هريرة أم عمر بن الخطاب؟"}' | \
  grep -E "rawi_id|عدد الأحاديث"
# Expected: both rawi_ids from tool results, not guessed
```

---

## Out of scope

- `compare_narrators` tool (escalation path if prompt proves unreliable)
- Top-N / superlative questions ("من أكثر الصحابة رواية؟")
- Relationship questions ("هل سمع X من Y مباشرة؟")
- Filtered narrator lists ("الرواة الثقات في القرن الثاني")
- Frontend side-by-side comparison card (future FE work)
