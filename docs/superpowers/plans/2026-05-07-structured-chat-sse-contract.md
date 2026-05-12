# Plan — Structured SSE Contract for Chat Responses

**Date:** 2026-05-07
**Goal:** Add a `kind` discriminator + structured per-kind payloads to `POST /api/v2/chat` so the FE can render hadith answers, narrator profiles, and disambiguation cards directly from JSON instead of regex-parsing Arabic prose.

**Status:** Contract agreed with FE. This plan covers the BE implementation.

---

## Final agreed contract

Four kinds: `hadith_answer`, `narrator_profile`, `narrator_disambiguation`, `chitchat`.

### `assistant_message_start` (new field: `kind`)

```json
{
  "type": "assistant_message_start",
  "session_id": "<uuid, only on new sessions>",
  "kind": "hadith_answer | narrator_profile | narrator_disambiguation | chitchat"
}
```

`start.kind` is an **optimization hint** — FE picks the UI shell early. `_complete.kind` is authoritative; if they differ, FE swaps shells and discards prematurely-rendered structured fields.

### `assistant_message_complete`

`hadith_answer`:
```json
{ "type": "assistant_message_complete",
  "data": { "kind": "hadith_answer", "content": "...", "citations": [...] } }
```

`chitchat`:
```json
{ "type": "assistant_message_complete",
  "data": { "kind": "chitchat", "content": "...", "citations": [] } }
```

`narrator_disambiguation`:
```json
{ "type": "assistant_message_complete",
  "data": {
    "kind": "narrator_disambiguation",
    "content": "...",
    "citations": [],
    "narrators": [
      { "rawi_id": 1772, "full_name": "...", "kunya": "أبو سلمة", "classification": "ثقة مكثر" }
    ]
  } }
```

`narrator_profile`:
```json
{ "type": "assistant_message_complete",
  "data": {
    "kind": "narrator_profile",
    "content": "...",
    "citations": [],
    "narrator": {
      "rawi_id": 1398,
      "full_name": "محمد بن مسلم الزهري",
      "kunya": "أبو بكر",
      "era": "القرن الثاني الهجري",
      "location": "المدينة المنورة",
      "classification": "ثقة حافظ متقن",
      "hadith_count": 1234,
      "transmission_stats": { "connected": 1100, "suspended": 100, "disputed": 34, "total": 1234 },
      "top_teachers": [{ "name": "...", "freq": 210 }],
      "top_students": [{ "name": "...", "freq": 400 }],
      "scholarly_opinion": "...",
      "sources": "تهذيب الكمال، سير أعلام النبلاء",
      "profile_url": "https://www.hadathana.app/narrator?nid=1398"
    }
  } }
```

**Guarantees on `narrator_profile.narrator`:** `rawi_id`, `full_name`, `profile_url` always non-null. Everything else is `T | null`. If even those three core fields are missing, BE downgrades to `chitchat`.

**Naming locked:** `classification` (not `rank`), `freq` (not `count`).

---

## Architecture: how `kind` is determined

The current router uses a **REFS-presence heuristic** on the LLM's text output to update narrator state. That works for state tracking but is too brittle to be the *classification* signal. We replace it with **structured payloads stashed by the tools**, mirroring the existing `_last_docs` pattern.

### New stash in [app/chatbot/agent.py](app/chatbot/agent.py)

```python
# Stash structured narrator payload per thread_id — populated by search_narrators,
# consumed by the router to build the SSE response. Mirrors _last_docs.
_last_narrator: dict[str, NarratorPayload] = {}

def get_last_narrator(thread_id: str) -> NarratorPayload | None:
    return _last_narrator.pop(thread_id, None)
```

Where `NarratorPayload` is one of:
- `{"mode": "single", "narrator": NarratorProfile}` → drives `narrator_profile`
- `{"mode": "disambiguation", "narrators": list[NarratorEntry]}` → drives `narrator_disambiguation`

### Classification rules in router (priority order)

1. `_last_docs[thread_id]` non-empty + REFS line in output → `hadith_answer`
2. `_last_narrator[thread_id]` populated + `mode=disambiguation` → `narrator_disambiguation`
3. `_last_narrator[thread_id]` populated + `mode=single` → `narrator_profile`
4. Both stashes populated → hadith wins (REFS line is the strong signal)
5. Neither stash populated **OR** stash populated but core fields missing → `chitchat`

This drops the REFS-heuristic from being load-bearing for *kind*, but keeps it for narrator state tracking (`update_current_narrator`) since that decision is independent.

### Emitting `start.kind` before streaming

Currently the router emits `assistant_message_start` *before* `agent.astream` runs, so the tool hasn't fired yet. To emit `kind` in start we have two options:

**Option A — Two-phase streaming (recommended):**
1. Emit `start` *without* `kind` immediately (preserves current latency for the spinner).
2. After the first tool call resolves and stashes its payload (still before the LLM begins streaming the final response), emit a separate `kind_hint` event:
   ```json
   { "type": "kind_hint", "kind": "narrator_profile" }
   ```
3. FE swaps shell on `kind_hint`, then again (if needed) on `_complete`.

**Option B — Buffer the stream:**
Run the agent with `stream_mode="updates"` to detect tool completion, *then* start forwarding `content` events. Adds latency = (tool duration) before first token reaches FE.

**Decision: Option A.** Adds one event type but preserves first-token latency. FE already handles `_complete` overriding earlier state; `kind_hint` is just an earlier hint of the same kind.

> **Update to FE contract:** `start.kind` becomes a separate `kind_hint` event. Same semantics — hint, not authoritative. Ask FE to confirm before BE codes.

---

## Step 1 — New Pydantic models in [app/chatbot/models.py](app/chatbot/models.py)

```python
from typing import Literal

ChatKind = Literal["hadith_answer", "narrator_profile", "narrator_disambiguation", "chitchat"]

class TransmissionStats(BaseModel):
    connected: int
    suspended: int
    disputed: int
    total: int

class NarratorRelation(BaseModel):
    name: str
    freq: int

class NarratorProfile(BaseModel):
    rawi_id: int                                  # required
    full_name: str                                # required
    profile_url: str                              # required
    kunya: str | None = None
    era: str | None = None
    location: str | None = None
    classification: str | None = None
    hadith_count: int | None = None
    transmission_stats: TransmissionStats | None = None
    top_teachers: list[NarratorRelation] = []
    top_students: list[NarratorRelation] = []
    scholarly_opinion: str | None = None
    sources: str | None = None

class NarratorEntry(BaseModel):                   # for disambiguation cards
    rawi_id: int
    full_name: str
    kunya: str | None = None
    classification: str | None = None
```

These live alongside `Citation`. They drive both the SSE payloads and the (optional) future REST endpoint that returns the same shape.

---

## Step 2 — Refactor `search_narrators` in [app/chatbot/agent.py](app/chatbot/agent.py)

The tool currently returns a single Arabic string (used by both LLM and disambiguation rendering). We need to keep that string for the LLM but **also stash the structured payload** for the router.

### Plan

1. Add `_last_narrator: dict[str, NarratorPayload]` module-level (mirrors `_last_docs`).
2. Refactor `_format_narrator_full` and `_format_disambiguation` to also return structured data:
   - `build_narrator_profile(narrator, biography) -> tuple[NarratorProfile, str]` — returns `(payload, formatted_arabic_string)`
   - `build_disambiguation(pairs) -> tuple[list[NarratorEntry], str]`
3. Inside `search_narrators`, after building the text response:
   - On `rawi_id` lookup or single match: stash `{"mode": "single", "narrator": NarratorProfile}`
   - On 2+ matches: stash `{"mode": "disambiguation", "narrators": [NarratorEntry, ...]}`
   - On no results / error: stash nothing (router falls through to `chitchat`)
4. Tool needs `RunnableConfig` to read `thread_id` for stashing. Add it like `search_hadiths` does:
   ```python
   async def search_narrators(
       query: str | None = None,
       rawi_id: int | None = None,
       config: RunnableConfig = None,
   ) -> str:
       thread_id = (config.get("configurable") or {}).get("thread_id", "")
       ...
       if thread_id:
           _last_narrator[thread_id] = payload
   ```

### Empty-state guard

When building `NarratorProfile`, validate the three required fields are present. If `rawi_id`, `full_name`, or `profile_url` are missing/null, **don't stash** — the router will classify as `chitchat`. Log a warning (`narrator_profile_incomplete`) so we catch data gaps.

---

## Step 3 — Router changes in [app/chatbot/router.py](app/chatbot/router.py)

### Imports

```python
from app.chatbot.agent import generate_title, get_agent, get_last_docs, get_last_narrator
from app.chatbot.models import ChatKind, NarratorProfile, NarratorEntry  # ...
```

### Event-stream changes

1. **Emit `start` without `kind`** — keep current behavior.
2. **After `agent.astream` completes**, before `_complete`:
   - Pop both stashes: `docs = get_last_docs(thread_id)`, `narrator = get_last_narrator(thread_id)`
   - Apply classification rules (Step in "Architecture" above) → `kind: ChatKind`
3. **Emit `kind_hint`** — *if we want the early-hint optimization*. For V1, skip `kind_hint` and only emit `kind` in `_complete`. Adding `kind_hint` later is non-breaking. **Drop `kind_hint` from V1 scope to ship faster.**
4. **Build `_complete.data` per kind:**
   ```python
   data = {"kind": kind, "content": full_content, "citations": [c.model_dump() for c in citations]}
   if kind == "narrator_profile":
       data["narrator"] = narrator["narrator"].model_dump()
   elif kind == "narrator_disambiguation":
       data["narrators"] = [n.model_dump() for n in narrator["narrators"]]
   yield _sse({"type": "assistant_message_complete", "data": data})
   ```

### Narrator-state heuristic (unchanged but simpler)

The existing REFS-presence heuristic for `update_current_narrator` continues to work, but we can simplify it now that we have the stash:

```python
if kind == "hadith_answer":
    await update_current_narrator(db, session.session_id, None)
elif kind == "narrator_profile":
    await update_current_narrator(db, session.session_id, narrator["narrator"].rawi_id)
# disambiguation + chitchat: leave state unchanged
```

This is more reliable than counting `رقم الراوي:` matches with regex.

---

## Step 4 — Tests

### New test file: `tests/test_chat_sse_kinds.py`

Mock the agent's `astream` + the two stashes. Test cases:

| Test | Stashes | LLM output | Expected kind | Expected payload |
|---|---|---|---|---|
| `test_hadith_answer` | `_last_docs=[d1, d2]`, no narrator | `...REFS:[1,2]` | `hadith_answer` | `citations=[c1, c2]` |
| `test_narrator_profile` | narrator single mode | profile prose | `narrator_profile` | `data.narrator.rawi_id=1398` |
| `test_narrator_disambiguation` | narrator disambig mode | numbered list | `narrator_disambiguation` | `data.narrators` len=3, each has `kunya` |
| `test_chitchat_greeting` | both empty | "مرحبا" | `chitchat` | `citations=[]` |
| `test_tool_called_then_ignored` | narrator stash empty (incomplete) | profile prose | `chitchat` | downgrade rule fired |
| `test_both_stashes_populated` | docs + narrator | REFS line | `hadith_answer` | hadith wins |
| `test_narrator_state_set_on_profile` | narrator single mode | — | — | `update_current_narrator` called with `rawi_id` |
| `test_narrator_state_cleared_on_hadith` | docs stash | REFS line | — | `update_current_narrator` called with `None` |
| `test_narrator_state_unchanged_on_chitchat` | both empty | "مرحبا" | — | `update_current_narrator` NOT called |
| `test_classification_field_required` | narrator with `classification=None` | — | — | profile renders, FE handles null |

### Update `tests/test_chatbot_narrator_tool.py`

Tool now stashes — assertions should verify `_last_narrator[thread_id]` is populated correctly:
- Single match → `mode=single`, `narrator.full_name` matches expected
- Disambiguation → `mode=disambiguation`, `len(narrators) == N`, every entry has `kunya` field
- Summary-null fallback → core fields populated from narrator doc, optional fields `None`
- Error path → stash NOT populated

Pass `config={"configurable": {"thread_id": "test-thread"}}` when invoking.

Run:
```bash
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" -m pytest tests/test_chat_sse_kinds.py tests/test_chatbot_narrator_tool.py -v
```

---

## Step 5 — Documentation updates

| File | Update |
|---|---|
| [tasks.md](tasks.md) | Add entry under chatbot section: "Structured SSE contract — kind discriminator + per-kind payloads" |
| [CLAUDE.md](CLAUDE.md) | Update the SSE event sequence block to show `kind` field and the four kinds. Add a row to the chatbot architecture table for `_last_narrator` stash |
| [README.md](README.md) | Document the new `/api/v2/chat` response shape under "API Endpoints" |

---

## Step 6 — Manual verification

```bash
# Hadith question — expect kind=hadith_answer
curl -X POST http://localhost:8001/api/v2/chat \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"question": "ما حديث النية؟"}' | grep -o '"kind":"[^"]*"'

# Narrator single — expect kind=narrator_profile + narrator.rawi_id=1398
curl ... -d '{"question": "من هو الزهري؟"}'

# Narrator ambiguous — expect kind=narrator_disambiguation + narrators[]
curl ... -d '{"question": "ابن عمر"}'

# Greeting — expect kind=chitchat, no narrator/citations
curl ... -d '{"question": "مرحبا"}'

# Direct rawi_id (simulated card click) — expect kind=narrator_profile
curl ... -d '{"question": "أخبرني عن الراوي رقم 1398"}'
```

---

## Critical files

| File | Change |
|---|---|
| [app/chatbot/models.py](app/chatbot/models.py) | + `ChatKind`, `NarratorProfile`, `NarratorEntry`, `NarratorRelation`, `TransmissionStats` |
| [app/chatbot/agent.py](app/chatbot/agent.py) | + `_last_narrator` stash + `get_last_narrator()`. Refactor `_format_*` helpers to return `(payload, text)`. Tool stashes structured payload before returning string |
| [app/chatbot/router.py](app/chatbot/router.py) | Pop both stashes after stream → classify kind → build `data` per kind. Replace REFS-regex narrator state with stash-based |
| [app/chatbot/session.py](app/chatbot/session.py) | No change |
| [tests/test_chatbot_narrator_tool.py](tests/test_chatbot_narrator_tool.py) | Update existing tests to assert stash population |
| `tests/test_chat_sse_kinds.py` | New — covers classification rules + per-kind SSE payload shape |
| [tasks.md](tasks.md), [CLAUDE.md](CLAUDE.md), [README.md](README.md) | Doc updates per Step 5 |

---

## Out of scope (V1)

- `kind_hint` early event — defer to V2 if FE measures the flicker as a real UX problem. Adding it later is non-breaking.
- Backward compatibility shim — old sessions in history don't have `kind` in their stored messages. FE already plans a fallback classifier; BE doesn't need to backfill.
- New REST endpoint mirroring the SSE shape — out of scope. Not requested.
- Multi-tool turn handling beyond "hadith wins" — covered by the priority rules; no special UI.

---

## Open questions for FE before coding starts

1. **`kind_hint` deferral OK?** V1 emits `kind` only in `_complete`. FE's UI shell switches on `_complete`. Ask: is the brief generic-shell flicker during streaming acceptable for V1?
2. **Backfill for stored messages?** Stored sessions in MongoDB don't have `kind` — FE's text-based fallback classifier handles them. Confirm BE doesn't need to migrate old data.

---

## Sizing

~150 LoC across `agent.py`, `router.py`, `models.py`. ~80 LoC of new tests. **~half a day of focused work** including tests and docs.
