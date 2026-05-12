# Golden Dataset Sync — Design Spec

**Date:** 2026-05-12
**File:** `app/chatbot/golden_dataset.py`
**LangSmith dataset ID:** `f24ed392-0dee-4c34-bbed-b05fc3f1aaac`

---

## Goal

Maintain a curated set of golden Q&A examples in `golden_dataset.py` and sync them into the existing LangSmith dataset. The file is the single source of truth for example content; LangSmith is the backing store used during eval runs.

---

## Schema

Each example has two top-level keys:

```python
{
    "input":  {"question": "<Arabic question string>"},
    "output": {"kind": "<hadith_answer | narrator_profile | narrator_disambiguation | chitchat>"},
}
```

`kind` mirrors the `ChatKind` discriminator already defined in `app/chatbot/models.py`. No other fields are required at this stage.

---

## File structure

```
DATASET_ID = "f24ed392-0dee-4c34-bbed-b05fc3f1aaac"

EXAMPLES: list[dict] = [
    {
        "input":  {"question": "..."},
        "output": {"kind": "hadith_answer"},
    },
    ...
]

def sync() -> None: ...

if __name__ == "__main__":
    sync()
```

---

## `sync()` behaviour

1. Instantiate `langsmith.Client()` — reads `LANGSMITH_API_KEY` from env automatically (no explicit key passing).
2. Fetch all existing examples: `client.list_examples(dataset_id=DATASET_ID)`.
3. Build a set of already-existing questions: `{ex.inputs["question"] for ex in existing}`.
4. Filter `EXAMPLES` to only those whose `input["question"]` is not in the set.
5. If no new examples, print `"No new examples to sync."` and return.
6. Call `client.create_examples(inputs=[...], outputs=[...], dataset_id=DATASET_ID)`.
7. Print `"Synced {N} new examples ({M} already existed)."`.

Dedup key is the exact question string. Updating an existing example (e.g. changing its expected `kind`) requires manually deleting it from LangSmith first — intentional, keeps the sync logic simple.

---

## Invocation

```bash
# standalone
PYTHON="/home/abdo_kamar/Projects/.venv/bin/python"
"$PYTHON" app/chatbot/golden_dataset.py

# or as a module
"$PYTHON" -m app.chatbot.golden_dataset
```

Requires `LANGSMITH_API_KEY` in `.env` (already present as `LANGSMITH_API_KEY_DEV` / `LANGSMITH_API_KEY_PROD` — caller sets the right one before running, or the script can read either).

---

## What is NOT in scope

- Running eval experiments (that's a separate script)
- Pulling examples back from LangSmith
- Automatic dedup on update (edit the example in LangSmith UI or delete + re-sync)
- CI integration (can be added later once we have eval scoring)
