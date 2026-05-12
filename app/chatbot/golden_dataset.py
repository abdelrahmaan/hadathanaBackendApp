# app/chatbot/golden_dataset.py
"""
Curated golden examples for the Hadathana chatbot.

Each entry has:
  input  = {"question": "<Arabic question>"}
  output = {"kind": "<hadith_answer|narrator_profile|narrator_disambiguation|chitchat>"}

Run to sync new examples into the LangSmith dataset:
    python app/chatbot/golden_dataset.py
"""

from langsmith import Client

DATASET_ID = "f24ed392-0dee-4c34-bbed-b05fc3f1aaac"

EXAMPLES: list[dict] = [
    # ── Hadith questions ──────────────────────────────────────────────────────
    {
        "input":  {"question": "ما هو حديث النية؟"},
        "output": {"kind": "hadith_answer"},
    },
    {
        "input":  {"question": "ما هي أحاديث الطهارة في صحيح البخاري؟"},
        "output": {"kind": "hadith_answer"},
    },
    {
        "input":  {"question": "ما هو حديث بدء الوحي؟"},
        "output": {"kind": "hadith_answer"},
    },
    {
        "input":  {"question": "ما هي أحاديث الصيام في البخاري؟"},
        "output": {"kind": "hadith_answer"},
    },
    {
        "input":  {"question": "ما هي أحاديث الزكاة؟"},
        "output": {"kind": "hadith_answer"},
    },
    # ── Narrator profile questions ────────────────────────────────────────────
    {
        "input":  {"question": "من هو أبو هريرة؟"},
        "output": {"kind": "narrator_profile"},
    },
    {
        "input":  {"question": "اعطني معلومات عن الزهري"},
        "output": {"kind": "narrator_profile"},
    },
    {
        "input":  {"question": "من هو الإمام مالك؟"},
        "output": {"kind": "narrator_profile"},
    },
    {
        "input":  {"question": "أخبرني عن عائشة رضي الله عنها"},
        "output": {"kind": "narrator_profile"},
    },
    {
        "input":  {"question": "ما درجة ابن عمر في الجرح والتعديل؟"},
        "output": {"kind": "narrator_profile"},
    },
    # ── Narrator disambiguation questions ────────────────────────────────────
    {
        "input":  {"question": "من هو عمر بن سعيد؟"},
        "output": {"kind": "narrator_disambiguation"},
    },
    {
        "input":  {"question": "أخبرني عن محمد بن عبدالله"},
        "output": {"kind": "narrator_disambiguation"},
    },
    # ── Chitchat / out-of-scope ───────────────────────────────────────────────
    {
        "input":  {"question": "مرحبا"},
        "output": {"kind": "chitchat"},
    },
    {
        "input":  {"question": "شكراً جزيلاً"},
        "output": {"kind": "chitchat"},
    },
    {
        "input":  {"question": "ما هو أفضل فريق كرة قدم؟"},
        "output": {"kind": "chitchat"},
    },
]


def sync() -> None:
    """Push new examples from EXAMPLES into the LangSmith dataset.

    Deduplication key: exact question string. Existing questions are skipped.
    To update an example's expected kind, delete it in LangSmith first then re-run.
    """
    client = Client()

    existing_questions: set[str] = {
        ex.inputs["question"]
        for ex in client.list_examples(dataset_id=DATASET_ID)
    }

    new = [ex for ex in EXAMPLES if ex["input"]["question"] not in existing_questions]
    already_existed = len(EXAMPLES) - len(new)

    if not new:
        print(f"No new examples to sync. ({already_existed} already existed)")
        return

    client.create_examples(
        inputs=[ex["input"] for ex in new],
        outputs=[ex["output"] for ex in new],
        dataset_id=DATASET_ID,
    )
    print(f"Synced {len(new)} new examples ({already_existed} already existed).")


if __name__ == "__main__":
    sync()
