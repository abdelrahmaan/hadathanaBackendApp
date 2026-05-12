# tests/test_golden_dataset.py
from unittest.mock import MagicMock, patch

import pytest


def _make_existing(question: str):
    ex = MagicMock()
    ex.inputs = {"question": question}
    return ex


def test_sync_skips_existing_examples():
    """sync() must not re-upload examples already present in the dataset."""
    from app.chatbot.golden_dataset import DATASET_ID, EXAMPLES, sync

    existing_question = EXAMPLES[0]["input"]["question"]
    existing = [_make_existing(existing_question)]

    mock_client = MagicMock()
    mock_client.list_examples.return_value = iter(existing)

    with patch("app.chatbot.golden_dataset.Client", return_value=mock_client):
        sync()

    call_args = mock_client.create_examples.call_args
    if call_args is not None:
        uploaded_questions = [i["question"] for i in call_args.kwargs.get("inputs", [])]
        assert existing_question not in uploaded_questions


def test_sync_uploads_all_when_dataset_empty():
    """sync() must upload every example when the dataset has no existing entries."""
    from app.chatbot.golden_dataset import DATASET_ID, EXAMPLES, sync

    mock_client = MagicMock()
    mock_client.list_examples.return_value = iter([])

    with patch("app.chatbot.golden_dataset.Client", return_value=mock_client):
        sync()

    mock_client.create_examples.assert_called_once()
    call_args = mock_client.create_examples.call_args
    uploaded_questions = [i["question"] for i in call_args.kwargs.get("inputs", [])]
    expected_questions = [ex["input"]["question"] for ex in EXAMPLES]
    assert uploaded_questions == expected_questions


def test_sync_skips_all_when_all_exist():
    """sync() must call create_examples with empty lists or not call it when all exist."""
    from app.chatbot.golden_dataset import DATASET_ID, EXAMPLES, sync

    existing = [_make_existing(ex["input"]["question"]) for ex in EXAMPLES]

    mock_client = MagicMock()
    mock_client.list_examples.return_value = iter(existing)

    with patch("app.chatbot.golden_dataset.Client", return_value=mock_client):
        sync()

    # Either not called at all, or called with empty inputs
    if mock_client.create_examples.called:
        call_args = mock_client.create_examples.call_args
        assert call_args.kwargs.get("inputs", []) == []


def test_sync_uses_correct_dataset_id():
    """sync() must pass the hardcoded dataset ID to LangSmith."""
    from app.chatbot.golden_dataset import DATASET_ID, sync

    mock_client = MagicMock()
    mock_client.list_examples.return_value = iter([])

    with patch("app.chatbot.golden_dataset.Client", return_value=mock_client):
        sync()

    mock_client.list_examples.assert_called_once_with(dataset_id=DATASET_ID)
