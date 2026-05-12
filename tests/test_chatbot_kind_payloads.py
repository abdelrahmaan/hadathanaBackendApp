"""
Unit tests for the structured narrator payloads consumed by the router.

The router classifies SSE `kind` from two stashes (`_last_docs`, `_last_narrator`)
populated by the tools. These tests cover the pure builder functions that produce
those payloads — `_build_narrator_profile` and `_build_disambiguation` — and the
`get_last_narrator` accessor.
"""
import pytest

from app.chatbot.agent import (
    _build_disambiguation,
    _build_narrator_profile,
    _last_narrator,
    get_last_narrator,
)

# ── Fixture data ───────────────────────────────────────────────────────────────

NARRATOR_FULL = {
    "rawi_id": 1398,
    "full_name_plain": "محمد بن مسلم بن شهاب الزهري",
    "rank_plain": "ثقة حافظ متقن",
}

BIO_FULL = {
    "rawi_id": 1398,
    "summary": {
        "full_name": "محمد بن مسلم الزهري",
        "kunya": "أبو بكر",
        "era": "القرن الثاني الهجري",
        "location": "المدينة المنورة",
        "classification": "ثقة حافظ متقن",
        "hadith_count": 1234,
        "transmission_stats": {"connected": 1100, "suspended": 100, "disputed": 34, "total": 1234},
        "top_teachers": [
            {"name": "سعيد بن المسيب", "freq": 210},
            {"name": "عروة بن الزبير", "freq": 180},
        ],
        "top_students": [{"name": "مالك بن أنس", "freq": 400}],
        "tarajim_sources": ["تهذيب الكمال", "سير أعلام النبلاء"],
        "notes": "ثقة حافظ متقن، إمام في الحديث والفقه.",
    },
}

NARRATOR_NO_SUMMARY = {
    "rawi_id": 999,
    "full_name_plain": "اسم اختبار الكامل",
    "rank_plain": "غير معروف",
}

BIO_NULL_SUMMARY = {
    "rawi_id": 999,
    "summary": None,
    "tarajim": [{"tarjama_plain": "نص الترجمة الكامل" + "ا" * 1000}],
}

NARRATOR_UMAR = {
    "rawi_id": 1091,
    "full_name_plain": "عمر بن الخطاب",
    "rank_plain": "صحابي",
}

BIO_UMAR = {
    "rawi_id": 1091,
    "summary": {
        "full_name": "عمر بن الخطاب",
        "kunya": "أبو حفص",
        "classification": "صحابي جليل",
    },
}


# ── Builder tests ─────────────────────────────────────────────────────────────


def test_build_profile_with_full_summary_populates_all_fields():
    profile, text = _build_narrator_profile(NARRATOR_FULL, BIO_FULL)

    assert profile is not None
    assert profile.rawi_id == 1398
    assert profile.full_name == "محمد بن مسلم الزهري"
    assert profile.profile_url == "https://www.hadathana.app/narrator?nid=1398"
    assert profile.kunya == "أبو بكر"
    assert profile.era == "القرن الثاني الهجري"
    assert profile.location == "المدينة المنورة"
    assert profile.classification == "ثقة حافظ متقن"
    assert profile.hadith_count == 1234
    assert profile.transmission_stats.connected == 1100
    assert profile.transmission_stats.total == 1234
    assert len(profile.top_teachers) == 2
    assert profile.top_teachers[0].name == "سعيد بن المسيب"
    assert profile.top_teachers[0].freq == 210
    assert profile.top_students[0].name == "مالك بن أنس"
    assert profile.scholarly_opinion == "ثقة حافظ متقن، إمام في الحديث والفقه."
    assert profile.sources == "تهذيب الكمال، سير أعلام النبلاء"

    # Arabic text returned alongside structured payload
    assert "محمد بن مسلم الزهري" in text
    assert "أبو بكر" in text


def test_build_profile_with_null_summary_falls_back_to_name_fields():
    profile, text = _build_narrator_profile(NARRATOR_NO_SUMMARY, BIO_NULL_SUMMARY)

    assert profile is not None
    assert profile.rawi_id == 999
    assert profile.full_name == "اسم اختبار الكامل"
    assert profile.profile_url == "https://www.hadathana.app/narrator?nid=999"
    # Optional fields default to None when summary is missing
    assert profile.kunya is None
    assert profile.era is None
    assert profile.transmission_stats is None
    assert profile.top_teachers == []
    # Tarjama text surfaces in scholarly_opinion (truncated to 800 chars)
    assert profile.scholarly_opinion is not None
    assert len(profile.scholarly_opinion) <= 800
    assert "نص الترجمة" in text


def test_build_profile_returns_none_when_required_field_missing():
    # No rawi_id → cannot build profile_url → no profile possible
    narrator_no_id = {"full_name_plain": "اسم"}
    profile, text = _build_narrator_profile(narrator_no_id, None)
    assert profile is None
    assert text  # text fallback still returned

    # Has rawi_id but no full_name anywhere → returns None (router downgrades to chitchat)
    narrator_no_name = {"rawi_id": 42}
    bio_no_name = {"rawi_id": 42, "summary": None, "tarajim": []}
    profile2, _text2 = _build_narrator_profile(narrator_no_name, bio_no_name)
    assert profile2 is None


def test_build_disambiguation_produces_one_entry_per_pair():
    pairs = [(NARRATOR_FULL, BIO_FULL), (NARRATOR_UMAR, BIO_UMAR)]
    entries, text = _build_disambiguation(pairs)

    assert len(entries) == 2

    by_id = {e.rawi_id: e for e in entries}
    assert by_id[1398].full_name == "محمد بن مسلم الزهري"
    assert by_id[1398].kunya == "أبو بكر"
    assert by_id[1398].classification == "ثقة حافظ متقن"
    assert by_id[1091].full_name == "عمر بن الخطاب"
    assert by_id[1091].kunya == "أبو حفص"
    assert by_id[1091].classification == "صحابي جليل"

    # Text contains both rawi_ids and both URLs
    assert "1398" in text and "1091" in text
    assert "https://www.hadathana.app/narrator?nid=1398" in text
    assert "https://www.hadathana.app/narrator?nid=1091" in text


def test_build_disambiguation_skips_entries_without_rawi_id():
    """Entries lacking rawi_id are skipped — they can't be card-clicked anyway."""
    bad_narrator = {"full_name_plain": "بدون رقم"}
    pairs = [(NARRATOR_FULL, BIO_FULL), (bad_narrator, None)]
    entries, _text = _build_disambiguation(pairs)
    assert len(entries) == 1
    assert entries[0].rawi_id == 1398


# ── Stash accessor tests ─────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_stash():
    _last_narrator.clear()
    yield
    _last_narrator.clear()


def test_get_last_narrator_returns_none_when_unset():
    assert get_last_narrator("never-stashed") is None


def test_get_last_narrator_pops_payload():
    profile, _ = _build_narrator_profile(NARRATOR_FULL, BIO_FULL)
    _last_narrator["thread-1"] = {"mode": "single", "narrator": profile}

    payload = get_last_narrator("thread-1")
    assert payload["mode"] == "single"
    assert payload["narrator"].rawi_id == 1398

    # Pop semantics — second call returns None
    assert get_last_narrator("thread-1") is None
