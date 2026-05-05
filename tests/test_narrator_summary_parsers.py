"""Tests for the pure-Python structured-field parsers in generate_narrator_summaries."""
import pytest
from scripts.generate_narrator_summaries import (
    parse_hadith_count,
    parse_transmission_stats,
    build_top_relations,
    TransmissionStats,
    NarratorRelation,
)

NARRATOR_INFO_520 = [
    {"action": "get_matn_entries",       "text": "عدد أحاديثه المرقَّمة في الصحيح: 214"},
    {"action": "get_esnad_entries",      "text": "عدد رواياته الكلي: 281"},
    {"action": "get_esnad_mt_entries",   "text": "الروايات الموصولة: 226"},
    {"action": "get_esnad_kht_entries",  "text": "الروايات المعلقة: 49"},
    {"action": "get_esnad_mt_kht_entries","text": "الروايات المُختَلَف في وصْلِها وتعليقها: 0"},
    {"action": "get_esnad_shk_entries",  "text": "الروايات المُختَلَف فيها بين رواة الصحيح أو شرَّاحه: 3"},
    {"action": "get_esnad_mqron_entries","text": "الأسانيد التي قْرٍن فيها بغيره: 68"},
]

TEACHERS_520 = [
    {"rawi_id": 1794, "name": "أَبا هُرَيْرَةَ", "freq": 133},
    {"rawi_id": 1818, "name": "عايِشَةَ",         "freq": 14},
    {"rawi_id": 487,  "name": "سَعْدًا",           "freq": 7},
    {"rawi_id": 484,  "name": "أَبِي سَعِيدٍ",    "freq": 5},
    {"rawi_id": 325,  "name": "حَكِيمِ بْنِ حِزامٍ", "freq": 4},
    {"rawi_id": 212,  "name": "جُبَيْرَ بْنَ مُطْعِمٍ", "freq": 3},
]


def test_parse_hadith_count():
    assert parse_hadith_count(NARRATOR_INFO_520) == 214


def test_parse_hadith_count_missing():
    assert parse_hadith_count([]) == 0


def test_parse_transmission_stats():
    stats = parse_transmission_stats(NARRATOR_INFO_520)
    assert stats == TransmissionStats(total=281, connected=226, suspended=49, disputed=3)


def test_parse_transmission_stats_missing():
    stats = parse_transmission_stats([])
    assert stats == TransmissionStats(total=0, connected=0, suspended=0, disputed=0)


def test_build_top_relations_top5():
    top = build_top_relations(TEACHERS_520, n=5)
    assert len(top) == 5
    assert top[0] == NarratorRelation(rawi_id=1794, name="أَبا هُرَيْرَةَ", freq=133)


def test_build_top_relations_fewer_than_n():
    top = build_top_relations(TEACHERS_520[:2], n=5)
    assert len(top) == 2


def test_build_top_relations_empty():
    assert build_top_relations([], n=5) == []


def test_extract_number_arabic_indic():
    from scripts.generate_narrator_summaries import _extract_number
    assert _extract_number("عدد أحاديثه: ٢١٤") == 214


def test_build_top_relations_missing_keys():
    malformed = [
        {"rawi_id": 1, "name": "أحمد", "freq": 10},
        {"freq": 5},  # missing rawi_id and name — should be skipped
        {"rawi_id": 2, "name": "محمد", "freq": 3},
    ]
    top = build_top_relations(malformed, n=5)
    assert len(top) == 2
    assert top[0].rawi_id == 1


# ---------------------------------------------------------------------------
# Tests for assemble_summary
# ---------------------------------------------------------------------------

from scripts.generate_narrator_summaries import (
    assemble_summary,
    LLMExtracted,
    NarratorSummary,
)

BIO_DOC_520 = {
    "rawi_id": 520,
    "full_name": "سَعِيد بن المُسَيَّب",
    "rank": "أحدُ العلماء الأثبات الفقهاء الكبار",
    "narrator_info": NARRATOR_INFO_520,
    "tarajim": [
        {"source": "الجرح والتعديل", "tarjama": "...", "tarjama_plain": "..."},
    ],
}

STATS_DOC_520 = {
    "rawi_id": 520,
    "hadith_count": 199,
    "teachers": TEACHERS_520,
    "students": [
        {"rawi_id": 1398, "name": "الزُّهْرِيِّ", "freq": 164},
        {"rawi_id": 46,   "name": "الزُّهْرِيِّ", "freq": 6},
    ],
}

LLM_RESULT_520 = LLMExtracted(
    kunya="أبو محمد",
    era="القرن 1 هـ",
    location="المدينة المنورة",
    notes="وثّقه الأئمة وعدّوه من أفقه التابعين.",
)


def test_assemble_summary_full():
    summary = assemble_summary(BIO_DOC_520, STATS_DOC_520, LLM_RESULT_520)
    assert isinstance(summary, NarratorSummary)
    assert summary.full_name == "سَعِيد بن المُسَيَّب"
    assert summary.kunya == "أبو محمد"
    assert summary.era == "القرن 1 هـ"
    assert summary.location == "المدينة المنورة"
    assert summary.classification == "أحدُ العلماء الأثبات الفقهاء الكبار"
    assert summary.hadith_count == 214          # from narrator_info, not stats
    assert summary.transmission_stats.total == 281
    assert summary.transmission_stats.connected == 226
    assert summary.transmission_stats.suspended == 49
    assert summary.transmission_stats.disputed == 3
    assert len(summary.top_teachers) == 5
    assert summary.top_teachers[0].rawi_id == 1794
    assert len(summary.top_students) == 2       # only 2 students available
    assert summary.tarajim_sources == ["الجرح والتعديل"]
    assert summary.notes == "وثّقه الأئمة وعدّوه من أفقه التابعين."


def test_assemble_summary_no_stats():
    summary = assemble_summary(BIO_DOC_520, None, LLM_RESULT_520)
    assert summary.top_teachers == []
    assert summary.top_students == []
    assert summary.hadith_count == 214  # falls back to narrator_info


def test_assemble_summary_llm_none():
    summary = assemble_summary(BIO_DOC_520, STATS_DOC_520, None)
    assert summary is None
