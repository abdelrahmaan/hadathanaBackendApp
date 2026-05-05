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
