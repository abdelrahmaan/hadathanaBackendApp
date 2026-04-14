"""
Unit tests for normalize_for_search() in normalization.py.

TDD — written before the ة→ه and ى→ي mappings are added.
Run with: pytest tests/test_normalization.py -v
"""

from normalization import normalize_for_search

# ---------------------------------------------------------------------------
# Existing behaviour (must not break)
# ---------------------------------------------------------------------------

def test_strips_tashkeel():
    assert normalize_for_search("مُحَمَّد") == "محمد"


def test_normalizes_hamza_above():
    assert normalize_for_search("أبو") == "ابو"


def test_normalizes_hamza_below():
    assert normalize_for_search("إبراهيم") == "ابراهيم"


def test_normalizes_alef_madda():
    assert normalize_for_search("آدم") == "ادم"


def test_normalizes_hamza_on_waw():
    assert normalize_for_search("مؤمن") == "مومن"


def test_normalizes_hamza_on_yeh():
    # ئ (yeh with hamza, U+0626) → ي — results in رييس (double yeh is correct)
    assert normalize_for_search("رئيس") == "رييس"


def test_removes_tatweel():
    assert normalize_for_search("محمـــد") == "محمد"


def test_collapses_whitespace():
    assert normalize_for_search("  سفيان  ") == "سفيان"


def test_empty_string():
    assert normalize_for_search("") == ""


def test_none_input():
    assert normalize_for_search(None) == ""


# ---------------------------------------------------------------------------
# New: taa marbuta normalization  ة → ه
# ---------------------------------------------------------------------------

def test_taa_marbuta_end_of_word():
    # "هريرة" → "هريره"
    assert normalize_for_search("هريرة") == "هريره"


def test_taa_marbuta_in_middle():
    # "رحمة الله" → "رحمه الله"
    assert normalize_for_search("رحمة الله") == "رحمه الله"


def test_taa_marbuta_multiple():
    # "الصحابة الكرام" → "الصحابه الكرام"  (only one ة here)
    assert normalize_for_search("الصحابة الكرام") == "الصحابه الكرام"


# ---------------------------------------------------------------------------
# New: alef maqsura normalization  ى → ي
# ---------------------------------------------------------------------------

def test_alef_maqsura_end_of_word():
    # "موسى" → "موسي"
    assert normalize_for_search("موسى") == "موسي"


def test_alef_maqsura_in_name():
    # "يحيى" → "يحيي"
    assert normalize_for_search("يحيى") == "يحيي"


def test_alef_maqsura_multiple():
    # "على المستوى" → "علي المستوي"
    assert normalize_for_search("على المستوى") == "علي المستوي"


# ---------------------------------------------------------------------------
# Combined / real-world cases
# ---------------------------------------------------------------------------

def test_abu_hurayra_variant_1():
    # Exact form with hamza and taa marbuta
    assert normalize_for_search("أبوهريرة") == "ابوهريره"


def test_abu_hurayra_variant_2():
    # Colloquial spelling — already plain, no hamza, haa instead of taa marbuta
    assert normalize_for_search("ابوهريره") == "ابوهريره"


def test_ibrahim_variant():
    # Both should normalize to same string
    assert normalize_for_search("إبراهيم") == normalize_for_search("ابراهيم")


def test_musa_variant():
    # "موسى" and "موسي" should normalize to the same string
    assert normalize_for_search("موسى") == normalize_for_search("موسي")


def test_combined_tashkeel_and_taa_marbuta():
    # Strip tashkeel AND normalize taa marbuta
    assert normalize_for_search("رَحْمَة") == "رحمه"


def test_combined_hamza_and_alef_maqsura():
    # Normalize hamza + alef maqsura in one pass
    assert normalize_for_search("إيسى") == "ايسي"
