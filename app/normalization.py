"""
Arabic text normalization utilities for hadith narrator names.

This module provides functions to normalize Arabic text for consistent
matching and deduplication of narrator names in the knowledge graph.
"""

import re
from typing import Optional


def normalize_ar(name: Optional[str]) -> str:
    """
    Normalize Arabic narrator names for consistent matching.

    Normalization steps:
    1. Strip leading/trailing whitespace
    2. Collapse multiple spaces to single space
    3. Remove tatweel (kashida) character
    4. Unify hamza variants (أ/إ/آ → ا, ؤ → و, ئ → ي)

    Args:
        name: Arabic name string to normalize

    Returns:
        Normalized name string, or empty string if input is None/empty

    Examples:
        >>> normalize_ar("أبو  بكر")
        'ابو بكر'
        >>> normalize_ar("محمـــد بن إبراهيم")
        'محمد بن ابراهيم'
        >>> normalize_ar("  سفيان  ")
        'سفيان'
    """
    if not name:
        return ""

    # Strip leading/trailing whitespace
    result = name.strip()

    # Collapse multiple whitespace characters to single space
    result = re.sub(r'\s+', ' ', result)

    # Remove tatweel (kashida) - Unicode U+0640
    result = result.replace('\u0640', '')

    # Unify hamza variants on alef
    # أ (alef with hamza above) → ا
    result = result.replace('\u0623', '\u0627')
    # إ (alef with hamza below) → ا
    result = result.replace('\u0625', '\u0627')
    # آ (alef with madda above) → ا
    result = result.replace('\u0622', '\u0627')

    # Unify hamza on waw and yeh
    # ؤ (waw with hamza) → و
    result = result.replace('\u0624', '\u0648')
    # ئ (yeh with hamza) → ي
    result = result.replace('\u0626', '\u064A')

    return result


def normalize_for_search(name: Optional[str]) -> str:
    """
    Normalize Arabic text for search/matching purposes.

    This is a more aggressive normalization that also removes
    diacritics (tashkeel) for fuzzy matching.

    Args:
        name: Arabic name string to normalize

    Returns:
        Normalized name string suitable for search

    Examples:
        >>> normalize_for_search("مُحَمَّد")
        'محمد'
    """
    if not name:
        return ""

    # First apply standard normalization
    result = normalize_ar(name)

    # Remove Arabic diacritics (tashkeel)
    # Fatha, Damma, Kasra, Sukun, Shadda, etc.
    diacritics = [
        '\u064B',  # Fathatan
        '\u064C',  # Dammatan
        '\u064D',  # Kasratan
        '\u064E',  # Fatha
        '\u064F',  # Damma
        '\u0650',  # Kasra
        '\u0651',  # Shadda
        '\u0652',  # Sukun
        '\u0653',  # Maddah above
        '\u0654',  # Hamza above
        '\u0655',  # Hamza below
    ]
    for diacritic in diacritics:
        result = result.replace(diacritic, '')

    # Normalize taa marbuta and alef maqsura
    # ة (taa marbuta U+0629) → ه (haa U+0647)
    result = result.replace('\u0629', '\u0647')
    # ى (alef maqsura U+0649) → ي (yaa U+064A)
    result = result.replace('\u0649', '\u064A')

    return result


if __name__ == "__main__":
    # Test cases
    test_cases = [
        ("أبو بكر الصديق", "ابو بكر الصديق"),
        ("إبراهيم بن أدهم", "ابراهيم بن ادهم"),
        ("محمـــد بن عبد الله", "محمد بن عبد الله"),
        ("  سفيان   بن   عيينة  ", "سفيان بن عيينة"),
        ("عمر بن الخطّاب", "عمر بن الخطاب"),
        ("آدم", "ادم"),
        ("مؤمن", "مومن"),
        ("رئيس", "ريس"),
        (None, ""),
        ("", ""),
    ]

    print("Testing normalize_ar():")
    print("-" * 50)
    all_passed = True
    for input_name, expected in test_cases:
        result = normalize_ar(input_name)
        status = "✓" if result == expected else "✗"
        if result != expected:
            all_passed = False
        print(f"{status} Input: '{input_name}' -> '{result}' (expected: '{expected}')")

    print("-" * 50)
    print(f"All tests passed: {all_passed}")
