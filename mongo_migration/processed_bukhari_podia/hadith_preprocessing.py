"""
Hadith text preprocessing, name normalization, transmission classification,
tashkeel recovery, ground truth validation, and rawi ID resolution.

Extracted from advanced_extractions_llm_pydantic.py to keep preprocessing
logic separate from the LLM extraction pipeline.
"""

import os
import re
import json
import logging
from typing import List, Dict, Any, Tuple, Optional

from pydantic import BaseModel, Field

# Late import to avoid circular dependency — HadithExtraction and ChainItem
# are defined in the main script. We import them lazily inside functions that
# need them, OR the caller passes them in.  To keep things simple we use
# TYPE_CHECKING for annotations and accept the models at runtime via a
# one-time registration call.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from advanced_extractions_llm_pydantic import HadithExtraction, ChainItem


# =========================
# Helpers
# =========================
_SCRAPE_GLYPHS = re.compile(r"[☺♦♣♠☻╡√~•|{}\[\]]")
_FOOTNOTE_MARKERS = re.compile(r"\(\s*\d+\s*\)")
_HADITH_NUM_PREFIX = re.compile(r"^\d+(?:_\d+)*_\s*")
_SALLAM_ABBREV = re.compile(r"\bصلعم\b")
_TATWEEL = re.compile(r"\u0640+")


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def remove_tatweel(s: str) -> str:
    """Remove tatweel/kashida stretching characters (ـ) from Arabic text."""
    return _TATWEEL.sub("", s)


def preprocess_hadith_text(raw: str) -> str:
    """Clean bukhari-pedia scrape noise from hadith text before LLM extraction."""
    text = raw
    text = _HADITH_NUM_PREFIX.sub("", text)
    text = _FOOTNOTE_MARKERS.sub("", text)
    text = _SCRAPE_GLYPHS.sub("", text)
    text = _SALLAM_ABBREV.sub("صلى الله عليه وسلم", text)
    text = remove_tatweel(text)
    text = normalize_whitespace(text)
    return text


def clean_ground_truth_name(name_in_chain: str) -> str:
    """Strip footnote markers and scrape glyphs from bukhari-pedia name_in_chain."""
    name = _FOOTNOTE_MARKERS.sub("", name_in_chain)
    name = _SCRAPE_GLYPHS.sub("", name)
    return normalize_whitespace(name)


def clean_narrator_name(name: str) -> str:
    """Remove honorifics, titles, footnotes, and leaked transmission prefixes."""
    name = normalize_whitespace(name)
    name = _FOOTNOTE_MARKERS.sub("", name)
    name = _SCRAPE_GLYPHS.sub("", name)

    prefixes = [
        r"^حد[َّ]?ثنا\s+",
        r"^أخب[َ]?ر[َ]?نا\s+",
        r"^حد[َّ]?ثني\s+",
        r"^أخب[َ]?ر[َ]?ني\s+",
        r"^سمعت[ُ]?\s+",
        r"^عن\s+",
        r"^قال[َ]?\s+",
        r"^أنبأنا\s+",
    ]
    for pattern in prefixes:
        name = re.sub(pattern, "", name)

    honorifics = [
        r"\s+رضي الله عنه.*",
        r"\s+رضي الله عنها.*",
        r"\s+رضي الله عنهم.*",
        r"\s+رضي الله عنهما.*",
        r"\s+صلى الله عليه وسلم.*",
        r"\s+عليه السلام.*",
        r"\s+رحمه الله.*",
        r"\s+رحمها الله.*",
        r"\s+أم المؤمنين.*",
        r"\s+الصحابي.*",
        r"\s+التابعي.*",
    ]

    for pattern in honorifics:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE)

    return normalize_whitespace(name)


# =========================
# Normalized form for matching only (NOT for output)
# =========================
# Equivalence groups for i'rab variants of common prefixes
_IRAB_NORMALIZATIONS = [
    # أبو / أبي / أبا → أبو
    (re.compile(r"\bأبي\b"), "أبو"),
    (re.compile(r"\bأبا\b"), "أبو"),
    # ابن variants with kasra
    (re.compile(r"\bابن[ِ]?\b"), "ابن"),
    # أم / أمّ
    (re.compile(r"\bأم[ِّ]?\b"), "أم"),
]


def normalize_for_matching(name: str) -> str:
    """Normalize a narrator name for comparison/matching purposes ONLY.

    Applies i'rab normalization (أبو/أبي/أبا → أبو), strips tashkeel,
    and cleans honorifics. The result is NEVER used in the output —
    only for rawi_id resolution and ground truth validation.
    """
    name = clean_narrator_name(name)
    name = _strip_tashkeel(name)
    for pattern, replacement in _IRAB_NORMALIZATIONS:
        name = pattern.sub(replacement, name)
    return normalize_whitespace(name)


def is_arabic_heavy(text: str) -> bool:
    return sum(1 for ch in text if "\u0600" <= ch <= "\u06FF") > 20


def renumber_chain_ids(chains: list) -> list:
    """Ensure chain_id contiguous chain_1..chain_n."""
    new = []
    for i, ch in enumerate(chains, 1):
        ch.chain_id = f"chain_{i}"
        new.append(ch)
    return new


_VALID_TRANSMISSIONS = {
    "حدثنا", "حدثني", "أخبرنا", "أخبرني", "سمعت",
    "عن", "قال", "أنبأنا", "أنبأني", "ناولني",
    "كتب إلي", "قرأت عليه", "أن", "أنه سمع", "أنها",
}


def basic_validate_extraction(
    _hadith_text: str, extraction
) -> Tuple[bool, str]:
    """
    Lightweight sanity checks on the LLM extraction output.
    """
    prophet_markers = [
        "رسول الله",
        "النبي",
        "محمد صلى الله عليه وسلم",
        "رسولِ الله",
    ]
    for ch in extraction.chains:
        if not re.match(r"^chain_\d+$", ch.chain_id):
            return False, f"chain_id غير صحيح: {ch.chain_id}"
        if ch.type not in ("primary", "follow_up", "nested"):
            return False, f"{ch.chain_id} نوع غير صحيح: {ch.type}"
        if not ch.narrators:
            return False, f"{ch.chain_id} بلا رواة"
        for n in ch.narrators:
            name = normalize_whitespace(n.name)
            if not name:
                return False, f"{ch.chain_id} فيه اسم فاضي"
            if any(pm in name for pm in prophet_markers):
                return (
                    False,
                    f"{ch.chain_id} يحتوي النبي ضمن الرواة (غير مسموح)",
                )
            if not n.role:
                return False, f"{ch.chain_id} راوٍ بدون role"
        if ch.narrators[-1].role != "lead":
            return False, f"{ch.chain_id} آخر راوٍ لازم يكون lead"
    return True, "ok"


# =========================
# Transmission regex fallback
# =========================
_KNOWN_TRANSMISSIONS = [
    "حدثنا", "حدثني", "أخبرنا", "أخبرني", "سمعت",
    "عن", "قال", "أنبأنا", "أنبأني", "ناولني",
    "كتب إلي", "قرأت عليه", "أن", "أنه سمع",
    "حدَّثنا", "حدَّثني", "أخبَرَنا", "أخبَرَني",
]

_TRANSMISSION_RE = re.compile(
    r"(حد[َّ]?ثنا|حد[َّ]?ثني|أخب[َ]?ر[َ]?نا|أخب[َ]?ر[َ]?ني|سمعت|أنبأنا|أنبأني"
    r"|ناولني|كتب إلي|قرأت عليه|أنه سمع|أن(?:ه|ها)?|عن)"
)

# "قال" in isnad context: only when preceded by "و" (وقال) or at line start,
# and NOT followed by prophet/messenger markers that indicate matn.
_QAAL_ISNAD_RE = re.compile(
    r"(?:^|و)\s*قال[َ]?\s+"
    r"(?!رسول|النبي|صلى|عليه|لها|لهم|له|لنا|لي|فيه|إن[َّ]?)"
)

_MATN_QAAL_MARKERS = {"رسول", "النبي", "صلى", "عليه", "فقال", "قالت"}

_TRANSMISSION_CANONICAL = {
    "حدَّثنا": "حدثنا", "حدَّثني": "حدثني",
    "أخبَرَنا": "أخبرنا", "أخبَرَني": "أخبرني",
    "أخبرنا": "أخبرنا", "أخبرني": "أخبرني",
    "أنَّه": "أن", "أنَّها": "أن", "أنه": "أن", "أنها": "أن",
    "أنَّهُ سَمِعَ": "أنه سمع", "أنَّه سمع": "أنه سمع",
    "سمعتُ": "سمعت",
    "قالَ": "قال",
    "أنَّ": "أن",
}


# =========================
# Tashkeel
# =========================
_TASHKEEL_RE = re.compile(r"[\u064B-\u065F\u0670\u0640]")


def _strip_tashkeel(s: str) -> str:
    """Remove Arabic diacritics and tatweel for fuzzy name comparison."""
    return _TASHKEEL_RE.sub("", s)


def _build_tashkeel_index(text: str) -> Tuple[str, List[int]]:
    """Build a mapping from stripped-char-index to original-char-index.
    Returns (stripped_text, index_map) where index_map[i] is the
    position in the original text of the i-th non-tashkeel character."""
    stripped_chars = []
    index_map = []
    for i, ch in enumerate(text):
        if not _TASHKEEL_RE.match(ch):
            stripped_chars.append(ch)
            index_map.append(i)
    return "".join(stripped_chars), index_map


def recover_tashkeel(plain: str, full_text: str) -> str:
    """Find a plain (tashkeel-stripped) string in full_text and return
    the corresponding tashkeel'd substring from full_text."""
    if not plain or not full_text:
        return plain or ""
    stripped_text, index_map = _build_tashkeel_index(full_text)
    plain_stripped = _strip_tashkeel(plain)
    pos = stripped_text.find(plain_stripped)
    if pos < 0:
        return plain
    start_orig = index_map[pos]
    end_idx = pos + len(plain_stripped) - 1
    if end_idx >= len(index_map):
        end_orig = len(full_text)
    else:
        end_orig = index_map[end_idx] + 1
        while end_orig < len(full_text) and _TASHKEEL_RE.match(full_text[end_orig]):
            end_orig += 1
    return full_text[start_orig:end_orig]


def canonicalize_transmission(t: Optional[str]) -> Optional[str]:
    """Canonicalize transmission for type classification lookup (strips tashkeel variants)."""
    if not t:
        return None
    t = normalize_whitespace(t)
    t = _TASHKEEL_RE.sub("", t)
    return _TRANSMISSION_CANONICAL.get(t, t)


# =========================
# Transmission type classification (deterministic, post-LLM)
# =========================
TRANSMISSION_TYPE_MAP: Dict[str, Tuple[str, bool]] = {
    # (transmission_type, is_explicit_hearing)
    # السماع
    "حدثنا":     ("samaa", True),
    "حدثني":     ("samaa", True),
    "سمعت":      ("samaa", True),
    "أنه سمع":   ("samaa", True),
    # سماع أو عرض (ambiguous between the two)
    "أخبرنا":    ("samaa_or_ard", True),
    "أخبرني":    ("samaa_or_ard", True),
    # العرض (القراءة على الشيخ)
    "قرأت عليه": ("ard", True),
    # العنعنة
    "عن":        ("anana", False),
    "أن":        ("anana", False),
    # غير صريح / محتمل
    "قال":       ("ambiguous", False),
    # الإجازة / المناولة
    "أنبأنا":    ("ijaza_or_munawala", False),
    "أنبأني":    ("ijaza_or_munawala", False),
    "ناولني":    ("munawala", False),
    # المكاتبة
    "كتب إلي":  ("mukataba", False),
}


def classify_transmission(
    transmission: Optional[str],
) -> Tuple[Optional[str], Optional[bool]]:
    """
    Given a canonical transmission literal, return (type, is_explicit_hearing).
    Pure dict lookup -- no LLM, no hallucination.
    """
    if not transmission:
        return None, None
    entry = TRANSMISSION_TYPE_MAP.get(transmission)
    if entry:
        return entry
    return "unknown", False


def extract_transmissions_regex(hadith_text: str) -> List[Tuple[str, str]]:
    """
    Regex scan: extract (transmission_formula, following_text_snippet) pairs.
    Returns list of (formula, next_50_chars) for matching against narrator names.

    "قال" is handled separately to avoid false positives from matn occurrences
    like "قال رسول الله" or "قالت عائشة" inside the hadith body.
    """
    pairs = []
    for m in _TRANSMISSION_RE.finditer(hadith_text):
        formula = m.group(1)
        rest = hadith_text[m.end():m.end() + 80].strip()
        pairs.append((canonicalize_transmission(formula), rest))

    for m in _QAAL_ISNAD_RE.finditer(hadith_text):
        rest = hadith_text[m.end():m.end() + 80].strip()
        first_word = rest.split()[0] if rest.split() else ""
        if first_word not in _MATN_QAAL_MARKERS:
            pairs.append(("قال", rest))

    return pairs


def apply_transmission_fallback(
    extraction, hadith_text: str
):
    """
    If LLM returned null for a narrator's transmission, try to fill it from regex.
    """
    regex_pairs = extract_transmissions_regex(hadith_text)
    if not regex_pairs:
        return extraction

    for chain in extraction.chains:
        for narrator in chain.narrators:
            if narrator.transmission is not None:
                narrator.transmission = canonicalize_transmission(narrator.transmission)
                continue
            clean_name = clean_narrator_name(narrator.name)
            for formula, snippet in regex_pairs:
                if clean_name and clean_name[:10] in snippet[:50]:
                    narrator.transmission = formula
                    break
    return extraction


# =========================
# Ground truth validation
# =========================


def validate_against_ground_truth(
    extraction,
    ground_truth_narrators: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Compare LLM-extracted narrators against bukhari-pedia ground truth.
    Returns a match report dict.
    """
    gt_names_raw = [
        clean_ground_truth_name(n.get("name_in_chain", ""))
        for n in ground_truth_narrators
    ]
    gt_names_norm = [normalize_for_matching(n) for n in gt_names_raw]

    extracted_names = set()
    for ch in extraction.chains:
        for n in ch.narrators:
            extracted_names.add(clean_narrator_name(n.name))

    extracted_norm = {normalize_for_matching(n) for n in extracted_names}

    matched_gt = []
    missing_gt = []
    for raw, norm in zip(gt_names_raw, gt_names_norm):
        found = False
        for ext in extracted_norm:
            if norm in ext or ext in norm:
                found = True
                break
        if found:
            matched_gt.append(raw)
        else:
            missing_gt.append(raw)

    extra_extracted = []
    for ext_name in extracted_names:
        ext_n = normalize_for_matching(ext_name)
        found = False
        for gt_n in gt_names_norm:
            if ext_n in gt_n or gt_n in ext_n:
                found = True
                break
        if not found:
            extra_extracted.append(ext_name)

    gt_total = len(gt_names_raw)
    coverage = len(matched_gt) / gt_total if gt_total > 0 else 1.0

    return {
        "gt_total": gt_total,
        "matched": len(matched_gt),
        "missing_from_extraction": missing_gt,
        "extra_in_extraction": extra_extracted,
        "coverage": round(coverage, 3),
    }


# =========================
# Rawi ID resolution
# =========================
RAWI_LOOKUP_PATH = os.path.join(os.path.dirname(__file__), "rawi_lookup.json")


def load_rawi_lookup(path: str = RAWI_LOOKUP_PATH) -> Dict[int, List[str]]:
    """Load pre-built rawi_id → name variations from rawi_lookup.json.
    Returns dict mapping rawi_id (int) → list of stripped name variations."""
    if not os.path.exists(path):
        logging.warning("rawi_lookup.json not found at %s — run build_rawi_lookup.py first", path)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return {int(k): v.get("names_stripped", []) for k, v in raw.items()}


def resolve_rawi_id(
    extracted_name: str,
    gt_narrators: List[Dict[str, Any]],
    rawi_lookup: Optional[Dict[int, List[str]]] = None,
) -> Optional[int]:
    """
    Resolve an LLM-extracted narrator name to a rawi_id.
    1) Per-hadith: match against this hadith's ground truth narrators.
    2) Global fallback: match against the pre-built rawi_lookup.json.
    """
    name_norm = normalize_for_matching(extracted_name)

    for gt in gt_narrators:
        rid = gt.get("rawi_id")
        gt_norm = normalize_for_matching(gt.get("name_in_chain", ""))
        if not gt_norm or not rid:
            continue
        if name_norm in gt_norm or gt_norm in name_norm:
            return rid
        gt_full = normalize_for_matching(gt.get("full_name", ""))
        if gt_full and (name_norm in gt_full or gt_full in name_norm):
            return rid

    if rawi_lookup:
        matched_rids = set()
        for rid, names_stripped in rawi_lookup.items():
            for var in names_stripped:
                var_norm = normalize_for_matching(var) if var else var
                if name_norm in var_norm or var_norm in name_norm:
                    matched_rids.add(rid)
                    break
        if len(matched_rids) == 1:
            return matched_rids.pop()

    return None
