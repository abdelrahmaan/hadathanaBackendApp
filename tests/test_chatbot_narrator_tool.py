"""
Tests for the search_narrators chatbot tool.

The tool queries MongoDB directly (no Qdrant). Two parallel queries are merged
and deduped by rawi_id:
  A. processed_podia_narrators — regex on full_name_plain + name_in_chain_plain
  B. processed_podia_narrator_biographies — regex on summary.kunya

All MongoDB access is mocked.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Pass a thread_id so the tool stashes its structured payload — the router
# pops this stash to classify kind and build the SSE response.
_RUNNABLE_CONFIG = {"configurable": {"thread_id": "test-thread"}}


@pytest.fixture(autouse=True)
def _clear_stash():
    """Reset the per-thread narrator stash between tests."""
    from app.chatbot.agent import _last_narrator
    _last_narrator.clear()
    yield
    _last_narrator.clear()

# ── Fixture data ───────────────────────────────────────────────────────────────

ZUHRI_NARRATOR = {
    "rawi_id": 1398,
    "name_in_chain": "ابن شهاب",
    "name_in_chain_plain": "ابن شهاب",
    "full_name": "محمد بن مسلم بن عبيد الله بن عبد الله بن شهاب القرشي الزهري، أبو بكر",
    "full_name_plain": "محمد بن مسلم بن عبيد الله بن عبد الله بن شهاب القرشي الزهري، أبو بكر",
    "rank": "ثقة",
    "rank_plain": "ثقة",
}

ZUHRI_BIOGRAPHY = {
    "rawi_id": 1398,
    "summary": {
        "full_name": "محمد بن مسلم الزهري",
        "kunya": "أبو بكر",
        "era": "القرن الثاني الهجري",
        "location": "المدينة المنورة",
        "classification": "ثقة حافظ متقن",
        "hadith_count": 1234,
        "transmission_stats": {"total": 1234, "connected": 1100, "suspended": 100, "disputed": 34},
        "top_teachers": [
            {"rawi_id": 5, "name": "سعيد بن المسيب", "freq": 210},
            {"rawi_id": 7, "name": "عروة بن الزبير", "freq": 180},
        ],
        "top_students": [
            {"rawi_id": 9, "name": "مالك بن أنس", "freq": 400},
        ],
        "tarajim_sources": ["تهذيب الكمال", "سير أعلام النبلاء"],
        "notes": "ثقة حافظ متقن، إمام في الحديث والفقه.",
    },
    "tarajim": [{"source": "تهذيب الكمال", "tarjama_plain": "محمد بن مسلم..."}],
}

# Narrator with NO summary — fallback path
ORPHAN_NARRATOR = {
    "rawi_id": 999,
    "name_in_chain": "اسم اختبار",
    "name_in_chain_plain": "اسم اختبار",
    "full_name": "اسم اختبار الكامل",
    "full_name_plain": "اسم اختبار الكامل",
    "rank": "غير معروف",
    "rank_plain": "غير معروف",
}

ORPHAN_BIOGRAPHY = {
    "rawi_id": 999,
    "summary": None,  # explicit null — triggers fallback
    "tarajim": [{"source": "مصدر", "tarjama_plain": "نص الترجمة الكامل" + "ا" * 1000}],
}

# Umar — kunya "أبو حفص" not embedded in full_name_plain
UMAR_NARRATOR = {
    "rawi_id": 1091,
    "name_in_chain": "عمر بن الخطاب",
    "name_in_chain_plain": "عمر بن الخطاب",
    "full_name": "عمر بن الخطاب بن نفيل بن عبد العزى القرشي العدوي، أمير المؤمنين",
    "full_name_plain": "عمر بن الخطاب بن نفيل بن عبد العزى القرشي العدوي، أمير المؤمنين",
    "rank": "صحابي",
    "rank_plain": "صحابي",
}

UMAR_BIOGRAPHY = {
    "rawi_id": 1091,
    "summary": {
        "full_name": "عمر بن الخطاب",
        "kunya": "أبو حفص",
        "era": "صدر الإسلام",
        "location": "المدينة المنورة",
        "classification": "صحابي جليل",
        "hadith_count": 537,
        "transmission_stats": {"total": 537, "connected": 537, "suspended": 0, "disputed": 0},
        "top_teachers": [],
        "top_students": [{"rawi_id": 100, "name": "عبدالله بن عمر", "freq": 200}],
        "tarajim_sources": ["تهذيب الكمال"],
        "notes": "أمير المؤمنين الفاروق.",
    },
    "tarajim": [],
}


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_cursor(items: list):
    """Build a Motor-style async cursor that returns `items` from to_list()."""
    cursor = MagicMock()
    cursor.to_list = AsyncMock(return_value=items)
    return cursor


def _make_collections(narrators_results=None, kunya_results=None, bio_lookup=None):
    """
    Build mock narrators + biographies collections.
      - narrators_results: list of docs returned by find() on names collection (Query A)
      - kunya_results: list of {rawi_id} docs returned by find() on bio collection (Query B)
      - bio_lookup: dict[rawi_id] → biography doc (returned by find_one)
    """
    narrators_results = narrators_results or []
    kunya_results = kunya_results or []
    bio_lookup = bio_lookup or {}

    narrators = MagicMock()
    narrators.find = MagicMock(return_value=_make_cursor(narrators_results))

    async def narrators_find_one(query, *args, **kwargs):
        if "rawi_id" in query:
            for doc in narrators_results + list(bio_lookup.values()):
                # match against narrators-shape docs (have full_name_plain)
                pass
            for doc in [
                d for d in [ZUHRI_NARRATOR, ORPHAN_NARRATOR, UMAR_NARRATOR]
                if d["rawi_id"] == query["rawi_id"]
            ]:
                return doc
        return None

    narrators.find_one = AsyncMock(side_effect=narrators_find_one)

    biographies = MagicMock()
    biographies.find = MagicMock(return_value=_make_cursor(kunya_results))

    async def bio_find_one(query, projection=None):
        rawi_id = query.get("rawi_id")
        return bio_lookup.get(rawi_id)

    biographies.find_one = AsyncMock(side_effect=bio_find_one)

    return narrators, biographies


# ── Tests ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_search_narrators_single_match_with_summary():
    """Single name match returns full profile from summary fields."""
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections(
        narrators_results=[ZUHRI_NARRATOR],
        bio_lookup={1398: ZUHRI_BIOGRAPHY},
    )

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({"query": "الزهري"}, config=_RUNNABLE_CONFIG)

    # Pulls fields from summary
    assert "محمد بن مسلم الزهري" in result
    assert "1398" in result
    assert "أبو بكر" in result  # kunya
    assert "ثقة حافظ متقن" in result  # classification
    assert "١٢٣٤" in result  # hadith_count rendered with Arabic-Indic digits
    assert "ثقة حافظ متقن، إمام في الحديث والفقه" in result  # notes
    assert "https://www.hadathana.app/narrator?nid=1398" in result

    # Stash populated with structured single-mode payload
    payload = _last_narrator["test-thread"]
    assert payload["mode"] == "single"
    assert payload["narrator"].rawi_id == 1398
    assert payload["narrator"].full_name == "محمد بن مسلم الزهري"
    assert payload["narrator"].kunya == "أبو بكر"
    assert payload["narrator"].classification == "ثقة حافظ متقن"
    assert payload["narrator"].hadith_count == 1234
    assert payload["narrator"].transmission_stats.total == 1234
    assert payload["narrator"].top_teachers[0].name == "سعيد بن المسيب"
    assert payload["narrator"].top_teachers[0].freq == 210
    assert payload["narrator"].profile_url == "https://www.hadathana.app/narrator?nid=1398"


@pytest.mark.asyncio
async def test_search_narrators_summary_null_fallback():
    """When summary is null, falls back to name + truncated tarajim text."""
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections(
        narrators_results=[ORPHAN_NARRATOR],
        bio_lookup={999: ORPHAN_BIOGRAPHY},
    )

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({"query": "اختبار"}, config=_RUNNABLE_CONFIG)

    assert "اسم اختبار الكامل" in result
    assert "999" in result
    # fallback truncates to 800 chars
    assert "نص الترجمة" in result

    # Fallback path still produces a single-mode payload (no summary, but core fields present)
    payload = _last_narrator["test-thread"]
    assert payload["mode"] == "single"
    assert payload["narrator"].rawi_id == 999
    assert payload["narrator"].full_name == "اسم اختبار الكامل"
    # Optional fields default to None when summary is missing
    assert payload["narrator"].kunya is None
    assert payload["narrator"].transmission_stats is None
    assert payload["narrator"].top_teachers == []


@pytest.mark.asyncio
async def test_search_narrators_kunya_only_match():
    """Kunya search ('أبو حفص') matches Umar via Query B even though kunya is NOT in full_name_plain."""
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections(
        narrators_results=[],                          # Query A returns nothing
        kunya_results=[{"rawi_id": 1091}],            # Query B finds via summary.kunya
        bio_lookup={1091: UMAR_BIOGRAPHY},
    )

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({"query": "أبو حفص"}, config=_RUNNABLE_CONFIG)

    assert "عمر بن الخطاب" in result
    assert "1091" in result
    assert "أبو حفص" in result

    payload = _last_narrator["test-thread"]
    assert payload["mode"] == "single"
    assert payload["narrator"].rawi_id == 1091
    assert payload["narrator"].kunya == "أبو حفص"


@pytest.mark.asyncio
async def test_search_narrators_dedupe_overlapping_hits():
    """When the same rawi_id appears in both Query A and Query B, it appears once."""
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections(
        narrators_results=[ZUHRI_NARRATOR],
        kunya_results=[{"rawi_id": 1398}],   # same rawi_id from both queries
        bio_lookup={1398: ZUHRI_BIOGRAPHY},
    )

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({"query": "الزهري"}, config=_RUNNABLE_CONFIG)

    # Single match path — full profile, not disambiguation
    assert "ثقة حافظ متقن، إمام في الحديث والفقه" in result
    # rawi_id mentioned exactly once in a "رقم الراوي" line
    assert result.count("1398") >= 1

    # Dedupe collapses to a single payload, not disambiguation
    payload = _last_narrator["test-thread"]
    assert payload["mode"] == "single"
    assert payload["narrator"].rawi_id == 1398


@pytest.mark.asyncio
async def test_search_narrators_multiple_matches_disambiguation():
    """2+ matches return a disambiguation list, not full biographies."""
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections(
        narrators_results=[ZUHRI_NARRATOR, UMAR_NARRATOR],
        bio_lookup={1398: ZUHRI_BIOGRAPHY, 1091: UMAR_BIOGRAPHY},
    )

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({"query": "محمد"}, config=_RUNNABLE_CONFIG)

    # Both narrators referenced
    assert "1398" in result
    assert "1091" in result
    assert "https://www.hadathana.app/narrator?nid=1398" in result
    assert "https://www.hadathana.app/narrator?nid=1091" in result
    # Disambiguation does NOT include the full notes / classification block
    assert "ثقة حافظ متقن، إمام في الحديث والفقه" not in result

    # Stash carries disambiguation entries with kunya for FE rendering
    payload = _last_narrator["test-thread"]
    assert payload["mode"] == "disambiguation"
    assert len(payload["narrators"]) == 2
    rawi_ids = {n.rawi_id for n in payload["narrators"]}
    assert rawi_ids == {1398, 1091}
    # Every entry exposes kunya — the visual differentiator on FE cards
    for entry in payload["narrators"]:
        assert entry.kunya is not None


@pytest.mark.asyncio
async def test_search_narrators_rawi_id_lookup():
    """Direct rawi_id lookup skips name search and returns full profile."""
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections(
        bio_lookup={1398: ZUHRI_BIOGRAPHY},
    )

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({"rawi_id": 1398}, config=_RUNNABLE_CONFIG)

    assert "محمد بن مسلم الزهري" in result
    assert "1398" in result
    assert "ثقة حافظ متقن، إمام في الحديث والفقه" in result
    # name search must NOT have been called
    narrators.find.assert_not_called()

    payload = _last_narrator["test-thread"]
    assert payload["mode"] == "single"
    assert payload["narrator"].rawi_id == 1398


@pytest.mark.asyncio
async def test_search_narrators_no_results():
    """Both queries empty → Arabic no-results message."""
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections()  # both empty

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({"query": "اسم لا يوجد"}, config=_RUNNABLE_CONFIG)

    assert "لم يُعثر" in result or "لا يوجد" in result
    # No stash — router will classify as chitchat
    assert "test-thread" not in _last_narrator


@pytest.mark.asyncio
async def test_search_narrators_no_query_no_id():
    """Neither query nor rawi_id provided → guidance message."""
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections()

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({}, config=_RUNNABLE_CONFIG)

    assert "اسم" in result or "رقم" in result
    assert "test-thread" not in _last_narrator


@pytest.mark.asyncio
async def test_search_narrators_exact_chain_match_wins_priority():
    """
    When the user's query exactly matches a narrator's `name_in_chain_search`,
    that narrator must surface in the results — even if many other narrators
    contain the same fragment as a substring of their full names. This protects
    the canonical case ("عمر بن الخطاب" → Umar himself, not 16 of his descendants).
    """
    from app.chatbot.agent import search_narrators

    # 7 narrators all match "عمر بن الخطاب" via substring on full_name_search;
    # only one (Umar himself, rawi_id 1091) has an EXACT name_in_chain_search match.
    descendants = [
        {"rawi_id": 312,  "name_in_chain_plain": "حفص بن عاصم", "name_in_chain_search": "حفص بن عاصم", "full_name_plain": "حفص بن عاصم بن عمر بن الخطاب", "full_name_search": "حفص بن عاصم بن عمر بن الخطاب"},
        {"rawi_id": 1804, "name_in_chain_plain": "حفصة بنت عمر", "name_in_chain_search": "حفصه بنت عمر", "full_name_plain": "حفصة بنت عمر بن الخطاب", "full_name_search": "حفصه بنت عمر بن الخطاب"},
        {"rawi_id": 338,  "name_in_chain_plain": "حمزة بن عبد الله", "name_in_chain_search": "حمزه بن عبد الله", "full_name_plain": "حمزة بن عبد الله بن عمر بن الخطاب", "full_name_search": "حمزه بن عبد الله بن عمر بن الخطاب"},
        {"rawi_id": 461,  "name_in_chain_plain": "زيد بن عبد الله", "name_in_chain_search": "زيد بن عبد الله", "full_name_plain": "زيد بن عبد الله بن عمر بن الخطاب", "full_name_search": "زيد بن عبد الله بن عمر بن الخطاب"},
        {"rawi_id": 468,  "name_in_chain_plain": "سالم بن عبد الله", "name_in_chain_search": "سالم بن عبد الله", "full_name_plain": "سالم بن عبد الله بن عمر بن الخطاب", "full_name_search": "سالم بن عبد الله بن عمر بن الخطاب"},
        {"rawi_id": 664,  "name_in_chain_plain": "عاصم بن عمر", "name_in_chain_search": "عاصم بن عمر", "full_name_plain": "عاصم بن عمر بن الخطاب", "full_name_search": "عاصم بن عمر بن الخطاب"},
    ]
    umar = {
        "rawi_id": 1091,
        "name_in_chain_plain": "عمر بن الخطاب",
        "name_in_chain_search": "عمر بن الخطاب",
        "full_name_plain": "عمر بن الخطاب بن نفيل، أمير المؤمنين",
        "full_name_search": "عمر بن الخطاب بن نفيل، امير المومنين",
        "rank_plain": "صحابي",
    }

    # Mock returns: exact-chain query → just Umar; substring query → all descendants
    # (the real Mongo collection would do the filtering itself; we simulate the result).
    narrators = MagicMock()
    cursors = {"exact": _make_cursor([umar]), "substring": _make_cursor(descendants)}
    call_log = []

    def _find(filter_, **kwargs):
        # Exact match: one of the $or clauses uses an unanchored pattern with no \\s*
        # The exact-chain query targets `name_in_chain_search` with the literal value.
        if filter_.get("name_in_chain_search") == "عمر بن الخطاب":
            call_log.append("exact")
            return cursors["exact"]
        call_log.append("substring")
        return cursors["substring"]

    narrators.find = MagicMock(side_effect=_find)

    async def _find_one(query, *args, **kwargs):
        for d in [umar, *descendants]:
            if d["rawi_id"] == query.get("rawi_id"):
                return d
        return None
    narrators.find_one = AsyncMock(side_effect=_find_one)

    biographies = MagicMock()
    biographies.find = MagicMock(return_value=_make_cursor([]))
    biographies.find_one = AsyncMock(return_value=None)

    from app.chatbot.agent import _last_narrator
    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        await search_narrators.ainvoke({"query": "عمر بن الخطاب"}, config=_RUNNABLE_CONFIG)

    # Umar must appear in the disambiguation payload — and as the FIRST entry,
    # because exact name_in_chain matches outrank substring matches.
    payload = _last_narrator["test-thread"]
    rawi_ids = [n.rawi_id for n in payload["narrators"]]
    assert 1091 in rawi_ids, f"Umar (rawi_id 1091) must be in results, got {rawi_ids}"
    assert rawi_ids[0] == 1091, f"Umar must rank first (exact chain match), got {rawi_ids}"


@pytest.mark.asyncio
async def test_search_narrators_pattern_tolerates_missing_spaces():
    """
    User often types "ابوهريره" (no space) when the stored form is "أبو هريرة".
    Both the names pattern and the kunya pattern must match across an optional
    space — otherwise the unspaced query returns zero hits.
    """
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections()

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        await search_narrators.ainvoke({"query": "ابوهريره"}, config=_RUNNABLE_CONFIG)

    import re as _re
    names_filter = narrators.find.call_args.args[0]
    names_pattern = names_filter["$or"][0][next(iter(names_filter["$or"][0].keys()))]["$regex"]
    assert _re.search(names_pattern, "ابو هريره"), (
        f"Names pattern {names_pattern!r} must match the spaced normalized form 'ابو هريره'"
    )

    kunya_pattern = biographies.find.call_args.args[0]["summary.kunya_search"]["$regex"]
    assert _re.search(kunya_pattern, "ابو هريره"), (
        f"Kunya pattern {kunya_pattern!r} must match the spaced normalized form 'ابو هريره'"
    )


@pytest.mark.asyncio
async def test_search_narrators_query_targets_search_fields():
    """
    The names query must target `*_search` fields (fully normalized) and
    `summary.kunya` must be matched with a permissive pattern that tolerates
    hamza variants and ta-marbuta — otherwise narrators stored in formal
    Arabic (e.g. "أبو هريرة") never match a user query (e.g. "ابو هريره").
    """
    from app.chatbot.agent import search_narrators

    narrators, biographies = _make_collections()

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        await search_narrators.ainvoke({"query": "أبو هريرة"}, config=_RUNNABLE_CONFIG)

    # Query A — names search must hit `_search` fields, not `_plain`
    names_filter = narrators.find.call_args.args[0]
    or_clauses = names_filter["$or"]
    targeted_fields = {next(iter(c.keys())) for c in or_clauses}
    assert targeted_fields == {"name_in_chain_search", "full_name_search"}, (
        f"Names query must target _search fields, got: {targeted_fields}"
    )

    # Query B — kunya search must hit `summary.kunya_search` (the normalized mirror)
    kunya_filter = biographies.find.call_args.args[0]
    assert "summary.kunya_search" in kunya_filter, (
        f"Kunya query must target summary.kunya_search, got: {list(kunya_filter.keys())}"
    )


@pytest.mark.asyncio
async def test_search_narrators_mongo_error_returns_arabic_message():
    """Exception during query → graceful Arabic error message."""
    from app.chatbot.agent import search_narrators

    narrators = MagicMock()
    failing_cursor = MagicMock()
    failing_cursor.to_list = AsyncMock(side_effect=RuntimeError("mongo down"))
    narrators.find = MagicMock(return_value=failing_cursor)
    narrators.find_one = AsyncMock(return_value=None)

    biographies = MagicMock()
    biographies.find = MagicMock(return_value=_make_cursor([]))
    biographies.find_one = AsyncMock(return_value=None)

    from app.chatbot.agent import _last_narrator

    with patch("app.chatbot.agent.get_db"), \
         patch("app.chatbot.agent.get_client"), \
         patch("app.chatbot.agent.get_podia_narrators_collection", return_value=narrators), \
         patch("app.chatbot.agent.get_podia_narrators_tarajem_collection", return_value=biographies):
        result = await search_narrators.ainvoke({"query": "أي اسم"}, config=_RUNNABLE_CONFIG)

    # graceful, no exception
    assert "خطأ" in result or "حاول" in result
    # Error path must NOT populate the stash — router falls through to chitchat
    assert "test-thread" not in _last_narrator


# ── Prompt-rule regression guards ─────────────────────────────────────────────


def test_prompt_rule_9_present():
    """ARABIC_SYSTEM_PROMPT must contain Rule 9 forbidding fabricated rawi_id values."""
    from app.chatbot.prompts import ARABIC_SYSTEM_PROMPT

    assert "NEVER write" in ARABIC_SYSTEM_PROMPT or "لا تكتب" in ARABIC_SYSTEM_PROMPT, (
        "Rule 9 (no fabricated rawi_id) is missing from ARABIC_SYSTEM_PROMPT"
    )
    assert "رقم الراوي" in ARABIC_SYSTEM_PROMPT, (
        "Rule 9 must mention 'رقم الراوي' to be effective"
    )


def test_prompt_rule_10_present():
    """ARABIC_SYSTEM_PROMPT must contain Rule 10 requiring two tool calls + cited counts."""
    from app.chatbot.prompts import ARABIC_SYSTEM_PROMPT

    assert "search_narrators" in ARABIC_SYSTEM_PROMPT, (
        "Rule 10 must reference 'search_narrators' to direct the LLM"
    )
    assert "عدد الأحاديث" in ARABIC_SYSTEM_PROMPT, (
        "Rule 10 must mention 'عدد الأحاديث' so the LLM knows what to cite"
    )


def test_disambiguation_text_contains_only_returned_rawi_ids():
    """
    _build_disambiguation must only emit rawi_id values from the input docs.
    This is a regression guard against the bug where the LLM cited rawi_id 1070
    (علي بن عبد الله بن عباس) instead of 1066 (علي بن أبي طالب).
    The function itself can't fabricate IDs — this test locks that invariant.
    """
    from app.chatbot.agent import _build_disambiguation

    narrators = [
        {
            "rawi_id": 1066,
            "full_name_plain": "علي بن أبي طالب بن عبد المطلب الهاشمي",
            "rank_plain": "صحابي",
        },
        {
            "rawi_id": 271,
            "full_name_plain": "الحسن بن الحسن بن علي بن أبي طالب الهاشمي المدني",
            "rank_plain": "صدوق",
        },
        {
            "rawi_id": 289,
            "full_name_plain": "الحسن بن محمد بن علي بن أبي طالب الهاشمي",
            "rank_plain": "ثقة",
        },
    ]
    biographies = [
        {
            "rawi_id": 1066,
            "summary": {"full_name": "علي بن أبي طالب", "kunya": "أبو الحسن", "classification": "صحابي"},
        },
        {
            "rawi_id": 271,
            "summary": {"full_name": "الحسن بن الحسن", "kunya": "أبو محمد", "classification": "صدوق"},
        },
        {
            "rawi_id": 289,
            "summary": {"full_name": "الحسن بن محمد", "kunya": "أبو محمد", "classification": "ثقة"},
        },
    ]
    bio_map = {b["rawi_id"]: b for b in biographies}
    pairs = [(n, bio_map.get(n["rawi_id"])) for n in narrators]

    _entries, text = _build_disambiguation(pairs)

    assert "1066" in text
    assert "271" in text
    assert "289" in text
    assert "1070" not in text, (
        "_build_disambiguation must not emit rawi_id 1070 — it was not in the input"
    )
