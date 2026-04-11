"""
Data presence smoke tests — verify that actual data is loaded in the database.

Requires the relevant stack to be up before running:
  make dev   → test against http://localhost:8001
  make prod  → test against http://localhost:8000

Run via:
  APP_ENV=dev pytest tests/test_data_presence.py -v
"""
import os
import pytest
import httpx

_ENV = os.getenv("APP_ENV", "dev")
BASE_URL = "http://localhost:8001" if _ENV == "dev" else "http://localhost:8000"


@pytest.fixture(scope="session", autouse=True)
def require_api_up():
    """Fail fast with a clear message if the target API is not running."""
    try:
        r = httpx.get(f"{BASE_URL}/health", timeout=5)
        r.raise_for_status()
    except Exception:
        pytest.skip(
            f"API not reachable at {BASE_URL} — run `make {'dev' if _ENV == 'dev' else 'prod'}` first"
        )


# ── Health ─────────────────────────────────────────────────────────────────────

def test_health_collections_non_empty():
    """Health endpoint reports all major collections as non-empty."""
    r = httpx.get(f"{BASE_URL}/health", timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"

    collections = body.get("collections", {})
    assert collections, "Health response must include a 'collections' dict"

    expected = [
        "processed_podia_books",
        "processed_podia_narrators",
        "raw_shamela_books",
        "raw_shamela_narrators",
    ]
    for name in expected:
        assert name in collections, f"Collection '{name}' missing from health response"
        assert collections[name] > 0, f"Collection '{name}' is empty (count={collections[name]})"


# ── Shamela hadiths (v1) ───────────────────────────────────────────────────────

def test_shamela_hadiths_total():
    """Shamela hadith collection has at least 7000 hadiths."""
    r = httpx.get(f"{BASE_URL}/api/v1/hadiths", params={"limit": 1}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert "total" in body
    assert body["total"] >= 7000, f"Expected >=7000 Shamela hadiths, got {body['total']}"


def test_shamela_hadiths_list_returns_items():
    """GET /api/v1/hadiths returns a non-empty page with correctly shaped items."""
    r = httpx.get(f"{BASE_URL}/api/v1/hadiths", params={"limit": 5}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) == 5

    for hadith in body["items"]:
        assert hadith.get("id"), "Hadith must have an id"
        assert hadith.get("hadith_index") is not None, "Hadith must have hadith_index"
        assert hadith.get("hadith_plain"), "Hadith must have non-empty hadith_plain"
        assert isinstance(hadith.get("chains"), list), "Hadith must have a chains list"
        assert len(hadith["chains"]) > 0, "Hadith must have at least one chain"


def test_shamela_hadith_get_by_index():
    """GET /api/v1/hadiths/1 returns the first hadith with full fields."""
    r = httpx.get(f"{BASE_URL}/api/v1/hadiths/1", timeout=10)
    assert r.status_code == 200
    h = r.json()
    assert h.get("hadith_index") == 1
    assert h.get("hadith_plain"), "hadith_plain must be non-empty"
    assert isinstance(h.get("unique_narrators"), list)
    assert len(h["unique_narrators"]) > 0, "First hadith must have narrators"


def test_shamela_hadith_text_search():
    """Text search on Shamela hadiths returns relevant results."""
    r = httpx.get(
        f"{BASE_URL}/api/v1/hadiths",
        params={"hadith_plain": "النية", "limit": 5},
        timeout=10,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0, "Search for النية should return hadiths"
    assert len(body["items"]) > 0


def test_shamela_hadith_not_found():
    """GET /api/v1/hadiths/999999 returns 404."""
    r = httpx.get(f"{BASE_URL}/api/v1/hadiths/999999", timeout=10)
    assert r.status_code == 404


# ── Shamela narrators (v1) ─────────────────────────────────────────────────────

def test_shamela_narrators_total():
    """Shamela narrator collection has at least 1000 narrators."""
    r = httpx.get(f"{BASE_URL}/api/v1/narrators", params={"limit": 1}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] >= 1000, f"Expected >=1000 Shamela narrators, got {body['total']}"


def test_shamela_narrators_list_shape():
    """GET /api/v1/narrators returns items with required fields."""
    r = httpx.get(f"{BASE_URL}/api/v1/narrators", params={"limit": 5}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) == 5

    for narrator in body["items"]:
        assert narrator.get("id"), "Narrator must have an id"
        assert narrator.get("narrator_id") is not None, "Narrator must have narrator_id"
        assert narrator.get("name"), "Narrator must have a name"


def test_shamela_narrator_get_by_id():
    """GET /api/v1/narrators/822 returns a known narrator (Abu Hurayra)."""
    r = httpx.get(f"{BASE_URL}/api/v1/narrators/822", timeout=10)
    assert r.status_code == 200
    n = r.json()
    assert n.get("narrator_id") is not None
    assert n.get("name"), "Narrator name must be non-empty"


def test_shamela_narrator_stats():
    """GET /api/v1/narrators/822/stats returns teacher/student relationships."""
    r = httpx.get(f"{BASE_URL}/api/v1/narrators/822/stats", timeout=10)
    assert r.status_code == 200
    stats = r.json()
    assert "narrator_id" in stats
    assert "hadith_count" in stats
    assert stats["hadith_count"] > 0, "Abu Hurayra must appear in hadiths"
    assert isinstance(stats.get("teachers"), list)
    assert isinstance(stats.get("students"), list)
    assert len(stats["students"]) > 0, "Abu Hurayra must have students in the chain"


def test_shamela_narrator_name_search():
    """Name search on Shamela narrators returns results."""
    r = httpx.get(
        f"{BASE_URL}/api/v1/narrators",
        params={"name_plain": "البخاري", "limit": 5},
        timeout=10,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0, "Search for البخاري should return narrators"


# ── Podia hadiths (v2) ────────────────────────────────────────────────────────

def test_podia_hadiths_total():
    """Podia hadith collection has at least 7000 hadiths."""
    r = httpx.get(f"{BASE_URL}/api/v2/hadiths", params={"limit": 1}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] >= 7000, f"Expected >=7000 Podia hadiths, got {body['total']}"


def test_podia_hadiths_list_shape():
    """GET /api/v2/hadiths returns items with required fields."""
    r = httpx.get(f"{BASE_URL}/api/v2/hadiths", params={"limit": 5}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) == 5

    for hadith in body["items"]:
        assert hadith.get("id"), "Hadith must have an id"
        assert hadith.get("hadith_url"), "Hadith must have a hadith_url"
        assert hadith.get("hadith_text_plain"), "Hadith must have non-empty hadith_text_plain"
        assert hadith.get("matn_text_plain"), "Hadith must have non-empty matn_text_plain"
        assert hadith.get("sanad_text_plain"), "Hadith must have non-empty sanad_text_plain"
        assert isinstance(hadith.get("chains"), list), "Hadith must have a chains list"
        assert len(hadith["chains"]) > 0, "Hadith must have at least one chain"
        assert isinstance(hadith.get("narrators"), list)
        assert len(hadith["narrators"]) > 0, "Hadith must have at least one narrator"


def test_podia_hadiths_have_topics():
    """At least some Podia hadiths have topics assigned."""
    r = httpx.get(
        f"{BASE_URL}/api/v2/hadiths",
        params={"limit": 50},
        timeout=10,
    )
    assert r.status_code == 200
    items = r.json()["items"]
    hadiths_with_topics = [h for h in items if h.get("topics")]
    assert len(hadiths_with_topics) > 0, "At least some hadiths in first 50 must have topics"


def test_podia_hadith_get_by_index():
    """GET /api/v2/hadiths/1 returns the first hadith with full structure."""
    r = httpx.get(f"{BASE_URL}/api/v2/hadiths/1", timeout=10)
    assert r.status_code == 200
    h = r.json()
    assert 1 in h.get("hadith_indices", []), "hadith_indices must include 1"
    assert h.get("book"), "book must be non-empty"
    assert h.get("chapter"), "chapter must be non-empty"


def test_podia_hadith_text_search():
    """Text search on Podia hadiths returns results."""
    r = httpx.get(
        f"{BASE_URL}/api/v2/hadiths",
        params={"hadith_text_plain": "النية", "limit": 5},
        timeout=10,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0, "Search for النية should return Podia hadiths"


def test_podia_hadith_filter_by_topic():
    """Filtering Podia hadiths by topic returns relevant results."""
    # First, get a real topic from the topics endpoint
    topics_r = httpx.get(f"{BASE_URL}/api/v2/topics", params={"limit": 1}, timeout=10)
    assert topics_r.status_code == 200
    topics = topics_r.json()["items"]
    assert topics, "Topics list must be non-empty to run this test"
    topic = topics[0]["topic"]

    r = httpx.get(
        f"{BASE_URL}/api/v2/hadiths",
        params={"topic": topic, "limit": 5},
        timeout=10,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0, f"Filtering by topic '{topic}' should return hadiths"
    for hadith in body["items"]:
        assert topic in hadith.get("topics", []), f"Returned hadith must contain topic '{topic}'"


def test_podia_hadith_not_found():
    """GET /api/v2/hadiths/999999 returns 404."""
    r = httpx.get(f"{BASE_URL}/api/v2/hadiths/999999", timeout=10)
    assert r.status_code == 404


# ── Podia narrators (v2) ──────────────────────────────────────────────────────

def test_podia_narrators_total():
    """Podia narrator collection has at least 500 narrators."""
    r = httpx.get(f"{BASE_URL}/api/v2/narrators", params={"limit": 1}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] >= 500, f"Expected >=500 Podia narrators, got {body['total']}"


def test_podia_narrators_list_shape():
    """GET /api/v2/narrators returns items with required fields."""
    r = httpx.get(f"{BASE_URL}/api/v2/narrators", params={"limit": 5}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) == 5

    for narrator in body["items"]:
        assert narrator.get("id"), "Narrator must have an id"
        assert narrator.get("rawi_id") is not None, "Narrator must have rawi_id"
        assert narrator.get("full_name"), "Narrator must have a full_name"


def test_podia_narrator_get_by_id():
    """GET /api/v2/narrators/1 returns the first narrator with full fields."""
    r = httpx.get(f"{BASE_URL}/api/v2/narrators/1", timeout=10)
    assert r.status_code == 200
    n = r.json()
    assert n.get("rawi_id") == 1
    assert n.get("full_name"), "full_name must be non-empty"


def test_podia_narrator_name_search():
    """Name search on Podia narrators returns results."""
    r = httpx.get(
        f"{BASE_URL}/api/v2/narrators",
        params={"full_name_plain": "ابو هريره", "limit": 5},
        timeout=10,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0, "Search for ابو هريره should return Podia narrators"


def test_podia_narrator_stats():
    """GET /api/v2/narrators/{rawi_id}/stats returns teacher/student data."""
    # Get a valid rawi_id from the narrators list first
    list_r = httpx.get(f"{BASE_URL}/api/v2/narrators", params={"limit": 1}, timeout=10)
    rawi_id = list_r.json()["items"][0]["rawi_id"]

    r = httpx.get(f"{BASE_URL}/api/v2/narrators/{rawi_id}/stats", timeout=10)
    assert r.status_code == 200
    stats = r.json()
    assert "rawi_id" in stats
    assert "hadith_count" in stats
    assert isinstance(stats.get("teachers"), list)
    assert isinstance(stats.get("students"), list)


def test_podia_narrator_not_found():
    """GET /api/v2/narrators/999999 returns 404."""
    r = httpx.get(f"{BASE_URL}/api/v2/narrators/999999", timeout=10)
    assert r.status_code == 404


# ── Topics (v2) ───────────────────────────────────────────────────────────────

def test_topics_non_empty():
    """GET /api/v2/topics returns a list of topics with counts."""
    r = httpx.get(f"{BASE_URL}/api/v2/topics", timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0, "Topics list must be non-empty"
    assert len(body["items"]) > 0

    for item in body["items"][:5]:
        assert item.get("topic"), "Topic entry must have a non-empty topic string"
        assert item.get("count", 0) > 0, "Topic count must be > 0"


def test_topics_sorted_by_count_descending():
    """Topics are returned sorted by count descending."""
    r = httpx.get(f"{BASE_URL}/api/v2/topics", timeout=10)
    assert r.status_code == 200
    items = r.json()["items"]
    counts = [item["count"] for item in items[:10]]
    assert counts == sorted(counts, reverse=True), "Topics must be sorted by count descending"


def test_hadiths_by_topic():
    """GET /api/v2/topics/{topic}/hadiths returns hadiths for a valid topic."""
    # Grab the most-populated topic
    topics_r = httpx.get(f"{BASE_URL}/api/v2/topics", timeout=10)
    topic = topics_r.json()["items"][0]["topic"]
    expected_count = topics_r.json()["items"][0]["count"]

    r = httpx.get(
        f"{BASE_URL}/api/v2/topics/{topic}/hadiths",
        params={"limit": 5},
        timeout=10,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == expected_count, (
        f"Topic hadiths total ({body['total']}) must match topics count ({expected_count})"
    )
    assert len(body["items"]) > 0
    for hadith in body["items"]:
        assert topic in hadith.get("topics", []), f"Returned hadith must contain topic '{topic}'"


def test_hadiths_by_topic_pagination():
    """Pagination works correctly for topic hadiths (skip/limit)."""
    topics_r = httpx.get(f"{BASE_URL}/api/v2/topics", timeout=10)
    topic = topics_r.json()["items"][0]["topic"]

    page1 = httpx.get(
        f"{BASE_URL}/api/v2/topics/{topic}/hadiths",
        params={"skip": 0, "limit": 3},
        timeout=10,
    ).json()
    page2 = httpx.get(
        f"{BASE_URL}/api/v2/topics/{topic}/hadiths",
        params={"skip": 3, "limit": 3},
        timeout=10,
    ).json()

    ids_page1 = {h["id"] for h in page1["items"]}
    ids_page2 = {h["id"] for h in page2["items"]}
    assert ids_page1.isdisjoint(ids_page2), "Paginated pages must not overlap"
