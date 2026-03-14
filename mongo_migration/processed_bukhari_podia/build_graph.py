"""
build_graph.py — Ingest Bukhari Podia data into Neo4j

Uses the advanced extraction file (with multi-chain, transmission types,
sanad/matn separation) — data that is graph-native and NOT duplicated
in MongoDB.

MongoDB owns: flat hadith text, narrator profiles, tarajem, stats (search/API)
Neo4j  owns: chain graph, transmission semantics, multi-chain structure (traversal)

Input:
  bukhari_pedia_advanced_extraction_results.json  → chains, sanad, matn, tawabi
  bukhari_podia_narrators.jsonl                   → narrator biographical props
  narrators_tarajem.jsonl                         → narrator tarajim (stored as JSON)

Schema:
  (:PodiaBook {name, name_plain})
  (:PodiaChapter {chapter_key, name, name_plain, book_name})
  (:PodiaHadith {hadith_id, hadith_url, hadith_indices,
                  sanad_text, matn_text, tawabi_text,
                  hadith_text, hadith_text_plain})
  (:PodiaChain {chain_id, hadith_id, type, length})
  (:PodiaNarrator {rawi_id, name_in_chain_clean, name_in_chain_plain,
                    full_name, full_name_plain, rank, rank_plain,
                    url, tarajim_sources, tarajim_json, narrator_info_json})

  (PodiaHadith)-[:IN_CHAPTER]->(PodiaChapter)
  (PodiaChapter)-[:IN_BOOK]->(PodiaBook)
  (PodiaHadith)-[:HAS_CHAIN {chain_id, type}]->(PodiaChain)
  (PodiaChain)-[:POSITION {pos, transmission, transmission_type,
                            is_explicit_hearing, role}]->(PodiaNarrator)
  (PodiaNarrator)-[:NARRATED {position, hadith_id, chain_id,
                               transmission, transmission_type}]->(PodiaNarrator)
  (PodiaNarrator)-[:TRANSMITTED_HADITH {position, chain_id}]->(PodiaHadith)

Usage:
    python mongo_migration/processed_bukhari_podia/build_graph.py
    python mongo_migration/processed_bukhari_podia/build_graph.py --dry-run
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.parent.parent / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass

try:
    from neo4j import GraphDatabase
    _NEO4J_AVAILABLE = True
except ImportError:
    _NEO4J_AVAILABLE = False

_TASHKEEL_RE = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)


def strip_tashkeel(text: str) -> str:
    if not text:
        return text
    return _TASHKEEL_RE.sub("", text)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def extract_hadith_id(url: str) -> str:
    """Extract page ID from URL → 'podia_{page_id}'."""
    # https://www.bukhari-pedia.net/book/matn_bukhari/3 → podia_3
    m = re.search(r"/matn_bukhari/(\d+)", url)
    if m:
        return f"podia_{m.group(1)}"
    return f"podia_{url.rsplit('/', 1)[-1]}"


def load_advanced_hadiths(path: str) -> list[dict]:
    """Load advanced extraction JSON (with chains, sanad, matn)."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    for rec in data:
        rec["hadith_id"] = extract_hadith_id(rec["hadith_url"])
    print(f"  {len(data)} hadith records loaded.")
    return data


def load_narrators(path: str) -> dict[int, dict]:
    """Load narrator profiles from JSONL, keyed by rawi_id."""
    narrators = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                continue
            narrators[doc["rawi_id"]] = doc
    print(f"  {len(narrators)} narrator profiles loaded.")
    return narrators


def load_tarajem(path: str) -> dict[int, dict]:
    """Load tarajem from JSONL, keyed by rawi_id."""
    tarajem = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                continue
            tarajem[doc["rawi_id"]] = doc
    print(f"  {len(tarajem)} tarajem records loaded.")
    return tarajem


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

def _run_batch(session, query: str, batch: list) -> int:
    result = session.run(query, batch=batch)
    summary = result.consume()
    return summary.counters.nodes_created + summary.counters.relationships_created


def create_constraints(driver) -> None:
    queries = [
        "CREATE CONSTRAINT podia_book_name_unique IF NOT EXISTS FOR (b:PodiaBook) REQUIRE b.name IS UNIQUE",
        "CREATE CONSTRAINT podia_chapter_key_unique IF NOT EXISTS FOR (c:PodiaChapter) REQUIRE c.chapter_key IS UNIQUE",
        "CREATE CONSTRAINT podia_hadith_id_unique IF NOT EXISTS FOR (h:PodiaHadith) REQUIRE h.hadith_id IS UNIQUE",
        "CREATE CONSTRAINT podia_narrator_rawi_id_unique IF NOT EXISTS FOR (n:PodiaNarrator) REQUIRE n.rawi_id IS UNIQUE",
        "CREATE CONSTRAINT podia_chain_key_unique IF NOT EXISTS FOR (c:PodiaChain) REQUIRE c.chain_key IS UNIQUE",
        # Indexes
        "CREATE INDEX podia_narrator_full_name_plain_idx IF NOT EXISTS FOR (n:PodiaNarrator) ON (n.full_name_plain)",
        "CREATE INDEX podia_narrator_rank_plain_idx IF NOT EXISTS FOR (n:PodiaNarrator) ON (n.rank_plain)",
        "CREATE INDEX podia_chain_type_idx IF NOT EXISTS FOR (c:PodiaChain) ON (c.type)",
        "CREATE INDEX podia_book_name_plain_idx IF NOT EXISTS FOR (b:PodiaBook) ON (b.name_plain)",
        # Full-text indexes
        "CREATE FULLTEXT INDEX podia_hadith_text_ft IF NOT EXISTS FOR (h:PodiaHadith) ON EACH [h.hadith_text_plain, h.matn_text_plain]",
        "CREATE FULLTEXT INDEX podia_narrator_name_ft IF NOT EXISTS FOR (n:PodiaNarrator) ON EACH [n.name_in_chain_plain, n.full_name_plain]",
    ]
    with driver.session() as session:
        for q in queries:
            session.run(q)
    print("  Constraints and indexes created.")


def ingest_books(driver, records: list[dict], batch_size: int = 200) -> int:
    seen = {}
    for r in records:
        name = r.get("book_name", "")
        if not name or name in seen:
            continue
        seen[name] = {
            "name": name,
            "name_plain": strip_tashkeel(name),
            "source": "bukhari_podia",
        }

    unique = list(seen.values())
    query = """
    UNWIND $batch AS row
    MERGE (b:PodiaBook {name: row.name})
    SET b.name_plain = row.name_plain, b.source = row.source
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(unique), batch_size):
            total += _run_batch(session, query, unique[i:i + batch_size])
    return total


def ingest_chapters(driver, records: list[dict], batch_size: int = 200) -> int:
    seen = {}
    for r in records:
        book = r.get("book_name", "")
        chapter = r.get("chapter", "")
        if not chapter:
            continue
        key = f"{book}||{chapter}"
        if key in seen:
            continue
        seen[key] = {
            "chapter_key": key,
            "name": chapter,
            "name_plain": strip_tashkeel(chapter),
            "book_name": book,
            "source": "bukhari_podia",
        }

    unique = list(seen.values())
    query = """
    UNWIND $batch AS row
    MERGE (c:PodiaChapter {chapter_key: row.chapter_key})
    SET c.name = row.name, c.name_plain = row.name_plain,
        c.book_name = row.book_name, c.source = row.source
    WITH c, row
    MATCH (b:PodiaBook {name: row.book_name})
    MERGE (c)-[:IN_BOOK]->(b)
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(unique), batch_size):
            total += _run_batch(session, query, unique[i:i + batch_size])
    return total


def ingest_hadiths(driver, records: list[dict], batch_size: int = 200) -> int:
    with_chapter = []
    without_chapter = []
    for r in records:
        hadith_text = r.get("hadith_text_clean") or r.get("hadith_text", "")
        doc = {
            "hadith_id": r["hadith_id"],
            "hadith_url": r["hadith_url"],
            "hadith_indices": r.get("hadith_indices", []),
            "hadith_text": hadith_text,
            "hadith_text_plain": strip_tashkeel(hadith_text),
            "sanad_text": r.get("sanad_text") or "",
            "sanad_text_plain": strip_tashkeel(r.get("sanad_text") or ""),
            "matn_text": r.get("matn_text") or "",
            "matn_text_plain": strip_tashkeel(r.get("matn_text") or ""),
            "tawabi_text": r.get("tawabi_text") or "",
            "book": r.get("book_name", ""),
            "chapter": r.get("chapter", ""),
            "source": "bukhari_podia",
        }
        if doc["chapter"]:
            doc["chapter_key"] = f"{doc['book']}||{doc['chapter']}"
            with_chapter.append(doc)
        else:
            without_chapter.append(doc)

    query_with_ch = """
    UNWIND $batch AS row
    MERGE (h:PodiaHadith {hadith_id: row.hadith_id})
    SET h.hadith_url = row.hadith_url,
        h.hadith_indices = row.hadith_indices,
        h.hadith_text = row.hadith_text,
        h.hadith_text_plain = row.hadith_text_plain,
        h.sanad_text = row.sanad_text,
        h.sanad_text_plain = row.sanad_text_plain,
        h.matn_text = row.matn_text,
        h.matn_text_plain = row.matn_text_plain,
        h.tawabi_text = row.tawabi_text,
        h.book = row.book,
        h.chapter = row.chapter,
        h.source = row.source
    WITH h, row
    MATCH (c:PodiaChapter {chapter_key: row.chapter_key})
    MERGE (h)-[:IN_CHAPTER]->(c)
    """
    query_no_ch = """
    UNWIND $batch AS row
    MERGE (h:PodiaHadith {hadith_id: row.hadith_id})
    SET h.hadith_url = row.hadith_url,
        h.hadith_indices = row.hadith_indices,
        h.hadith_text = row.hadith_text,
        h.hadith_text_plain = row.hadith_text_plain,
        h.sanad_text = row.sanad_text,
        h.sanad_text_plain = row.sanad_text_plain,
        h.matn_text = row.matn_text,
        h.matn_text_plain = row.matn_text_plain,
        h.tawabi_text = row.tawabi_text,
        h.book = row.book,
        h.chapter = row.chapter,
        h.source = row.source
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(with_chapter), batch_size):
            total += _run_batch(session, query_with_ch, with_chapter[i:i + batch_size])
        for i in range(0, len(without_chapter), batch_size):
            total += _run_batch(session, query_no_ch, without_chapter[i:i + batch_size])
    return total


def ingest_narrators(
    driver,
    rawi_ids: set[int],
    narrators_data: dict[int, dict],
    tarajem_data: dict[int, dict],
    batch_size: int = 200,
) -> int:
    narrator_list = []
    for rid in rawi_ids:
        nd = narrators_data.get(rid, {})
        td = tarajem_data.get(rid, {})

        # Extract queryable tarjama source names
        tarajim = td.get("tarajim", [])
        tarajim_sources = [t.get("source", "") for t in tarajim if t.get("source")]

        narrator_list.append({
            "rawi_id": rid,
            "name_in_chain": nd.get("name_in_chain", ""),
            "name_in_chain_clean": nd.get("name_in_chain_clean", ""),
            "name_in_chain_plain": nd.get("name_in_chain_plain", ""),
            "full_name": nd.get("full_name") or td.get("full_name", ""),
            "full_name_plain": nd.get("full_name_plain") or td.get("full_name_plain", ""),
            "rank": nd.get("rank") or td.get("rank", ""),
            "rank_plain": nd.get("rank_plain") or td.get("rank_plain", ""),
            "url": td.get("url", ""),
            "tarajim_sources": tarajim_sources,
            "tarajim_json": json.dumps(tarajim, ensure_ascii=False) if tarajim else "[]",
            "narrator_info_json": json.dumps(td.get("narrator_info", []), ensure_ascii=False),
            "source": "bukhari_podia",
        })

    query = """
    UNWIND $batch AS row
    MERGE (n:PodiaNarrator {rawi_id: row.rawi_id})
    SET n.name_in_chain = row.name_in_chain,
        n.name_in_chain_clean = row.name_in_chain_clean,
        n.name_in_chain_plain = row.name_in_chain_plain,
        n.full_name = row.full_name,
        n.full_name_plain = row.full_name_plain,
        n.rank = row.rank,
        n.rank_plain = row.rank_plain,
        n.url = row.url,
        n.tarajim_sources = row.tarajim_sources,
        n.tarajim_json = row.tarajim_json,
        n.narrator_info_json = row.narrator_info_json,
        n.source = row.source
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(narrator_list), batch_size):
            total += _run_batch(session, query, narrator_list[i:i + batch_size])
    return total


def ingest_chains(driver, records: list[dict], batch_size: int = 200) -> int:
    """Create PodiaChain nodes + HAS_CHAIN, POSITION, NARRATED, TRANSMITTED_HADITH."""
    chain_nodes = []
    has_chain_rels = []
    position_rels = []
    narrated_rels = []
    transmitted_rels = []

    for r in records:
        hadith_id = r["hadith_id"]
        for chain in r.get("chains", []):
            chain_id = chain["chain_id"]
            chain_type = chain.get("type", "primary")
            narrators = chain.get("narrators", [])
            chain_key = f"{hadith_id}_{chain_id}"

            chain_nodes.append({
                "chain_key": chain_key,
                "chain_id": chain_id,
                "hadith_id": hadith_id,
                "type": chain_type,
                "length": len(narrators),
            })

            has_chain_rels.append({
                "hadith_id": hadith_id,
                "chain_key": chain_key,
                "chain_id": chain_id,
                "type": chain_type,
            })

            for pos, n in enumerate(narrators):
                position_rels.append({
                    "chain_key": chain_key,
                    "rawi_id": n["rawi_id"],
                    "pos": pos,
                    "transmission": n.get("transmission") or "",
                    "transmission_type": n.get("transmission_type") or "",
                    "is_explicit_hearing": n.get("is_explicit_hearing", False),
                    "role": n.get("role", "narrator"),
                })

            n_count = len(narrators)
            for i in range(n_count - 1):
                narrated_rels.append({
                    "from_id": narrators[i]["rawi_id"],
                    "to_id": narrators[i + 1]["rawi_id"],
                    "position": i,
                    "hadith_id": hadith_id,
                    "chain_id": chain_id,
                    "transmission": narrators[i].get("transmission") or "",
                    "transmission_type": narrators[i].get("transmission_type") or "",
                })

            if n_count > 0:
                transmitted_rels.append({
                    "rawi_id": narrators[-1]["rawi_id"],
                    "hadith_id": hadith_id,
                    "chain_id": chain_id,
                    "position": n_count - 1,
                })

    # 1. Chain nodes
    chain_query = """
    UNWIND $batch AS row
    MERGE (c:PodiaChain {chain_key: row.chain_key})
    SET c.chain_id = row.chain_id, c.hadith_id = row.hadith_id,
        c.type = row.type, c.length = row.length
    """
    # 2. HAS_CHAIN
    has_chain_query = """
    UNWIND $batch AS row
    MATCH (h:PodiaHadith {hadith_id: row.hadith_id})
    MATCH (c:PodiaChain {chain_key: row.chain_key})
    CREATE (h)-[:HAS_CHAIN {chain_id: row.chain_id, type: row.type}]->(c)
    """
    # 3. POSITION
    position_query = """
    UNWIND $batch AS row
    MATCH (c:PodiaChain {chain_key: row.chain_key})
    MATCH (n:PodiaNarrator {rawi_id: row.rawi_id})
    CREATE (c)-[:POSITION {pos: row.pos, transmission: row.transmission,
                           transmission_type: row.transmission_type,
                           is_explicit_hearing: row.is_explicit_hearing,
                           role: row.role}]->(n)
    """
    # 4. NARRATED
    narrated_query = """
    UNWIND $batch AS row
    MATCH (a:PodiaNarrator {rawi_id: row.from_id})
    MATCH (b:PodiaNarrator {rawi_id: row.to_id})
    CREATE (a)-[:NARRATED {position: row.position, hadith_id: row.hadith_id,
                           chain_id: row.chain_id, transmission: row.transmission,
                           transmission_type: row.transmission_type}]->(b)
    """
    # 5. TRANSMITTED_HADITH
    transmitted_query = """
    UNWIND $batch AS row
    MATCH (n:PodiaNarrator {rawi_id: row.rawi_id})
    MATCH (h:PodiaHadith {hadith_id: row.hadith_id})
    CREATE (n)-[:TRANSMITTED_HADITH {position: row.position, chain_id: row.chain_id}]->(h)
    """

    total = 0
    with driver.session() as session:
        print("    Chain nodes...")
        for i in range(0, len(chain_nodes), batch_size):
            total += _run_batch(session, chain_query, chain_nodes[i:i + batch_size])
        print("    HAS_CHAIN relationships...")
        for i in range(0, len(has_chain_rels), batch_size):
            total += _run_batch(session, has_chain_query, has_chain_rels[i:i + batch_size])
        print("    POSITION relationships...")
        for i in range(0, len(position_rels), batch_size):
            total += _run_batch(session, position_query, position_rels[i:i + batch_size])
        print("    NARRATED relationships...")
        for i in range(0, len(narrated_rels), batch_size):
            total += _run_batch(session, narrated_query, narrated_rels[i:i + batch_size])
        print("    TRANSMITTED_HADITH relationships...")
        for i in range(0, len(transmitted_rels), batch_size):
            total += _run_batch(session, transmitted_query, transmitted_rels[i:i + batch_size])

    return total


# ---------------------------------------------------------------------------
# Schema description
# ---------------------------------------------------------------------------

SCHEMA_DESCRIPTION = """\
# Bukhari Podia — Neo4j Graph Schema

Source: bukhari-pedia.net (Sahih al-Bukhari with LLM-extracted chain analysis)

---

## Node Labels

### PodiaBook
A major section (كتاب) of Sahih al-Bukhari.
- `name` (String): Arabic book name, e.g. "كتاب الإيمان"
- `name_plain` (String): Without tashkeel

### PodiaChapter
A sub-section (باب) within a book.
- `chapter_key` (String): Synthetic unique key "{book_name}||{chapter_name}"
- `name` / `name_plain` (String)
- `book_name` (String): Parent book name

### PodiaHadith
A hadith record with separated sanad and matn.
- `hadith_id` (String): "podia_{page_id}" from URL
- `hadith_url` (String): Original URL
- `hadith_indices` (List[Int]): Bukhari numbering, e.g. [1] or [3, 4]
- `hadith_text` / `hadith_text_plain` (String): Full text
- `sanad_text` / `sanad_text_plain` (String): Chain of narrators text
- `matn_text` / `matn_text_plain` (String): Hadith body text
- `tawabi_text` (String): Follow-up narrations (تصابع)
- `book`, `chapter` (String): Denormalized

### PodiaChain
A single chain of narration within a hadith. Multi-chain hadiths have multiple.
- `chain_key` (String): "{hadith_id}_{chain_id}"
- `chain_id` (String): e.g. "chain_1", "chain_2"
- `hadith_id` (String): Parent hadith
- `type` (String): "primary", "nested", or "follow_up"
- `length` (Int): Number of narrators

### PodiaNarrator
A narrator in the transmission chain with biographical data.
- `rawi_id` (Int): Unique ID from bukhari-pedia.net
- `name_in_chain` / `name_in_chain_clean` / `name_in_chain_plain` (String)
- `full_name` / `full_name_plain` (String)
- `rank` / `rank_plain` (String): Reliability grade
- `url` (String): Profile URL
- `tarajim_sources` (List[String]): Names of biographical sources
- `tarajim_json` (String): JSON-encoded biographical entries
- `narrator_info_json` (String): JSON-encoded narrator metadata

---

## Relationship Types

### (PodiaHadith)-[:IN_CHAPTER]->(PodiaChapter)
Hadith belongs to a chapter. Some hadiths have no chapter.

### (PodiaChapter)-[:IN_BOOK]->(PodiaBook)
Chapter belongs to a book.

### (PodiaHadith)-[:HAS_CHAIN {chain_id, type}]->(PodiaChain)
Links a hadith to its chain(s). Types: "primary", "nested", "follow_up".

### (PodiaChain)-[:POSITION {pos, transmission, transmission_type, is_explicit_hearing, role}]->(PodiaNarrator)
Links a chain to each narrator with their position and transmission details.
- `pos` (Int): 0-based position in chain
- `transmission` (String): Arabic verb, e.g. "حدثنا", "عن", "أخبرنا"
- `transmission_type` (String): "samaa", "anana", "samaa_or_ard", "ijaza_or_munawala", "mukataba", "ambiguous", "unknown"
- `is_explicit_hearing` (Boolean): Whether transmission is explicitly heard
- `role` (String): "narrator" or "lead" (companion/source)

### (PodiaNarrator)-[:NARRATED {position, hadith_id, chain_id, transmission, transmission_type}]->(PodiaNarrator)
Direct transmission link between consecutive narrators in a chain.

### (PodiaNarrator)-[:TRANSMITTED_HADITH {position, chain_id}]->(PodiaHadith)
Links the last narrator (lead/companion) to the hadith.

---

## Key Query Patterns

```cypher
-- All chains for a hadith
MATCH (h:PodiaHadith {hadith_id: 'podia_3'})-[:HAS_CHAIN]->(c:PodiaChain)
MATCH (c)-[p:POSITION]->(n:PodiaNarrator)
RETURN c.chain_id, c.type, p.pos, n.full_name, p.transmission_type
ORDER BY c.chain_id, p.pos

-- Find hadiths with anana (عنعنة) chains
MATCH (c:PodiaChain)-[p:POSITION]->(n:PodiaNarrator)
WHERE p.transmission_type = 'anana'
WITH c, count(p) AS anana_count
MATCH (h:PodiaHadith)-[:HAS_CHAIN]->(c)
RETURN h.hadith_id, c.chain_id, anana_count
ORDER BY anana_count DESC LIMIT 10

-- Narrator teacher-student network
MATCH (teacher:PodiaNarrator)-[r:NARRATED]->(student:PodiaNarrator)
WHERE teacher.rawi_id = 729
RETURN student.full_name, r.transmission_type, count(r) AS freq
ORDER BY freq DESC

-- Multi-chain hadiths
MATCH (h:PodiaHadith)-[:HAS_CHAIN]->(c:PodiaChain)
WITH h, count(c) AS chain_count
WHERE chain_count > 1
RETURN h.hadith_id, h.hadith_indices, chain_count
ORDER BY chain_count DESC LIMIT 10

-- Full chain path traversal
MATCH path = (first:PodiaNarrator)-[:NARRATED*]->(last:PodiaNarrator)-[:TRANSMITTED_HADITH]->(h:PodiaHadith)
WHERE h.hadith_id = 'podia_3'
RETURN [n IN nodes(path) WHERE n:PodiaNarrator | n.full_name] AS chain, h.matn_text

-- Narrators rated ثقة
MATCH (n:PodiaNarrator)
WHERE n.rank_plain CONTAINS 'ثقة'
RETURN n.full_name_plain, n.rank_plain
ORDER BY n.full_name_plain

-- Full-text search on hadith body
CALL db.index.fulltext.queryNodes("podia_hadith_text_ft", "الأعمال بالنيات")
YIELD node
RETURN node.hadith_id, node.matn_text LIMIT 5

-- Find narrators with biographies from a specific source
MATCH (n:PodiaNarrator)
WHERE any(s IN n.tarajim_sources WHERE s CONTAINS 'ابن أبي حاتم')
RETURN n.full_name, n.tarajim_sources
```
"""


def write_schema_description(path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(SCHEMA_DESCRIPTION)
    print(f"  Schema description written to {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest Bukhari Podia data into Neo4j")
    _root = Path(__file__).parent.parent.parent
    _podia = Path(__file__).parent

    parser.add_argument("--hadith",
                        default=str(_root / "extract_data_v2" / "playwrite" / "bukhari_pedia_advanced_extraction_results.json"),
                        help="Path to advanced extraction JSON")
    parser.add_argument("--narrators",
                        default=str(_podia / "bukhari_podia_narrators.jsonl"),
                        help="Path to narrator profiles JSONL")
    parser.add_argument("--tarajem",
                        default=str(_podia / "narrators_tarajem.jsonl"),
                        help="Path to narrators tarajem JSONL")
    parser.add_argument("--neo4j-uri",
                        default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user",
                        default=os.environ.get("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password",
                        default=os.environ.get("NEO4J_PASSWORD", ""))
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print stats without writing to Neo4j")
    parser.add_argument("--schema-out",
                        default=str(_podia / "schema_description.md"),
                        help="Path to write schema description")
    args = parser.parse_args()

    # --- Load data ---
    print("Loading advanced hadith extraction...")
    records = load_advanced_hadiths(args.hadith)

    print("Loading narrator profiles...")
    narrators_data = load_narrators(args.narrators)

    print("Loading tarajem...")
    tarajem_data = load_tarajem(args.tarajem)

    # --- Collect stats ---
    rawi_ids: set[int] = set()
    total_chains = 0
    chain_type_counts: dict[str, int] = {}
    transmission_type_counts: dict[str, int] = {}
    for r in records:
        for chain in r.get("chains", []):
            total_chains += 1
            ct = chain.get("type", "primary")
            chain_type_counts[ct] = chain_type_counts.get(ct, 0) + 1
            for n in chain.get("narrators", []):
                rawi_ids.add(n["rawi_id"])
                tt = n.get("transmission_type") or "unknown"
                transmission_type_counts[tt] = transmission_type_counts.get(tt, 0) + 1

    books = {r["book_name"] for r in records if r.get("book_name")}
    chapters = {
        (r["book_name"], r["chapter"])
        for r in records if r.get("chapter")
    }
    multi_chain = sum(1 for r in records if len(r.get("chains", [])) > 1)

    print(f"\n--- Stats ---")
    print(f"  Hadiths:        {len(records)}")
    print(f"  Books:          {len(books)}")
    print(f"  Chapters:       {len(chapters)}")
    print(f"  Narrators:      {len(rawi_ids)} (in chains)")
    print(f"  Total chains:   {total_chains}")
    print(f"  Multi-chain:    {multi_chain} hadiths")
    print(f"  Chain types:    {chain_type_counts}")
    print(f"  Transmission:   {transmission_type_counts}")

    write_schema_description(args.schema_out)

    if args.dry_run:
        print("\nDry run complete. No data written to Neo4j.")
        return

    # --- Ingest ---
    if not _NEO4J_AVAILABLE:
        print("\nneo4j driver not installed. Run: pip install neo4j")
        sys.exit(1)

    driver = GraphDatabase.driver(
        args.neo4j_uri,
        auth=(args.neo4j_user, args.neo4j_password),
    )
    try:
        print("\nCreating constraints and indexes...")
        create_constraints(driver)

        print("Ingesting PodiaBook nodes...")
        n = ingest_books(driver, records, args.batch_size)
        print(f"  Done ({n} created/merged).")

        print("Ingesting PodiaChapter nodes + IN_BOOK...")
        n = ingest_chapters(driver, records, args.batch_size)
        print(f"  Done ({n} created/merged).")

        print("Ingesting PodiaHadith nodes + IN_CHAPTER...")
        n = ingest_hadiths(driver, records, args.batch_size)
        print(f"  Done ({n} created/merged).")

        print("Ingesting PodiaNarrator nodes...")
        n = ingest_narrators(driver, rawi_ids, narrators_data, tarajem_data, args.batch_size)
        print(f"  Done ({n} created/merged).")

        print("Ingesting chains (PodiaChain + HAS_CHAIN + POSITION + NARRATED + TRANSMITTED_HADITH)...")
        n = ingest_chains(driver, records, args.batch_size)
        print(f"  Done ({n} created).")

        # Verification
        print("\nVerification counts:")
        with driver.session() as session:
            for label in ("PodiaBook", "PodiaChapter", "PodiaHadith", "PodiaChain", "PodiaNarrator"):
                count = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
                print(f"  {label}: {count}")
            for rel in ("IN_BOOK", "IN_CHAPTER", "HAS_CHAIN", "POSITION", "NARRATED", "TRANSMITTED_HADITH"):
                count = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()["c"]
                print(f"  [{rel}]: {count}")

    finally:
        driver.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
