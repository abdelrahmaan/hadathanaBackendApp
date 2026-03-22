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
