# Narrator ID Mapping: Bukhari LLM Chains → Shamela IDs

This document explains the full pipeline used to attach Shamela narrator IDs to the
LLM-extracted Bukhari hadith chains, including the problems encountered and how each
one was solved.

---

## The Problem

We have two datasets that need to be joined on narrator identity:

| Dataset | Source | Format | Tashkeel? | Has IDs? |
|---------|--------|--------|-----------|----------|
| `Bukhari_Without_Tashkel_results_advanced_with_matn.json` | LLM extraction (GPT-4) | JSON | No | No |
| `narrator_hadith_names.json` | Shamela scrape | JSON | Yes | Yes (1,525 narrators) |

The Bukhari file has 44,733 narrator mentions across 7,008 hadiths, with names written
**without tashkeel** (no diacritics). The Shamela lookup has name variants **with tashkeel**.
They cannot be joined directly — the same name looks completely different in each file.

---

## Step 1 — Normalization (the foundation of everything)

Before any matching, both sides are normalized to the same base form. This is done
identically in both scripts so the comparison is always apples-to-apples.

```python
def normalize(name: str) -> str:
    # 1. Strip tashkeel (Unicode diacritic ranges)
    name = _TASHKEEL_RE.sub("", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    # 2. Remove parenthetical content: (١), (2), (text)
    name = _PARENS_RE.sub("", name)
    # 3. Remove punctuation: . ، ; : ? ! « » - _ / \
    name = _PUNCT_RE.sub("", name)
    # 4. Unify hamza variants → bare alef
    name = name.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    name = name.replace("ئ", "ي").replace("ؤ", "و")
    # 5. Ta marbuta (ة) → ha (ه)
    name = name.replace("ة", "ه")
    # 6. Alef maqsura (ى) → ya (ي)
    name = name.replace("ى", "ي")
    # 7. Strip leading waw connector (و)
    name = re.sub(r"^و\s*", "", name)
    # 8. Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name
```

**Why each step matters:**

| Step | Example Before | Example After | Why |
|------|---------------|---------------|-----|
| Strip tashkeel | `عَائِشَةَ` | `عائشة` | LLM output has no diacritics; lookup does |
| Remove parens | `الزهري (١)` | `الزهري` | LLM sometimes adds sequence numbers |
| Hamza unification | `أحمد / إبراهيم / آدم` | `احمد / ابراهيم / ادم` | Arabic orthography variation |
| Ta marbuta | `عائشة` | `عايشه` (after hamza+ta) | Written inconsistently at word ends |
| Alef maqsura | `يحيى` | `يحيي` | `ى` and `ي` are confused in many texts |
| Leading waw | `وعائشة` | `عائشة` | Coordinating conjunction not part of name |

---

## Phase 1 — Exact Match (`enrich_narrator_ids.py`)

**Script:** `extract_data_v2/firecrawl/enrich_narrator_ids.py`

**Logic:**
1. Load `narrator_hadith_names.json` — a dict of `narrator_id → [name variants]`
2. Normalize every name variant and build a lookup: `normalized_name → narrator_id`
3. Detect **collisions**: when the same normalized form maps to 2+ different IDs
   (e.g. `عايشه` appears under both ID 3026 and ID 3027) — these go into a
   `collision_set` and are left as `null` for later resolution
4. For each narrator in the Bukhari file: normalize the name, look it up

**Result after Phase 1:**

| Status | Count | % |
|--------|-------|---|
| Matched (exact) | 30,502 | 68.2% |
| Ambiguous (collision) | 13,031 | 29.1% |
| Unmatched (not in lookup) | 1,200 | 2.7% |

**Output:** `Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json`
Each narrator object gets `narrator_id` (the ID or `null`) and
`narrator_id_ambiguous: true` if it hit a collision.

---

## Phase 2 — 3-Pass Resolution (`resolve_remaining_narrators.py`)

**Script:** `extract_data_v2/firecrawl/resolve_remaining_narrators.py`

Reads the Phase 1 output and attempts to resolve all remaining `null` entries.
Only touches narrators with `narrator_id = null`. Already-resolved narrators get
tagged `narrator_id_resolution: "exact_match"` and are skipped.

---

### Pass 1 — Shamela JSONL Cross-Reference

**Problem it solves:** Some narrator names in the Bukhari chains appear in the Shamela
scraped data (`shamela_book_1681.jsonl`) but were **not included** in
`narrator_hadith_names.json` (the 1,525-narrator lookup). The JSONL has narrator IDs
attached directly to every mention.

**Logic:**
1. Parse `shamela_book_1681.jsonl` — each line is a page with hadith blocks and
   narrator objects, each already having a `narrator_id` from Shamela
2. Normalize every narrator name from the JSONL → build lookup:
   `normalized_name → id` (only when unambiguous — same collision detection as Phase 1)
3. For each still-null narrator: normalize its name, look it up in the Shamela lookup

**Result:** In the final pipeline this pass resolves 0 additional mentions on top of
Phase 1, because the normalization improvements (hamza, ta marbuta, ya) made Phase 1
already cover everything the Shamela JSONL would have added. The infrastructure remains
in the script for future use if the lookup changes.

---

### Pass 2 — Context Mappings

**Problem it solves:** Many common names like `سفيان`, `هشام`, `عائشة` appear as
multiple distinct narrators in Shamela (different IDs). Normalized, they are identical
— so Phase 1 leaves them as `null` (collision). But in a hadith chain, the **identity
of the next narrator** (the student — the one narrating from this person) usually
makes the identity unambiguous.

**Source file:** `extract_data_v2/Bukhari/resolved_context_mappings.json`
Contains 1,529 rules, each structured as:

```json
{
  "context_mappings": {
    "عائشة|عروة بن الزبير": "عائشة بنت أبي بكر",
    "سفيان|الحميدي":        "سفيان بن عيينة",
    "سفيان|ابن المبارك":    "سفيان الثوري"
  }
}
```

Key format: `"ambiguous_name|student_name"`
- **ambiguous_name** = the name as written in the Bukhari chain (raw, not normalized)
- **student_name** = the next narrator in the chain toward the collector (البخاري),
  i.e. `narrators[idx - 1]` — the person who narrated FROM this narrator

**Logic:**
1. For each still-null narrator at index `idx`, build the key:
   `f"{raw_name}|{narrators[idx-1]['name']}"`
2. If the key exists in context_mappings → get the canonical full name
3. Resolve the canonical name to an ID via `lookup_canonical()`

**`lookup_canonical()` — the canonical-to-ID resolver:**

This function takes a canonical name like `"عائشة بنت أبي بكر"` and tries to find
its ID. It uses a **progressive prefix fallback** plus **collision disambiguation**:

```
Full normalized match → "عايشه بنت ابي بكر"  → not in lookup
Drop last word        → "عايشه بنت ابي"       → not in lookup
Drop last word        → "عايشه بنت"            → not in lookup
Drop last word        → "عايشه"                → in collision_set (IDs 3026 + 3027)
  → try disambiguation:
      ID 3027 has variant "عايشه بنت سعد":
        starts_with("عايشه") = True
        full_norm.startswith("عايشه بنت سعد") = False  ← CONFLICT → eliminate ID 3027
      ID 3026 has no conflicting longer variant → only non-conflicting ID
  → assign ID 3026 ✓
```

**Disambiguation rule:** A longer variant `v` of ID `X` **conflicts** with the
canonical if:
- `v` starts with the collision prefix (it's an extension of the same short name)
- AND the canonical does NOT start with `v` (they diverge in different directions)

If exactly one ID has no conflicting longer variant → that ID is the answer.

**Result:** +989 narrator mentions resolved.

---

### Pass 3 — Name Mapping Fallback

**Problem it solves:** Some names have no context rule (no student in chain, or the
context rule wasn't written), but they consistently refer to one specific narrator
when used without further qualification.

**Source file:** `extract_data_v2/narrator_mappings.json`
Contains 204 simple mappings: `short_name → canonical_full_name`

```json
{
  "mappings": {
    "سفيان": "سفيان الثوري",
    "الزهري": "محمد بن مسلم بن شهاب الزهري"
  }
}
```

**Logic:**
1. If a still-null narrator's raw name is a key in name_mappings
2. Get the canonical full name
3. Resolve it to an ID via `lookup_canonical()` (same function as Pass 2)
4. Only assigns if the canonical resolves to exactly one unambiguous ID

**Result:** +31 narrator mentions resolved.

---

### Pass 4 — Matn-Based Chain Matching

**Problem it solves:** Some narrators can't be resolved by name at all — the name is too
ambiguous, too rare, or too generic for any lookup to work. But the hadith itself is
unique: matching the hadith's matn (statement) against the Shamela JSONL gives us the
exact Shamela chain, which has IDs for every narrator.

**Key insight:** Both sources contain the same physical hadiths. Shamela stores them
verbatim (with tashkeel and punctuation); the Bukhari file stores them as LLM output
(no tashkeel, no punctuation). Normalizing both sides identically makes them comparable.

**Logic:**
1. Build a Shamela matn index: `normalize(matn or full_text)[:100] → [{narrator_ids, narrator_map, page}]`
2. For each Bukhari hadith that still has null narrators:
   - Normalize `matn_segments[0][:100]` as the lookup key
   - Look up in the Shamela index
   - If **exactly one** Shamela block matches:
     - Collect IDs already resolved in this Bukhari hadith
     - `extra_ids = shamela_ids − resolved_ids`
     - If `len(extra_ids) == len(null_slots)` → assign positionally (chain order is preserved)
   - Otherwise: skip (ambiguous or mismatched counts)

**Verified example:** Hadith 4742, matn `"يا رسول الله إن البكر تستحي قال رضاها صمتها"`:
- Matches Shamela page 7654 (unique)
- Shamela has narrator `أبي عمرو مولى عائشة` with ID **1907**
- Bukhari chain has that slot as `null` → assigned **1907** ✓

**Why positional assignment is safe:** The hadith chain order is fixed (collector first,
Companion last) in both sources. Both Shamela's HTML scraper and the LLM extractor
preserve that order faithfully.

**Limitation:** 73.9% of remaining nulls have no Shamela matn match (LLM paraphrased the
text, or the hadith appears only in Bukhari's specific form). This pass only fires when the
texts align perfectly after normalization.

**Result:** +379 narrator mentions resolved.

---

## Final Results

| Resolution method | Mentions | % of total |
|-------------------|---------|------------|
| Phase 1 exact match | 41,665 | 93.1% |
| Pass 4 matn chain match | 379 | 0.8% |
| Pass 2 context mapping | 0* | — |
| Pass 3 name mapping | 0* | — |
| **Total resolved** | **42,044** | **94.0%** |
| Still null | 2,689 | 6.0% |

\* Passes 2 and 3 fire during the initial run to resolve ambiguous names;
their gains are already included in the Phase 1 exact match count above
(Phase 1 output = 93.1% after all name-based passes ran together).

Each narrator object in the output has:

```json
{
  "name": "أبو عمرو مولى عائشة",
  "narrator_id": "1907",
  "narrator_id_resolution": "matn_chain_match"
}
```

Possible `narrator_id_resolution` values:

| Value | Meaning |
|-------|---------|
| `"exact_match"` | Resolved in Phase 1 (normalized name → unique ID) |
| `"context_mapping"` | Resolved by `name\|student` context rule |
| `"name_mapping"` | Resolved by short-name → canonical fallback rule |
| `"matn_chain_match"` | Resolved by matching the hadith matn to Shamela chain |
| `null` | Still unresolved |

---

## Why 6.0% Remains Null

The remaining 2,689 unresolved mentions (651 unique names) fall into categories that
cannot be resolved automatically:

| Category | Examples | Why unresolvable |
|----------|---------|-----------------|
| Relational pronouns | `أبيه`, `أبوه`, `أبي`, `جده`, `أمه` | No fixed referent — means "his father", changes per hadith |
| Generic references | `رجل من قريش`, `بعض أصحاب النبي`, `فلان` | Intentionally anonymous in the text |
| Highly ambiguous | `عبد الله بن عمر` (225×), `علي` (118×), `عبد الله` (105×) | Multiple narrators share this name, no context rule |
| Matn mismatch | LLM paraphrased the text | Shamela matn ≠ Bukhari matn after normalization |
| Very rare names | Long-tail names appearing 1–3 times | Not worth writing a manual rule for |

These require either manual annotation or a more powerful disambiguation method
(e.g. embedding-based similarity, or full chain comparison against Shamela chains).

---

## Data Flow Diagram

```
narrator_hadith_names.json          Bukhari_...with_matn.json         shamela_book_1681.jsonl
(1,525 IDs, tashkeel names)         (44,733 narrator mentions,        (7,236 hadiths, IDs attached,
         │                           no tashkeel, no IDs)              matn with tashkeel+punctuation)
         │                                    │                                   │
         │                                    │                                   │
         └──────────── normalize() ───────────┘              normalize() (matn) ─┘
                            │                                          │
                    enrich_narrator_ids.py                 matn index (key→narrators)
                            │                                          │
                    ┌───────┴────────┐                                 │
                    │                │                                 │
               exact match       collision / unmatched                │
               (68.2%)           (31.8%)                              │
                    │                │                                 │
                    └───────┬────────┘                                 │
                            │                                          │
                resolve_remaining_narrators.py ◄────────────────────────┘
                            │
              ┌─────────────┼──────────────┬──────────────┐
              │             │              │              │
           Pass 1        Pass 2         Pass 3         Pass 4
        Shamela JSONL   Context       Name mapping   Matn chain
        cross-ref       mappings      fallback       matching
        (0 new)         (+2.2%)       (+0.1%)        (+0.8%)
              │             │              │              │
              └─────────────┴──────────────┴──────────────┘
                            │
                     94.0% resolved
                      6.0% still null
                    (relational / generic /
                     ambiguous / matn mismatch)
```

---

## Files Reference

| File | Role |
|------|------|
| `extract_data_v2/firecrawl/enrich_narrator_ids.py` | Phase 1: exact name match |
| `extract_data_v2/firecrawl/resolve_remaining_narrators.py` | Phase 2: 4-pass resolution (name + matn) |
| `extract_data_v2/firecrawl/narrator_hadith_names.json` | ID → name variants lookup (1,525 narrators) |
| `extract_data_v2/firecrawl/shamela_book_1681.jsonl` | Shamela scrape with IDs attached (ground truth, also used for matn index) |
| `extract_data_v2/Bukhari/resolved_context_mappings.json` | 1,529 `name\|student` → canonical rules |
| `extract_data_v2/narrator_mappings.json` | 204 short-name → canonical fallback rules |
| `extract_data_v2/Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn.json` | Input: LLM-extracted chains (no IDs) |
| `extract_data_v2/Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json` | Output: same chains with `narrator_id` injected |
