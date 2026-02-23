# Extract Data V2 - Advanced Hadith Chain Extraction with Context-Aware Disambiguation

## Overview

This directory contains the V2 pipeline for hadith chain extraction with **context-aware narrator disambiguation** based on علم الرجال (hadith narrator science). Uses student-teacher relationships to resolve ambiguous names like سفيان, هشام, يحيى to their full canonical forms.

**Status:** Phase 2 Complete (disambiguation), Phase 3 In Planning (Neo4j ingestion)

---

## Pipeline Architecture

```
┌─────────────────┐     ┌──────────────────────────────┐     ┌─────────────────────────────┐
│   CSV/JSON      │────▶│ advanced_extractions_llm_    │────▶│ *_results_advanced_with_    │
│ (raw hadith)    │     │ pydantic_with_matn.py        │     │ matn.json                   │
└─────────────────┘     │ (GPT-4o + Pydantic)          │     │ (structured chains + matn)  │
                        └──────────────────────────────┘     └──────────┬──────────────────┘
                                                                       │
                                                                       ▼
                                                        ┌────────────────────────────────┐
                                                        │ extract_ambiguous_context.py    │
                                                        │ (Extract ambiguous name-student │
                                                        │  pairs from chains → CSV)       │
                                                        └──────────┬─────────────────────┘
                                                                   │
                                                                   ▼
                                                        ┌────────────────────────────────┐
                                                        │ solve_ambiguity.py             │
                                                        │ (Rule engine: ~200 rules from  │
                                                        │  علم الرجال → context JSON)     │
                                                        └──────────┬─────────────────────┘
                                                                   │
                                                                   ▼
                                                        ┌────────────────────────────────┐
                                                        │ narrators_mapping.py           │
                                                        │ (3-step resolution pipeline)   │
                                                        │ 1. Context JSON lookup         │
                                                        │ 2. Live rule resolution        │
                                                        │ 3. Static mapping fallback     │
                                                        └──────────┬─────────────────────┘
                                                                   │
                                                                   ▼
                                                        ┌────────────────────────────────┐
                                                        │ *_Normalized_Ready_For_        │
                                                        │ Graph.json + nodes CSV         │
                                                        └──────────┬─────────────────────┘
                                                                   │
                                                                   ▼
                                                        ┌────────────────────────────────┐
                                                        │ ingest_v3.py (PLANNED)         │────▶ Neo4j
                                                        └────────────────────────────────┘
```

---

## Files

### Scripts

| File | Purpose | Input | Output |
|------|---------|-------|--------|
| `advanced_extractions_llm_pydantic_with_matn.py` | Extract narrator chains and matn using GPT-4o | Raw hadith JSON | `*_results_advanced_with_matn.json` |
| `extract_ambiguous_context.py` | Extract ambiguous name-student pairs | Extraction output | `ambiguous_narrators_for_llm.csv` |
| `solve_ambiguity.py` | Rule engine for context-aware disambiguation | CSV of ambiguous pairs | `resolved_context_mappings.json` |
| `narrators_mapping.py` | 3-step normalization pipeline with ID generation | Extraction output + context mappings | `*_Normalized_Ready_For_Graph.json` |
| `analyze_narrators.py` | Analyze narrator frequency distribution | Extraction output | `narrators_stats.csv` |

### Configuration

| File | Purpose |
|------|---------|
| `narrator_mappings.json` | Static name normalization dictionary (204 entries) |

### Data Files (Bukhari/)

| File | Description |
|------|-------------|
| `Bukhari_Without_Tashkel.json` | Raw input (7,563 hadiths) |
| `Bukhari_Without_Tashkel_results_advanced_with_matn.json` | Extracted chains (Step 1 output) |
| `Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.json` | Chains enriched with narrator IDs (7,008 hadiths, 93.99% coverage) |
| `ambiguous_narrators_for_llm.csv` | 1,555 ambiguous name-student pairs |
| `resolved_context_mappings.json` | 1,529 pre-computed context resolutions (98.3%) |
| `remaining_ambiguous_pairs.json` | 186 still-ambiguous pairs with candidates and difficulty |
| `Bukhari_Normalized_Ready_For_Graph.json` | Final normalized output with IDs |
| `narrators_nodes.csv` | Unique narrator nodes for Neo4j LOAD CSV |
| `unmapped_narrators_report.csv` | Unmapped narrator analysis |
| `narrator_hadith_names_bukhari.json` | narrator_id → list of name forms seen in Bukhari chains (1,366 IDs, no-tashkeel) |
| `covered_narrators.csv` | 1,366 resolved narrators — ID, canonical Shamela name, name variants, role, resolution method, sample hadith indices |
| `uncovered_narrators.csv` | 699 unresolved narrator names — name, role, total occurrences, sample hadith indices |

### Documentation

| File | Description |
|------|-------------|
| `DISAMBIGUATION_RESULTS.md` | Detailed disambiguation results, fixes, and remaining work |
| `NEO4J_INGESTION_PLAN.md` | Neo4j ingestion plan |

---

## Usage

### Step 1: Extract Chains

```bash
cd extract_data_v2
python advanced_extractions_llm_pydantic_with_matn.py
```

Uses GPT-4o with Pydantic structured outputs to extract narrator chains and matn segments.

### Step 2: Extract Ambiguous Contexts

```bash
python extract_ambiguous_context.py
```

Scans all chains and extracts every (ambiguous_name, student) pair with frequency counts. Output: `Bukhari/ambiguous_narrators_for_llm.csv` (1,555 pairs).

### Step 3: Generate Context Mappings

```bash
python solve_ambiguity.py
```

Applies ~200 hadith scholarship rules to resolve ambiguous pairs. Rules based on:
- **Unambiguous names** (260+ entries): عائشة → عائشة بنت أبي بكر
- **Student-based context** (14 ambiguous names): سفيان|الحميدي → سفيان بن عيينة
- **Pronoun resolution** (117+ father-son pairs): أبيه|هشام → عروة بن الزبير

Output: `Bukhari/resolved_context_mappings.json` (1,529 resolved, 98.3% coverage).

### Step 4: Normalize All Names

```bash
python narrators_mapping.py
```

3-step resolution with honest categorized counting:

| Step | Method | Description |
|------|--------|-------------|
| 1 | Context JSON lookup | Pre-computed from `resolved_context_mappings.json` |
| 1b | Live rule resolution | `resolve_ambiguous()` for pairs not in CSV |
| 2 | Static mapping | 204-entry dictionary fallback |
| 3 | Identity | Keep original name |

Statistics are separated into honest categories:

| Category | Mentions | % of Total | Description |
|----------|----------|-----------|-------------|
| Unambiguous | 13,894 | 31.1% | 1:1 lookups (e.g. عائشة, أنس) |
| Context-disambiguated | 2,718 | 6.1% | Student-based rules (e.g. سفيان → الثوري/ابن عيينة) |
| Pronoun-resolved | 781 | 1.7% | Kinship lookups (e.g. أبيه → father's name) |
| Static mapping | 16,167 | 36.1% | Dictionary fallback (204 entries) |
| **Total mapped** | **33,560** | **75.0%** | |
| Unmapped | 11,173 | 25.0% | See breakdown below |

### Unmapped Breakdown

| Type | Est. Mentions | % of Unmapped | Action |
|------|--------------|---------------|--------|
| Full canonical names (not in dictionary) | ~8,989 | 78% | Auto-add to NAME_MAPPING for ~95% coverage |
| Short ambiguous names | ~2,184 | 19% | Need more rules or LLM |

**Quick win:** Adding ~2,303 unique full canonical names to `narrator_mappings.json` would push coverage from 75% to ~95%.

---

## Disambiguation Rules

### How It Works

An ambiguous name like "سفيان" is resolved by checking **who narrated from him** (the student):

```
الحميدي ← سفيان ← ...   →  سفيان = سفيان بن عيينة  (الحميدي was ابن عيينة's student)
يحيى    ← سفيان ← ...   →  سفيان = سفيان الثوري    (يحيى القطان was الثوري's student)
```

### Key Disambiguations

| Name | Split Into | Method |
|------|-----------|--------|
| سفيان | الثوري / ابن عيينة | 7 students each branch |
| هشام | بن عروة / الدستوائي | Student-specific (يحيى is ambiguous - see below) |
| يحيى | القطان / بن بكير / الأنصاري | مسدد→القطان, البخاري→بن بكير |
| أبيه | 117+ father names | Father-son lookup table |
| عبد الله | ابن مسعود / ابن عمر / ابن المبارك / ابن عباس | Student-specific |

### Known Limitations

1. **هشام + يحيى**: يحيى القطان narrated from ALL THREE Hishams (بن عروة, بن حسان, الدستوائي). Cannot disambiguate with student context alone - marked as غامض.
2. **عبد الله + البخاري**: البخاري (born 194 AH) could not have narrated from ابن المبارك (died 181 AH). Marked as غامض instead of guessing.
3. **186 pairs** (923 mentions) remain ambiguous - see `Bukhari/remaining_ambiguous_pairs.json` for full list with candidates.

---

## Remaining Ambiguous Pairs

Full data in `Bukhari/remaining_ambiguous_pairs.json`. Summary:

| Category | Pairs | Mentions | Action |
|----------|-------|----------|--------|
| context_dependent | 160 | 820 | Add student/teacher rules or use LLM |
| kinship_pronoun | 18 | 76 | Add father/brother lookup pairs |
| data_error (عبة) | 6 | 19 | Fix extraction bug (شعبة) |
| unsolvable | 2 | 8 | غيره, رجل - cannot resolve |

Top ambiguous by frequency: هشام (258), يحيى (117), سفيان (75), إبراهيم (66), سعيد (64), علي (61).

### Resolution Approaches

1. **Teacher-context lookup**: look at narrator AFTER the ambiguous name (e.g. هشام → عروة = هشام بن عروة)
2. **More student rules**: research additional student-teacher pairs
3. **LLM-assisted**: use the CSV's empty column for AI disambiguation
4. **Fix data errors**: عبة → شعبة extraction bug

---

## ID Generation

**Format:** `NAR_<12-char-SHA256-hash>`

```python
hashlib.sha256(canonical_name.encode('utf-8')).hexdigest()[:12].upper()
# "سفيان الثوري" → NAR_A1B2C3D4E5F6
```

- Deterministic (same name = same ID)
- Collision-resistant (~1 in 16 trillion)
- Zero collisions observed across all narrators

---

## Development Status

**Current coverage: 75.0% (33,560/44,733 narrator mentions mapped)**

### Phase 1: Chain Extraction - COMPLETE
- [x] GPT-4o extraction with Pydantic
- [x] Matn segmentation
- [x] Chain structure validation

### Phase 2: Disambiguation - COMPLETE (core), ongoing (edge cases)
- [x] Static mapping dictionary (204 entries)
- [x] Context-aware disambiguation engine (~200 rules)
- [x] Pronoun resolution (117+ father-son pairs)
- [x] Honest statistics (separated unambiguous/context/pronoun/static)
- [x] Bug fixes (البخاري→ابن المبارك removed, هشام+يحيى fixed, stats inflation corrected)
- [x] Remaining ambiguous pairs cataloged (186 pairs, 923 mentions)
- [ ] Auto-add ~2,303 full canonical names to dictionary (→ ~95% coverage)
- [ ] Resolve remaining 186 ambiguous pairs (923 mentions)
- [ ] Fix عبة data extraction error (شعبة)

### Phase 3: Neo4j Ingestion (Planned)
- [ ] Create `ingest_v3.py`
- [ ] Implement v3 schema (Narrator, Hadith, Segment nodes)
- [ ] Batch ingestion with progress tracking

### Phase 4: Enrichment (Future)
- [ ] Biographical data (birth/death, generation)
- [ ] Scholarly assessments (reliability ratings)
- [ ] Vector embeddings for semantic search

---

## Troubleshooting

### "No module named 'normalization'"

Already handled - `narrators_mapping.py` adds parent directory to `sys.path`.

### High unmapped count

Expected. Remaining unmapped names are either:
- Already-canonical full names not in the static dictionary (will work fine in graph)
- Genuinely ambiguous names marked as غامض
- Low-frequency narrators (<30 mentions)

### Adding new disambiguation rules

1. Check `Bukhari/remaining_ambiguous_pairs.json` for high-frequency pairs
2. Research the student-teacher relationship in hadith sources
3. Add rule to `solve_ambiguity.py` in the appropriate section
4. Re-run: `python solve_ambiguity.py && python narrators_mapping.py`
5. Verify coverage improved and no regressions

---

## References

- [DISAMBIGUATION_RESULTS.md](DISAMBIGUATION_RESULTS.md) - Detailed results and Arabic explanation
- [NEO4J_INGESTION_PLAN.md](NEO4J_INGESTION_PLAN.md) - Neo4j ingestion plan
- [../normalization.py](../normalization.py) - Arabic text normalization utilities

---

**Last Updated:** 2026-02-22
