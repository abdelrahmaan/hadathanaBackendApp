# Narrator ID Matching — Playwrite Pipeline

Three-stage pipeline that enriches every narrator in Bukhari hadith chains with:
- `podia_rawi_id` — narrator ID from the Podia (dorar.net) database
- `tarajm_candidates` — array of up to 3 matching person IDs from tarajm.com

---

## Pipeline Overview

```
playwrite_bukhari_hadith_podia.jsonl  ──► [0] clean_narrators.py
                                                      │
                                                      ▼
                                    playwrite_bukhari_hadith_podia_clean.jsonl
                                                 │            │
                    Bukhari JSON ────────────────┘            │
                                 ▼                            ▼
                    [1] match_narrators.py     [2] match_podia_to_tarajm.py
                                 │                            │
                                 ▼                            ▼
                         with_ids.jsonl         podia_with_tarajm.jsonl
                                 │                            │
                                 └──────────────┬─────────────┘
                                                ▼
                                [3] enrich_bukhari_with_tarajm.py
                                                │
                                                ▼
                                 with_ids.jsonl  (updated in-place)
                                 unmatched_bukhari_narrators.jsonl
```

---

## Files

| File | Role |
|------|------|
| `clean_narrators.py` | **Stage 0** — clean raw Podia JSONL (remove tashkeel, punctuation, etc.) |
| `match_narrators.py` | **Stage 1** — match Bukhari narrator names → `podia_rawi_id` |
| `match_podia_to_tarajm.py` | **Stage 2** — match Podia narrators → `tarajm_candidates` |
| `enrich_bukhari_with_tarajm.py` | **Stage 3** — merge both lookups into the Bukhari JSONL |
| `run_pipeline.py` | **Run all stages in order** (use this for re-runs) |
| `playwrite_bukhari_hadith_podia.jsonl` | Raw Podia narrator records (scraped, with tashkeel) |
| `playwrite_bukhari_hadith_podia_clean.jsonl` | Cleaned Podia records — Stage 0 output, input to Stages 1 & 2 |
| `playwrite_bukhari_hadith_podia_with_tarajm.jsonl` | Podia records + `tarajm_candidates` — Stage 2 output |
| `Bukhari_Without_Tashkel_results_advanced_with_matn_with_ids.jsonl` | **Final output** — 7,008 hadiths, narrators fully enriched |
| `unmatched_bukhari_narrators.jsonl` | Narrator names where `podia_rawi_id` is null — for manual review |
| `unmatched_narrators.jsonl` | Stage 1 intermediate — unique names not found in Podia |
| `unmatched_podia_tarajm.jsonl` | Stage 2 intermediate — Podia narrators not found in tarajm |

Input sources:
```
playwrite_bukhari_hadith_podia.jsonl                              (raw Podia scrape)
../Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn.json
../../tarajm/out_people_csv/tarajm_people.csv
```

---

## Final Output Format

Each narrator object in the Bukhari JSONL:
```json
{
  "name": "عبد الله بن الزبير",
  "attributes": {"role": "narrator"},
  "podia_rawi_id": 729,
  "tarajm_candidates": [
    {"tarajm_id": 11407, "tarajm_name": "عبد الله بن الزبير بن عيسى الحميدي أبي بكر المكي", "match_type": "high_confidence", "score": 0.636},
    {"tarajm_id": 18559, "tarajm_name": "عبد الله بن الزبير بن العوام أبي بكر القرشي الأسدي", "match_type": "high_confidence", "score": 0.5}
  ]
}
```

When `podia_rawi_id` is null, `tarajm_candidates` is an empty array `[]`.

Unmatched file for manual review:
```json
{"name": "أبو الأشهب", "hadith_count": 2, "hadith_indices": [4481, 6617]}
```

---

## How Each Stage Works

### Stage 0 — `clean_narrators.py`
Reads `playwrite_bukhari_hadith_podia.jsonl` (raw scrape) and writes `playwrite_bukhari_hadith_podia_clean.jsonl`:
- Remove tashkeel (harakat)
- Remove parenthesized numbers like `(1)`, `(2)`
- Remove punctuation: `,` `.` `♠` etc.
- Remove underscore wrappers used for italics
- Normalize whitespace

### Stage 1 — `match_narrators.py`
Matches each narrator `name` from Bukhari chains to a Podia record:
1. **Exact** on `name_in_chain`
2. **Exact** on `full_name`
3. **Substring** — name inside `name_in_chain`/`full_name` or vice versa
4. **Token Jaccard ≥ 0.5** — word-overlap score
5. `null` if nothing found

Results cached per unique name. Output: `with_ids.jsonl` (narrator field `rawi_id`).

### Stage 2 — `match_podia_to_tarajm.py`
Matches each Podia narrator's `full_name` / `name_in_chain` to tarajm people:
1. **Exact** normalized name match
2. **Summary substring** — full_name appears inside tarajm `summary`
3. **Token Jaccard ≥ 0.25** — top-3 candidates, deduplicated by ID

Tags: `exact` | `high_confidence` (≥0.5) | `ambiguous` | `low_confidence` (0.25–0.5) | `summary_match`

### Stage 3 — `enrich_bukhari_with_tarajm.py`
Merges Stage 1 + Stage 2:
- Renames `rawi_id` → `podia_rawi_id`
- Adds `tarajm_candidates` array (looked up via `podia_rawi_id`)
- Updates `with_ids.jsonl` in-place
- Writes `unmatched_bukhari_narrators.jsonl`

---

## Results

### Stage 1 (Podia matching)
| Metric | Value |
|--------|-------|
| Hadiths | 7,008 |
| Total narrator mentions | 44,733 |
| Matched (`podia_rawi_id` set) | 44,055 (98.5%) |
| Unmatched (unique names) | 226 |

### Stage 2 (Tarajm matching)
| Metric | Value |
|--------|-------|
| Podia narrators | 1,017 |
| Matched to tarajm | 992 (97.5%) |
| Unmatched | 25 |

| Match type | Count |
|---|---|
| `high_confidence` | 568 |
| `low_confidence` | 277 |
| `ambiguous` | 66 |
| `exact` | 48 |
| `summary_match` | 33 |

---

## How to Run

```bash
cd extract_data_v2/playwrite
```

### Full pipeline — Podia JSONL updated (new scrape)
```bash
python3 run_pipeline.py          # runs Stage 0 → 1 → 2 → 3
```

### tarajm_people.csv updated (new tarajm scrape)
```bash
python3 run_pipeline.py --stage 2   # runs Stage 2 → 3
```

### Bukhari JSON updated
```bash
python3 run_pipeline.py --stage 1   # runs Stage 1 → 2 → 3
```

### Individual scripts
```bash
python3 clean_narrators.py            # Stage 0 — clean raw Podia JSONL
python3 match_narrators.py            # Stage 1 — Bukhari → Podia
python3 match_podia_to_tarajm.py      # Stage 2 — Podia → Tarajm
python3 enrich_bukhari_with_tarajm.py # Stage 3 — merge into final JSONL
```

Requires Python 3.10+. No external dependencies — stdlib only.
