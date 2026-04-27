# Keyword Taxonomy Engine · primer

Updated: 2026-04-26 5:30pm EDT
Repo: https://github.com/Gabicus/keyword-taxonomy-engine

## Current status

**Phase 3 — Expanding taxonomy + locking down keywords.** 7 pillars + 5 new downloaded (not yet parsed). 424K senses, 2.58M relationships, 14 disciplines, 97 lenses. Validation suite complete (A+ scorecard). Citation network + 7 interactive visualizations built. **Priority: finish keyword taxonomy before exploring more DOI/citation work.**

## What this is

Universal scientific keyword taxonomy engine. Ingests authoritative keyword hierarchies from multiple global sources ("pillars"), normalizes into unified schema, stores in DuckDB. Multi-perspective ontology with "vector bundle" keyword senses — same keyword carries different meaning depending on origin, domain context, and who's asking.

End goal: multi-modal analysis tool (VOSviewer x1000) with DOE/NETL/fossil energy at center.

## Key Commands
```bash
python -m src.cli lens fossil_energy --search "carbon"
python -m src.cli lens-explore "combustion" --discipline materials
python -m src.cli lens-compare "methane" --lenses hat:fossil_energy:researcher hat:earth_environmental:director
python -m src.cli lens-papers fossil_energy --search "carbon"
python -m src.cli lens-list --role researcher
python -m src.cli search "climate"
python -m pytest tests/ -v              # 178 tests passing
python3 -u scripts/validation_suite.py  # run full validation suite
```

## Data lake (7 pillars ingested)

| Source | Unified | Raw | Notes |
|---|---|---|---|
| OpenAlex | 31,995 | 4,798 | 4 domains → 27K keywords |
| MeSH (NIH) | 31,110 | 31,110 | 16 categories, 97% definitions |
| Library of Congress | 29,731 | 29,731 | Bulk SKOS N-Triples |
| NASA GCMD | 4,849 | 4,849 | 6 keyword types |
| UNESCO | 4,408 | 4,408 | English filter |
| NCBI | 3,044 | 3,044 | Capped at Order rank |
| DOE OSTI | 59 | 59 | 45 categories + 9 groups |
| **Total** | **105,196** | **77,999** | |

## Ontology layer

| Table | Count | Notes |
|---|---|---|
| keyword_senses | 423,782 | 105K base + 263K natlab WoS + 14K NETL WoS + 3.6K vocab + 1.9K OpenAlex pub + 1K meta |
| sense_relationships | 2,581,302 | 6.09 rels/sense |
| co_occurrence edges | 137,117 | OpenAlex pub keyword pairs (73% cross-discipline) |
| disciplines | 14 | 4 resolution tiers |
| ontology_lenses | 97 | 42 primary + 54 intersection + 1 baseline |
| orphan senses | 142 (0.03%) | Down from 93% via ML + AI reasoning |
| semantic embeddings | 63,434 × 384 dim | all-MiniLM-L6-v2 |
| T1 sub-ontology | 18 subcategories | carbon_capture, combustion, gasification, etc. |
| polysemous labels | 14,206 (3.5%) | Terms in 2+ sources |

## Quality scorecard: 97.8/100 (A+)

## Validation suite (complete)

| Metric | Value |
|---|---|
| Gold-standard sample | 343 items (6 AI strata) |
| Sensitivity (Jaccard) | 0.863 |
| Coherence (NPMI) | 0.762 |
| Lens divergence (Spearman) | 0.454 |
| Citation coherence | 17× keyword overlap (p≈0) |

5 figures in `figures/validation/`

## Visualizations built (session 7b-c)

| Viz | File | Status | Notes |
|---|---|---|---|
| Citation tree (2D) | `scripts/citation_tree_viz.html` | ✓ Working | 4-level organic tree, warm/cool colors, clickable DOIs, year-bands |
| Citation tree (3D) | `scripts/citation_tree_3d.html` | ✓ Working | Cylindrical domain-ring layout, per-order toggles, click-to-pin tooltip |
| 3D Nebula | `scripts/nebula_viz.html` | ✓ Shell ready | Three.js 63K points + 97 lens dots, needs UMAP refresh |
| Poincaré disk | `scripts/poincare_viz.html` | ✓ Working | Hyperbolic layout, gold cross-disc arcs, Möbius zoom, search |
| Chord diagram | `scripts/chord_viz.html` | ✓ Working | D3 circular, oversimplified — replace with HEB later |
| HEB | Not built | Planned | THE "see everything" diagram with ghost arcs |
| Coverage est. | `scripts/estimate_coverage.py` | Data ready | Our data vs OpenAlex universe |

User loved: citation tree 2D (organic roots, warm/cool palette), 3D domain-ring concept.
User feedback: chord too simple → replace with HEB. Poincaré missing some cross-disc edges.

## Next up — PRIORITY ORDER

### A. KEYWORD TAXONOMY (DO FIRST)
1. [ ] **Write parsers for 5 downloaded taxonomies**:
   - AGROvoc (~40K, SKOS N-Triples, `data/raw/agrovoc/agrovoc_lod.nt.zip`)
   - ERIC (~12K, XML, `data/raw/eric/ERICThesaurus2025.xml`)
   - IEEE (~10K, CSV, `data/raw/ieee_thesaurus/ieee-thesaurus_2023.csv`)
   - STW (~6K, N-Triples, `data/raw/stw/stw.nt`)
   - EuroVoc (~7K, CSV, `data/raw/eurovoc/eurovoc_concepts.csv`)
   - Reuse SKOS/N-Triples patterns from LoC parser
2. [ ] **Ingest new taxonomies** into DuckDB + ontology layer (unified + raw tables)
3. [ ] **Discipline assignment** for new senses (don't leave as general_science)
4. [ ] **Cross-taxonomy alignment** — run embedding-based matching for new pillars
5. [ ] **Orphan check** — ensure new senses get relationships
6. [ ] **Update quality scorecard** — source coverage should jump to 12 pillars
7. [ ] **Re-run validation suite** with expanded taxonomy
8. [ ] **Regenerate semantic embeddings** — include new taxonomy keywords

### B. COVERAGE & COMPLETENESS
9. [ ] **Complete WoS category list** — all 254 subjects (have 201), identify 53 gaps
10. [ ] **Run coverage estimation** vs OpenAlex universe
11. [ ] **Investigate FAST** — got 403, may need registration for bulk download

### C. VISUALIZATIONS (CIRCLE BACK)
12. [ ] **HEB (Hierarchical Edge Bundling)** — concentric rings + ghost arcs for gaps
13. [ ] **Fix Poincaré** — natural gas ↔ chemical engineering gap
14. [ ] **Regenerate viz data** — chord, Poincaré, UMAP with expanded taxonomy
15. [ ] **Polish 3D citation tree** — more filters, analytical controls per user feedback

### D. DOI/CITATION (AFTER KEYWORDS LOCKED)
16. [~] **WoS DOI→OpenAlex lookup** — PAUSED at batch 300/3959 (14,841 bridges, 596K citations). Resume: `python3 -u scripts/lookup_wos_dois_in_openalex.py`
17. [ ] **lens-citations CLI** — bidirectional citation tree traversal command
18. [ ] **Add metadata to openalex_citations** — title, year, DOI columns

### E. LONG-TERM
19. [ ] **Methodology paper** — vector bundle semantics, cross-walk, validation
20. [ ] **Benchmark vs VOSviewer/CiteSpace**
21. [ ] **API/query interface** — REST or GraphQL
22. [ ] **Paywalled taxonomies** — GeoRef ($), Inspec ($200-500), INIS (web-only)

## Downloaded taxonomy data (not yet ingested)

| Taxonomy | Terms | Format | File | Size |
|---|---|---|---|---|
| AGROvoc | ~40K | SKOS N-Triples | `data/raw/agrovoc/agrovoc_lod.nt.zip` | 90MB |
| ERIC | ~12K | XML | `data/raw/eric/ERICThesaurus2025.xml` | 9MB |
| IEEE | ~10K | CSV | `data/raw/ieee_thesaurus/ieee-thesaurus_2023.csv` | 1.4MB |
| STW | ~6K | N-Triples | `data/raw/stw/stw.nt` | 15MB |
| EuroVoc | ~7K | CSV | `data/raw/eurovoc/eurovoc_concepts.csv` | 400KB |

## Year anomaly note (2026-04-26)

15 papers in citation tree have dates AFTER papers that cite them. Cause: OpenAlex assigns current edition dates to books (e.g., "Binary Alloy Phase Diagrams" 2016 edition cited by 2013 paper). Not actionable yet but noted.

## Performance pitfalls (AVOID)

- **NOT EXISTS on sense_relationships = infinite CPU.** Use LEFT JOIN with CTE.
- **DuckDB executemany with VARCHAR[] = pathologically slow.** Use temp tables.
- **DuckDB `?` param binding = SQL text order, not definition order.**
- **`python3 -u`** for unbuffered output in background scripts.
- **Kill stale background processes** before launching new DB queries.
- **Always use `duckdb` CLI** (v1.5.2) instead of Python wrappers for queries.

## Don't forget

- ALWAYS be devil's advocate — challenge assumptions, no glass castles
- Audit between large steps for errors and gaps
- Push before destructive operations (always)
- Keywords are vector bundles — same word, different meaning per context
- New senses need discipline assignment (don't leave as general_science)
- Gold-standard sample needs 2 human annotators
- Cross-domain references are FIRST CLASS
- **Lock down keywords BEFORE more DOI/citation exploration**
