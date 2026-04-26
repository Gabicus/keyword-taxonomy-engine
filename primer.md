# Keyword Taxonomy Engine · primer

Updated: 2026-04-26 12:00pm EDT
Repo: https://github.com/Gabicus/keyword-taxonomy-engine

## Current status

**Phase 3 ACTIVE — Paper discovery + citation network + validation suite.** 7 pillars + 230K WoS pubs + 8K OpenAlex pubs. 423,782 senses, 2,581,302 relationships (6.09 rels/sense). 0.03% orphans. 5 lens query CLI commands. Citation network fetching (~400K edges). Validation suite with 4 figures. Quality scorecard 97.8/100 (A+). 178 tests passing.

## What this is

Universal scientific keyword taxonomy engine. Ingests authoritative keyword hierarchies from 7 global sources ("pillars"), normalizes into unified schema, stores in DuckDB. Multi-perspective ontology with "vector bundle" keyword senses — same keyword carries different meaning depending on origin, domain context, and who's asking.

End goal: multi-modal analysis tool (VOSviewer x1000) with DOE/NETL/fossil energy at center. Users stand around a sphere looking in through composed lenses (Role × Org × Discipline × Interest).

## Key Commands
```bash
python -m src.cli lens fossil_energy --search "carbon"    # query through lens
python -m src.cli lens-explore "combustion" --discipline materials  # explore relationships
python -m src.cli lens-compare "methane" --lenses hat:fossil_energy:researcher hat:earth_environmental:director
python -m src.cli lens-papers fossil_energy --search "carbon"  # NEW: find papers through lens
python -m src.cli lens-list --role researcher  # list 97 available lenses
python -m src.cli search "climate"      # basic keyword search
python -m pytest tests/ -v              # 178 tests passing
python3 -u scripts/validation_suite.py  # run full validation suite
python3 -u scripts/citation_coherence.py  # citation coherence analysis
```

## Data lake

| Source | Unified | Raw | Notes |
|---|---|---|---|
| OpenAlex | 31,995 | 4,798 | 4 domains → 27K keywords. works_count ranking signal |
| MeSH (NIH) | 31,110 | 31,110 | 16 categories, 97% definitions, avg 7.6 synonyms |
| Library of Congress | 29,731 | 29,731 | Science + Technology subtrees, bulk SKOS |
| NASA GCMD | 4,849 | 4,849 | 6 keyword types, 80% enriched |
| UNESCO Thesaurus | 4,408 | 4,408 | English filter |
| NCBI Taxonomy | 3,044 | 3,044 | Capped at Order rank |
| DOE OSTI | 59 | 59 | 45 categories + 9 groups |
| **Total** | **105,196** | **77,999** | |

## Ontology layer

| Table | Count | Notes |
|---|---|---|
| keyword_senses | 423,782 | 105K base + 263K natlab WoS + 14K NETL WoS + 3.6K vocab + 1.9K OpenAlex pub + 1K meta |
| sense_relationships | 2,581,302 | 6.09 rels/sense. subtopic_of + related_to + co_occurrence + bridges + equivalent + ai_reasoning |
| co_occurrence edges | 137,117 | NEW: from OpenAlex pub keyword pairs (73% cross-discipline) |
| disciplines | 14 | 4 resolution tiers |
| hierarchy_envelopes | 107 | NETL org structure |
| ontology_lenses | 97 | 42 primary + 54 intersection + 1 baseline |
| orphan senses | 142 (0.03%) | Down from 93% → 0.03% via ML + AI reasoning |
| semantic embeddings | 63,434 × 384 dim | all-MiniLM-L6-v2 |
| T1 sub-ontology | 18 subcategories | carbon_capture, combustion, gasification, fuel_cells, etc. |
| polysemous labels | 14,206 (3.5%) | Terms in 2+ sources — cross-domain bridges |

## Publication data

| Table | Rows | DOIs | Notes |
|---|---|---|---|
| raw_wos_publications | 6,019 | NO (backfill needed) | DOE/NETL pubs, keywords, abstracts |
| raw_wos_natlab_publications | 224,081 | NO (backfill needed) | 21 cols, 91% abstracts, 10 national labs |
| raw_openalex_publications | 7,983 | 7,761 (97%) | titles, years, citations, funders, topics |
| openalex_pub_keywords | 68,855 | — | keyword-to-paper mappings with relevance scores |
| openalex_citations | ~400K+ | — | NEW: citation network (citing→cited), in_corpus flag |
| raw_wos_keywords_plus_vocab | 7,488 | — | Unique Keywords Plus terms |
| raw_wos_netl_tech | 3,877 | — | Pub-to-NETL org structure mapping |

## Validation suite (NEW)

| Artifact | Value | Notes |
|---|---|---|
| Gold-standard sample | 343 items | 6 AI strategy strata, ready for human annotation |
| Sensitivity (Jaccard) | 0.863 | 10% corruption → 14% result change. computation_data=1.0, earth_env=0.72 |
| Coherence (NPMI) | 0.762 | Strong vs published baselines (0.3-0.8 typical) |
| Lens divergence (Spearman) | 0.454 | Range -0.31 to 1.0 — lenses genuinely diverge |
| Citation coherence | PENDING | Waiting on citation fetch to complete |

Figures in `figures/validation/`:
- fig2_sensitivity.png — Jaccard stability under 10% corruption
- fig3_coherence.png — NPMI distribution + by-discipline breakdown
- fig4_lens_divergence.png — pairwise Spearman heatmap across lenses
- fig5_citation_coherence.png — PENDING

## Architecture

```
Sources (7 pillars + WoS + OpenAlex publications)
  ├── NASA GCMD ─────────┐
  ├── UNESCO Thesaurus ──┤
  ├── NCBI Taxonomy ─────┤
  ├── LoC LCSH ──────────┤──→ Parsers ──→ DuckDB (21 tables)
  ├── DOE OSTI ──────────┤           │
  ├── OpenAlex Topics ───┤     ┌─────┴──────────────┐
  └── MeSH (NIH) ───────┘     │  Ontology Layer     │
                               │  14 disciplines     │
  WoS NETL (6K pubs) ───────→ │  424K senses        │
  WoS NatLabs (224K pubs) ──→ │  2.58M relations    │
  OpenAlex (8K pubs) ────────→│  97 lens hats       │
  OpenAlex Citations ────────→│  ~400K+ cite edges  │
                               │  69K pub-kw links   │
                               └─────┬──────────────┘
                                     │
                            Composed lens queries
                            (Role × Org × Disc × Interest)
                            lens / lens-explore / lens-compare
                            lens-papers / lens-list

                            Validation Suite
                            gold-standard / sensitivity / coherence
                            lens divergence / citation coherence
```

## Quality scorecard: 97.8/100 (A+)

| Metric | Score | Value |
|---|---|---|
| Source Coverage | 10.0 | 7 authoritative pillars |
| Relationship Density | 10.0 | 6.09 rels/sense (was 5.79) |
| Orphan Rate | 10.0 | 0.03% (142 true orphans, all tagged) |
| Cross-Domain Bridges | 10.0 | 13K+ bridges + 101K cross-disc co-occurrence |
| Polysemy Coverage | 8.8 | 3.5% (14,206 labels in 2+ sources) |
| Discipline Balance | 10.0 | 14 disciplines, 4 tiers |
| Provenance Diversity | 10.0 | 25+ provenance types |
| Enrichment Depth | 10.0 | abstract_freq + title_freq on 45K senses |
| Hierarchy Depth | 10.0 | NETL envelopes + T1 sub-ontology |
| Semantic Embeddings | 10.0 | 63K × 384-dim |

## Performance pitfalls (AVOID)

- **NOT EXISTS on sense_relationships = infinite CPU.** Use LEFT JOIN with CTE.
- **DuckDB executemany with VARCHAR[] = pathologically slow.** Use temp tables + SQL INSERT...SELECT.
- **Python fetchall() for 422K rows → dict = slow.** Use SQL-native JOINs instead.
- **DuckDB `?` param binding = SQL text order, not definition order.** When CTEs precede the main query, params must match CTE order.
- **N-gram set intersection** for text extraction (not label×text scanning). 37× faster.
- **`python3 -u`** for unbuffered output in background scripts.
- **Kill stale background processes** before launching new DB queries.
- **Always use `duckdb` CLI** (installed v1.5.2) instead of Python wrappers for queries.

## Next up

1. [x] Embedding-based fuzzy matching (50K semantic edges)
2. [x] T1 Fossil Energy deep sub-ontology (18 subcategories)
3. [x] Orphan reduction (93% → 0.03%)
4. [x] Grant agency entity resolution (12 canonical groups)
5. [x] Agentic orphan resolution (AI reasoning, 6,535 connected)
6. [x] Lens query engine (4 CLI commands)
7. [x] OpenAlex co-occurrence edges (137K edges, SQL-native)
8. [x] Paper discovery through lens (lens-papers command)
9. [x] Validation suite (gold-standard, sensitivity, coherence, lens divergence)
10. [x] **Citation network** — 444,259 edges fetched, 32,301 in-corpus
11. [x] **Citation coherence analysis** — citing 0.228 vs random 0.013 (17×, p≈0). Fig 5 generated.
12. [x] **WoS DOI backfill** — 7 columns added to both WoS tables from Hyper + NETL xlsx
13. [x] **Expanded natlab ingestion** — 227,433 rows, 61 cols loaded
14. [x] **WoS category mappings** — 201 subject→discipline mappings loaded
15. [~] **WoS DOI→OpenAlex lookup** — PAUSED at batch 300/3959, 14,841 bridges, 596K citations. Resumable: `python3 -u scripts/lookup_wos_dois_in_openalex.py` (skips already-bridged DOIs)
16. [ ] **Run viz data scripts** — chord, Poincaré v2, UMAP nebula, coverage estimation (blocked on DB lock)

### Visualizations (NEW — session 7b)
17. [x] **Nebula HTML shell** — Three.js 3D point cloud + lens dots on sphere, needs UMAP data
18. [x] **Poincaré disk v2 HTML** — hyperbolic layout + cross-discipline gold arcs + search + Möbius zoom
19. [x] **Chord diagram HTML** — D3 circular chord replacing simple Sankey, needs data extraction
20. [ ] **UMAP 3D projection** — script ready (scripts/compute_umap_nebula.py), needs DB access
21. [ ] **HEB (Hierarchical Edge Bundling)** — concentric rings with bundled edges, ghost arcs for gaps. THE primary "see everything" diagram. Not yet built.
22. [x] **Citation tree viz** — 4-level deep organic tree, 2,656 nodes, warm roots/cool branches, clickable DOIs, depth toggle, year-band condensation

### New taxonomy pillars (downloading NOW)
23. [~] **AGROvoc** — agriculture/FAO (~40K terms), downloading SKOS
24. [~] **ERIC** — education (~12K terms), downloading
25. [~] **GeoRef** — geosciences (~30K terms), checking access (may be paywalled)
26. [~] **IEEE Thesaurus** — electrical engineering, checking access
27. [~] **Inspec** — physics/engineering (~20K terms), checking access
28. [~] **EuroVoc** — EU multilingual (~7K concepts), downloading SKOS
29. [~] **STW** — economics (~6K terms), downloading
30. [~] **FAST** — faceted subjects (~1.8M terms from LCSH), downloading
31. [ ] **Write parsers** for each new taxonomy (reuse SKOS/N-Triples patterns from LoC parser)
32. [ ] **Ingest new taxonomies** into DuckDB + ontology layer

### Coverage & completeness
33. [ ] **Coverage estimation** — script ready, needs DB. Maps our data vs OpenAlex universe.
34. [ ] **Complete WoS category list** — all 254 subjects (we have 201), identify 53 gaps
35. [ ] **Ghost arc visualization** — show gaps in HEB diagram as dim/dashed arcs

### Remaining
36. [ ] **lens-citations CLI** — bidirectional citation tree traversal command
37. [ ] **Methodology paper** — vector bundle semantics, 7-pillar cross-walk, lens composition, AI curation, validation
38. [ ] **Benchmark vs VOSviewer/CiteSpace** — comparative analysis
39. [ ] **API/query interface** — REST or GraphQL

## Don't forget

- ALWAYS be devil's advocate — challenge assumptions, no glass castles
- Audit between large steps for errors and gaps
- Push before destructive operations (always)
- Use SQL-first for all DuckDB queries (duckdb CLI, not Python wrappers)
- Use LEFT JOIN for orphan detection (NOT EXISTS = CPU death)
- DuckDB ? params bind in SQL text order, not definition order
- Use `python3 -u` for background scripts (unbuffered stdout)
- Kill stale background processes before new DB queries
- Cross-domain references are FIRST CLASS
- Keywords are vector bundles — same word, different meaning per context
- Save WORKLOG.md proactively at natural breakpoints
- New senses need discipline assignment (don't leave as general_science)
- Gold-standard sample in data/validation/ needs 2 human annotators
