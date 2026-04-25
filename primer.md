# Keyword Taxonomy Engine · primer

Updated: 2026-04-25 2:30am EDT
Repo: https://github.com/Gabicus/keyword-taxonomy-engine

## Current status

**Phase 3 ACTIVE — Lens query engine operational.** 7 pillars + 230K WoS pubs + 8K OpenAlex pubs. 421,848 senses, 2.44M relationships (5.79 rels/sense). 0.03% orphans. 4 lens query CLI commands. Quality scorecard 97.8/100 (A+). 178 tests passing.

## What this is

Universal scientific keyword taxonomy engine. Ingests authoritative keyword hierarchies from 7 global sources ("pillars"), normalizes into unified schema, stores in DuckDB. Multi-perspective ontology with "vector bundle" keyword senses — same keyword carries different meaning depending on origin, domain context, and who's asking.

End goal: multi-modal analysis tool (VOSviewer x1000) with DOE/NETL/fossil energy at center. Users stand around a sphere looking in through composed lenses (Role × Org × Discipline × Interest).

## Key Commands
```bash
python -m src.cli lens fossil_energy --search "carbon capture"  # query through lens
python -m src.cli lens-explore "combustion" --discipline materials  # explore relationships
python -m src.cli lens-compare "methane" --lenses hat:fossil_energy:researcher hat:earth_environmental:director
python -m src.cli lens-list --role researcher  # list 97 available lenses
python -m src.cli search "climate"      # basic keyword search
python -m pytest tests/ -v              # 178 tests passing
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
| keyword_senses | 421,848 | 105K base + 263K natlab WoS + 14K NETL WoS + 3.6K vocab + 1K meta |
| sense_relationships | 2,444,185 | 5.79 rels/sense. subtopic_of + related_to + bridges + equivalent + ai_reasoning |
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
| raw_openalex_publications | 7,983 | 7,761 (97%) | NEW: titles, years, citations, funders, topics |
| openalex_pub_keywords | 68,855 | — | NEW: keyword-to-paper mappings with relevance scores |
| raw_wos_keywords_plus_vocab | 7,488 | — | Unique Keywords Plus terms |
| raw_wos_netl_tech | 3,877 | — | Pub-to-NETL org structure mapping |

**WoS DOI backfill:** User needs to re-export WoS data with DOI column. Then: `ALTER TABLE ADD COLUMN doi VARCHAR` + `UPDATE` matching on `accession_number`. Easy.

## Architecture

```
Sources (7 pillars + WoS + OpenAlex publications)
  ├── NASA GCMD ─────────┐
  ├── UNESCO Thesaurus ──┤
  ├── NCBI Taxonomy ─────┤
  ├── LoC LCSH ──────────┤──→ Parsers ──→ DuckDB (20 tables)
  ├── DOE OSTI ──────────┤           │
  ├── OpenAlex Topics ───┤     ┌─────┴──────────────┐
  └── MeSH (NIH) ───────┘     │  Ontology Layer     │
                               │  14 disciplines     │
  WoS NETL (6K pubs) ───────→ │  422K senses        │
  WoS NatLabs (224K pubs) ──→ │  2.44M relations    │
  OpenAlex (8K pubs) ────────→│  97 lens hats       │
                               │  69K pub-kw links   │
                               └─────┬──────────────┘
                                     │
                            Composed lens queries ← NEW
                            (Role × Org × Disc × Interest)
                            lens / lens-explore / lens-compare
```

## Quality scorecard: 97.8/100 (A+)

| Metric | Score | Value |
|---|---|---|
| Source Coverage | 10.0 | 7 authoritative pillars |
| Relationship Density | 10.0 | 5.79 rels/sense |
| Orphan Rate | 10.0 | 0.03% (142 true orphans, all tagged) |
| Cross-Domain Bridges | 10.0 | 13K+ bridges |
| Polysemy Coverage | 8.8 | 3.5% (14,206 labels in 2+ sources) |
| Discipline Balance | 10.0 | 14 disciplines, 4 tiers |
| Provenance Diversity | 10.0 | 25+ provenance types |
| Enrichment Depth | 10.0 | abstract_freq + title_freq on 45K senses |
| Hierarchy Depth | 10.0 | NETL envelopes + T1 sub-ontology |
| Semantic Embeddings | 10.0 | 63K × 384-dim |

## Performance pitfalls (AVOID)

- **NOT EXISTS on sense_relationships = infinite CPU.** Use LEFT JOIN with CTE: `WITH connected AS (SELECT DISTINCT source_sense_id as sid... UNION SELECT DISTINCT target_sense_id...) LEFT JOIN connected`.
- **DuckDB executemany with VARCHAR[] = pathologically slow.** Use temp tables + SQL INSERT...SELECT.
- **Python fetchall() for 422K rows → dict = slow.** Use SQL-native JOINs instead. The label_to_senses pattern killed the OpenAlex co-occurrence step.
- **N-gram set intersection** for text extraction (not label×text scanning). 37× faster.
- **`python3 -u`** for unbuffered output in background scripts. Otherwise stdout is invisible until process exits.
- **Kill stale background processes** before launching new DB queries. Check `ps aux | grep duckdb`.
- **Always use `duckdb` CLI** (installed v1.5.2) instead of Python wrappers for queries.

## Next up

1. [x] Embedding-based fuzzy matching (50K semantic edges)
2. [x] T1 Fossil Energy deep sub-ontology (18 subcategories)
3. [x] Orphan reduction (93% → 0.03%)
4. [x] Grant agency entity resolution (12 canonical groups)
5. [x] Agentic orphan resolution (AI reasoning, 6,535 connected)
6. [x] Lens query engine (4 CLI commands)
7. [~] **OpenAlex pub ingestion** — pubs + keyword mappings IN (7,983 + 68,855). Co-occurrence edges NOT done (script killed). Resume with SQL-native approach.
8. [ ] **OpenAlex co-occurrence + new senses** — finish co-occurrence edges + create senses for new keywords
9. [ ] **DOI/paper-level links** — lens-filtered paper discovery ("papers about X through lens Y")
10. [ ] **WoS DOI backfill** — user re-exports with DOI column
11. [ ] **Interactive visualization prototype** — the sphere with composed lenses
12. [ ] **Versioning + Zenodo DOI** — release tag, reproducibility
13. [ ] **Methodology paper** — vector bundle semantics, 7-pillar cross-walk, lens composition, AI curation
14. [ ] **Benchmark vs VOSviewer/CiteSpace** — comparative analysis
15. [ ] **API/query interface** — REST or GraphQL

## Don't forget

- ALWAYS be devil's advocate — challenge assumptions, no glass castles
- Audit between large steps for errors and gaps
- Push before destructive operations (always)
- Use SQL-first for all DuckDB queries (duckdb CLI, not Python wrappers)
- Use LEFT JOIN for orphan detection (NOT EXISTS = CPU death)
- Use `python3 -u` for background scripts (unbuffered stdout)
- Kill stale background processes before new DB queries
- Cross-domain references are FIRST CLASS
- Keywords are vector bundles — same word, different meaning per context
- Save WORKLOG.md proactively at natural breakpoints
