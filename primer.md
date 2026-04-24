# Keyword Taxonomy Engine · primer

Updated: 2026-04-24 4:15am EDT
Repo: https://github.com/Gabicus/keyword-taxonomy-engine

## Current status

**Phase 1+2+2.1 COMPLETE.** All 6 pillars ingested. 74,086 keywords in unified table + all 6 raw tables populated (46,889 raw records with source-specific fields). 178 tests passing.

**NEXT: Phase 2.5 — Cross-Taxonomy Alignment via Multi-Expert Bayesian Consensus**

## What this is

Universal scientific keyword taxonomy engine. Ingests authoritative keyword hierarchies from 6 global sources ("pillars"), normalizes into unified schema, stores in DuckDB, enables semantic matching against publication text, and cross-links concepts across taxonomies.

End goal: multi-modal analysis tool (VOSviewer x1000) that maps publication keywords, author keywords, and abstract-extracted terms into a navigable knowledge graph with cross-domain references.

## Data lake

| Source | Unified | Raw | Notes |
|---|---|---|---|
| OpenAlex | 31,995 | 4,798 | 4 domains, 26 fields, 252 subfields, 4,516 topics, 27,197 keywords. Raw has works_count, cited_by_count |
| Library of Congress | 29,731 | 29,731 | Science + Technology subtrees, depth 0-6, bulk SKOS N-Triples. Raw has broader/narrower/related arrays |
| NASA GCMD | 4,849 | 4,849 | 6 keyword types, enriched (80% definitions). Raw has hierarchy columns, short/long names |
| UNESCO Thesaurus | 4,408 | 4,408 | English filter. Raw has French/Spanish/Russian/Arabic/Chinese labels, match URIs |
| NCBI Taxonomy | 3,044 | 3,044 | Capped at Order rank from 2.7M total. Raw has lineage, genetic codes, common names |
| DOE OSTI | 59 | 59 | 45 categories + 9 group parents. Raw has group codes, DOE programs |
| **Total** | **74,086** | **46,889** | |

## Architecture

```
Sources (6 pillars)
  ├── NASA GCMD (CSV + JSON API) ──┐
  ├── UNESCO Thesaurus (SKOS/RDF) ─┤
  ├── NCBI Taxonomy (FTP dump) ────┤
  ├── LoC LCSH (bulk SKOS NT) ─────┤──→ Parsers ──→ DuckDB (raw + unified tables)
  ├── DOE OSTI (hardcoded list) ───┤
  └── OpenAlex Topics (API) ───────┘
                                        │
                              ┌─────────┴──────────┐
                              │  5-Expert Bayesian  │
                              │  Alignment Engine   │
                              └─────────┬──────────┘
                                        │
                              Cross-taxonomy graph
                              with cross-domain refs
```

### Two-layer schema
- **Raw tables** (6): full source fidelity with source-specific columns
- **Unified table**: `keywords` — 13 fields, cross-taxonomy queryable
- **Alignment table**: `cross_taxonomy_alignment` — confidence, method, match_type, reviewed

### File structure
```
src/
  config.py          — URLs, source config, rank hierarchy
  schema.py          — DDL for 8 tables with data dictionaries
  storage.py         — KeywordStore (upsert, upsert_raw, validate, export, search, stats)
  http_client.py     — requests-cache + retries
  graph.py           — NetworkX graph ops (ancestors, descendants, paths, subtree)
  cli.py             — ingest, export, stats, enrich, search, populate-raw commands
  raw_writers.py     — per-source raw table record builders
  parsers/           — 6 source parsers (nasa_gcmd, unesco, openalex, ncbi, loc, doe_osti)
  enrichment/        — gcmd_enricher.py (per-concept JSON API)
  alignment/         — unesco_matches.py (SKOS match harvester, 0 results from mirror)
  grants/            — extractor.py (regex per agency: NSF, NIH, DOE, NASA, EU, etc.)
tests/               — 178 tests across 11 test files
```

## Execution plan

### Phase 1: Foundation ✅
### Phase 2: Expansion ✅
### Phase 2.1: Raw table population ✅

### Phase 2.5: Multi-Expert Bayesian Alignment ⬅️ NEXT

**Approach: 5-Expert Ensemble with Bayesian Consensus**

Each expert independently designs the cross-taxonomy structural architecture, then we use Bayesian agreement to settle on a consensus structure. The final structure must include cross-domain and cross-category references based on keyword network tangles.

#### The 5 Expert Hats

1. **Physical Sciences Expert** — Physics, chemistry, materials, energy, astronomy, earth science. Sees the world through NASA GCMD earth science hierarchy, DOE OSTI energy categories, NCBI molecular biology overlap, OpenAlex physical sciences domain.

2. **Life Sciences Expert** — Biology, ecology, taxonomy, medicine, genetics. Sees through NCBI taxonomic ranks, OpenAlex health/life sciences domains, LoC biological subject headings, UNESCO environmental science concepts.

3. **Applied Sciences & Engineering Expert** — Technology, engineering, computation, instrumentation. Sees through LoC Technology subtree, DOE OSTI engineering/materials categories, OpenAlex applied fields, NASA GCMD instruments/platforms.

4. **Information & Social Sciences Expert** — Knowledge organization, classification theory, social dimensions of science, policy. Sees through UNESCO thesaurus structure (education, social science, communication), LoC cross-references, OpenAlex social sciences domain, DOE OSTI policy categories.

5. **Cross-Domain Integrator** — Explicitly looks for concepts that span multiple domains. Interdisciplinary fields, boundary objects, methods that cross fields. Identifies the "tangles" where keyword nets from different taxonomies overlap and interweave.

#### Process

For each expert:
1. Examine all 6 source taxonomies through their domain lens
2. Identify top-level structural categories (aim for 15-30 per expert)
3. Map which source keywords belong to each category
4. Identify cross-references to concepts outside their domain
5. Document reasoning and confidence levels

Then:
6. Bayesian agreement: weight each expert's structural proposals by confidence, find consensus clusters
7. Build consensus taxonomy with cross-domain edges
8. Populate `cross_taxonomy_alignment` table
9. Preserve expert provenance (which expert proposed what, confidence)

### Phase 3: Intelligence (Association Engine)
- [ ] spaCy PhraseMatcher keyword loader
- [ ] AssociationEngine class
- [ ] Embedding fallback for semantic matches

### Phase 4: Integration
- [ ] Publication keyword ingestion hooks
- [ ] Grant/funding extractor integration
- [ ] Abstract NLP pipeline
- [ ] Cross-taxonomy pivoting

## Known issues
- UNESCO mirror (skos.um.es) strips exactMatch/closeMatch triples → 0 alignment results
- LoC has 52,951 subjects total but depth filter caps at 29,731 (depth ≤ 6)
- NCBI actual count is 3,044 not ~1,200 as originally estimated
- OpenAlex works_count/cited_by_count are valuable ranking signals in raw table

## Data quality audit (2026-04-24)
- 0 empty labels, 0 orphans, 0 real PK violations
- 5 cross-source ID collisions (OpenAlex numeric IDs vs NCBI tax_ids) — handled by composite PK (id, source)
- Definition coverage: DOE 85%, NASA 80%, OpenAlex 15%, LoC 5%, UNESCO 0.5%, NCBI 0%
- Alias coverage: LoC 17%, OpenAlex 14%, UNESCO 6%, NCBI 4%, NASA 0.1%, DOE 0%

## Don't forget
- ALWAYS be devil's advocate — challenge assumptions, no glass castles
- Audit between large steps for errors and gaps
- Push before destructive operations (always)
- Grant extraction is separate module, not keyword engine
- Test against real data, not just mocks
- Cross-domain references are FIRST CLASS — not afterthoughts
