# Keyword Association Engine · primer

Updated: 2026-04-24 3:30am EDT

## Current status

**Phase 1+2+2.1 COMPLETE.** All 6 pillars ingested. 74,086 keywords in unified table + all 6 raw tables populated (46,889 raw records with source-specific fields). 178 tests passing.

## Data lake

| Source | Count | Notes |
|---|---|---|
| OpenAlex | 31,995 | 4 domains, 26 fields, 252 subfields, 4,516 topics, 27,197 keywords |
| Library of Congress | 29,731 | Science + Technology subtrees, depth 0-6, bulk SKOS N-Triples |
| NASA GCMD | 4,849 | 6 keyword types, enriched (80% definitions, aliases, cross_refs) |
| UNESCO Thesaurus | 4,408 | English filter, broader chains, related concepts |
| NCBI Taxonomy | 3,044 | Capped at Order rank, from 2.7M total |
| DOE OSTI | 59 | 45 categories + 9 group parents, hardcoded fallback |
| **Total** | **74,086** | |

## Architecture

```
Sources (6 pillars)
  ├── NASA GCMD (CSV + JSON API) ──┐
  ├── UNESCO Thesaurus (SKOS/RDF) ─┤
  ├── NCBI Taxonomy (FTP dump) ────┤
  ├── LoC LCSH (bulk SKOS NT) ─────┤──→ Parsers ──→ Unified Schema ──→ DuckDB
  ├── DOE OSTI (hardcoded list) ───┤
  └── OpenAlex Topics (API) ───────┘
                                        │
                              ┌─────────┴──────────┐
                              │  Association Engine │
                              │  (spaCy + embeddings)
                              └─────────┬──────────┘
                                        │
                              Cross-taxonomy alignment
```

### Two-layer schema
- **Raw tables** (6): `raw_nasa_gcmd` (4,849), `raw_unesco` (4,408), `raw_openalex` (4,798), `raw_ncbi` (3,044), `raw_loc` (29,731), `raw_doe_osti` (59) — full source fidelity
- **Unified table**: `keywords` — 13 fields, cross-taxonomy queryable
- **Alignment table**: `cross_taxonomy_alignment` — confidence, method, match_type, reviewed

### File structure
```
src/
  config.py          — URLs, source config, rank hierarchy
  schema.py          — DDL for 8 tables with data dictionaries
  storage.py         — KeywordStore (upsert, validate, export, search, stats)
  http_client.py     — requests-cache + retries
  graph.py           — NetworkX graph ops (ancestors, descendants, paths, subtree)
  cli.py             — ingest, export, stats, enrich, search, populate-raw commands
  raw_writers.py     — per-source raw table record builders
  parsers/
    nasa_gcmd.py     — two-pass CSV + synthetic roots
    unesco.py        — rdflib SKOS/RDF, English filter
    openalex.py      — cursor-based API pagination
    ncbi.py          — taxdump.tar.gz, rank filtering
    loc.py           — bulk SKOS N-Triples, BFS subtree filter
    doe_osti.py      — API with hardcoded fallback
  enrichment/
    gcmd_enricher.py — per-concept JSON API (definitions, aliases, cross_refs)
  alignment/
    unesco_matches.py — SKOS match harvester (0 results from mirror — known issue)
  grants/
    extractor.py     — regex per agency (NSF, NIH, DOE, NASA, EU, EPSRC, DFG, NSERC)
tests/               — 178 tests across 11 test files
```

## Execution plan

### Phase 1: Foundation ✅
- [x] Project structure, config, requirements
- [x] DuckDB schema + storage utilities
- [x] HTTP session helper with caching
- [x] NASA GCMD parser + enrichment (definitions, aliases, cross_refs)
- [x] UNESCO parser
- [x] Tests

### Phase 2: Expansion ✅
- [x] LoC parser (rewritten: bulk SKOS N-Triples, 30s vs hours)
- [x] NCBI parser (3,044 nodes from 2.7M)
- [x] DOE OSTI parser
- [x] OpenAlex parser (31,995 records)
- [x] All 6 pillars ingested, validated

### Phase 2.1: Raw table population ✅
- [x] Generic `upsert_raw()` in storage layer
- [x] `src/raw_writers.py` — per-source raw record builders
- [x] All 6 raw tables populated with source-specific fields
- [x] CLI `populate-raw` command + `ingest --raw` flag

### Phase 2.5: Alignment ⬅️ NEXT
- [ ] UNESCO alignment — need official RDF source (mirror strips match triples)
- [ ] Manual crosswalk for top ~200 root/branch nodes
- [ ] Embedding-based similarity (sentence-transformers)
- [ ] LLM spot-check for ambiguous mappings
- [ ] Populate cross_refs in unified schema

### Phase 3: Intelligence (Association Engine)
- [ ] spaCy PhraseMatcher keyword loader
- [ ] AssociationEngine class
- [ ] Embedding fallback for semantic matches
- [ ] Tests against real publication abstracts

### Phase 4: Integration
- [ ] Publication keyword ingestion hooks
- [ ] Grant/funding extractor (module exists, needs integration)
- [ ] Abstract NLP pipeline
- [ ] Cross-taxonomy pivoting

## Known issues
- UNESCO mirror (skos.um.es) strips exactMatch/closeMatch triples → 0 alignment results
- LoC has 52,951 subjects total but depth filter caps at 29,731 (depth ≤ 6)
- Raw tables now populated — `populate-raw` command or `ingest --raw`
- NCBI actual count is 3,044 not ~1,200 as originally estimated

## Don't forget
- ALWAYS be devil's advocate — challenge assumptions, no glass castles
- Audit between large steps
- Push before destructive operations
- Grant extraction is separate module
- Test against real data, not just mocks
