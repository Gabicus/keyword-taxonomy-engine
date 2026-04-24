# Keyword Taxonomy Engine · primer

Updated: 2026-04-24 6:00pm EDT
Repo: https://github.com/Gabicus/keyword-taxonomy-engine

## Current status

**Phase 3 ACTIVE.** 7 pillars ingested. 105,196 unified keywords → 123,202 senses with discipline assignments. 5,205 polysemous labels creating cross-domain bridges. 178 tests passing.

## What this is

Universal scientific keyword taxonomy engine. Ingests authoritative keyword hierarchies from 7 global sources ("pillars"), normalizes into unified schema, stores in DuckDB. Multi-perspective ontology with "vector bundle" keyword senses — same keyword carries different meaning depending on origin, domain context, and who's asking.

End goal: multi-modal analysis tool (VOSviewer x1000) with DOE/NETL/fossil energy at center. Users stand around a sphere looking in through composed lenses (Role × Org × Discipline × Interest).

## Data lake

| Source | Unified | Raw | Notes |
|---|---|---|---|
| OpenAlex | 31,995 | 4,798 | 4 domains → 27K keywords. works_count ranking signal |
| **MeSH (NEW)** | **31,110** | **31,110** | **16 categories, 97% definitions, avg 7.6 synonyms. Tree hierarchy.** |
| Library of Congress | 29,731 | 29,731 | Science + Technology subtrees, bulk SKOS |
| NASA GCMD | 4,849 | 4,849 | 6 keyword types, 80% enriched |
| UNESCO Thesaurus | 4,408 | 4,408 | English filter, mirror strips match triples |
| NCBI Taxonomy | 3,044 | 3,044 | Capped at Order rank |
| DOE OSTI | 59 | 59 | 45 categories + 9 groups |
| **Total** | **105,196** | **77,999** | |

## Ontology layer

| Table | Count | Notes |
|---|---|---|
| keyword_senses | 123,202 | 105K base + 14K WoS pub + 3.6K WoS vocab + 400 WoS metadata |
| sense_relationships | 5,541 | 1,953 alignment + 3,588 WoS cross-refs |
| disciplines | 14 | T1: fossil/coal/natgas, T2: materials/chem/earth/compute/EE, T3: bio/policy/space/renew/nuclear, T4: math/physics |
| hierarchy_envelopes | 107 | NETL org: 8 programs → 26 sub → 63 tech → 10 turbine |
| ontology_lenses | 97 | Template hats: 42 primary + 54 intersection + 1 baseline |
| polysemous labels | 5,205 | Terms in 2+ sources — cross-domain bridges |

## WoS publication data (3 staging tables)

- raw_wos_publications: 6,019 DOE/NETL pubs with keywords, abstracts, categories
- raw_wos_keywords_plus_vocab: 7,488 unique Keywords Plus terms
- raw_wos_netl_tech: 3,877 records mapping pubs to NETL org structure

## External data pulled (data/raw/)

| Source | Location | Size | Status |
|---|---|---|---|
| MeSH | data/raw/mesh/ | 31K descriptors | ✅ Ingested as 7th pillar |
| OpenAlex pubs | data/raw/openalex_pubs/ | 7,983 works, 190MB | ✅ Pulled, not yet ingested |
| Semantic Scholar | data/raw/semantic_scholar/ | 500 papers, 16 fields | ✅ Pulled, coarse taxonomy |
| CrossRef | data/raw/crossref/ | 1,600 works | ✅ Low priority (subject deprecated) |

## Architecture

```
Sources (7 pillars + WoS publications)
  ├── NASA GCMD ─────────┐
  ├── UNESCO Thesaurus ──┤
  ├── NCBI Taxonomy ─────┤
  ├── LoC LCSH ──────────┤──→ Parsers ──→ DuckDB (17 tables)
  ├── DOE OSTI ──────────┤           │
  ├── OpenAlex Topics ───┤     ┌─────┴──────────┐
  └── MeSH (NIH) ───────┘     │  Ontology Layer │
                               │  14 disciplines │
  WoS Publications ──────────→ │  123K senses    │
                               │  5.5K relations │
                               │  97 lens hats   │
                               └─────┬──────────┘
                                     │
                            Composed lens queries
                            (Role × Org × Disc × Interest)
```

### File structure
```
src/
  config.py, schema.py (17 tables), storage.py, http_client.py, graph.py, cli.py
  raw_writers.py, ontology.py (disciplines, senses, envelopes, lenses, hats)
  parsers/ — 8 parsers (nasa_gcmd, unesco, openalex, ncbi, loc, doe_osti, wos, mesh)
  enrichment/, alignment/, grants/
scripts/ — download_parse_mesh.py, fetch_openalex_pubs.py, fetch_crossref.py
tests/ — 178 tests across 11 files
data/raw/, data/lake/ (gitignored)
```

## 14 Disciplines (Resolution Tiers)

- **T1 (DOE core):** Fossil Energy & Carbon, Coal Science, Natural Gas & Unconventional
- **T2 (DOE adjacent):** Materials, Chemical Sciences, Earth & Environmental, Computation & Data, EE/ME Engineering
- **T3 (Other national lab):** Biological & Medical, Policy & Economics, Space & Atmospheric, Renewable & Alternative, Nuclear & Particle
- **T4 (Broad):** Mathematics & Physics Fundamentals

## Performance notes

- **DuckDB executemany with VARCHAR[] columns is pathologically slow.** Use temp table + SQL INSERT...SELECT instead. Seconds vs 12+ minutes for 105K rows.
- populate_keyword_senses() rewritten to use this fast path.

## Next up

1. [ ] Ingest OpenAlex pub-level keyword mappings (7,983 works → keyword frequency signals)
2. [ ] NLP pipeline: extract keywords from 4,691 WoS abstracts
3. [ ] Title keyword extraction
4. [ ] Build fossil energy T1 deep sub-ontology from publication keywords
5. [ ] Create lens query capability (the actual "look through the lens" feature)
6. [ ] Type relationships (upgrade to richer sense-level directed edges)
7. [ ] Grant agency entity resolution (2,986 variants → ~200 canonical)
8. [ ] Embedding-based fuzzy matching for non-exact labels

## Don't forget

- ALWAYS be devil's advocate — challenge assumptions, no glass castles
- Audit between large steps for errors and gaps
- Push before destructive operations (always)
- Cross-domain references are FIRST CLASS — not afterthoughts
- Keywords are vector bundles — same word, different meaning per context
- Org structures are cluster envelopes with temporal versioning
- Save WORKLOG.md proactively at natural breakpoints
