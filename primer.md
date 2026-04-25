# Keyword Taxonomy Engine · primer

Updated: 2026-04-24 10:30pm EDT
Repo: https://github.com/Gabicus/keyword-taxonomy-engine

## Current status

**Phase 3 ACTIVE — Full enrichment complete.** 7 pillars + 224K national lab publications ingested. 421,819 senses, 2.36M relationships (5.60 rels/sense). All WoS fields processed with per-field provenance. 8.3% orphan rate. 178 tests passing.

## What this is

Universal scientific keyword taxonomy engine. Ingests authoritative keyword hierarchies from 7 global sources ("pillars"), normalizes into unified schema, stores in DuckDB. Multi-perspective ontology with "vector bundle" keyword senses — same keyword carries different meaning depending on origin, domain context, and who's asking.

End goal: multi-modal analysis tool (VOSviewer x1000) with DOE/NETL/fossil energy at center. Users stand around a sphere looking in through composed lenses (Role × Org × Discipline × Interest).

## Data lake

| Source | Unified | Raw | Notes |
|---|---|---|---|
| OpenAlex | 31,995 | 4,798 | 4 domains → 27K keywords. works_count ranking signal |
| MeSH (NIH) | 31,110 | 31,110 | 16 categories, 97% definitions, avg 7.6 synonyms. Tree hierarchy |
| Library of Congress | 29,731 | 29,731 | Science + Technology subtrees, bulk SKOS |
| NASA GCMD | 4,849 | 4,849 | 6 keyword types, 80% enriched |
| UNESCO Thesaurus | 4,408 | 4,408 | English filter, mirror strips match triples |
| NCBI Taxonomy | 3,044 | 3,044 | Capped at Order rank |
| DOE OSTI | 59 | 59 | 45 categories + 9 groups |
| **Total** | **105,196** | **77,999** | |

## Ontology layer

| Table | Count | Notes |
|---|---|---|
| keyword_senses | 421,819 | 105K base + 263K natlab WoS + 14K NETL WoS + 3.6K vocab + 1K meta |
| sense_relationships | 2,360,061 | 5.60 rels/sense. 2.05M subtopic_of + 289K related_to + 13K bridges + 8K equivalent |
| disciplines | 14 | T1: fossil/coal/natgas, T2: materials/chem/earth/compute/EE, T3: bio/policy/space/renew/nuclear, T4: math/physics |
| hierarchy_envelopes | 107 | NETL org: 8 programs → 26 sub → 63 tech → 10 turbine |
| ontology_lenses | 97 | Template hats: 42 primary + 54 intersection + 1 baseline |
| polysemous labels | 5,205 | Terms in 2+ sources — cross-domain bridges |
| orphan senses | 35,070 (8.3%) | Down from 93% pre-relationships |

## Enrichment tags on senses

| Tag | Senses | Source |
|---|---|---|
| abstract_freq | 26,408 | N-gram extraction from 207K abstracts |
| title_freq | 19,379 | N-gram extraction from 230K titles |
| pub_freq | 6,748 | OpenAlex publication frequency |
| Per-field provenance | all WoS senses | author_keyword, keywords_plus, subject_cat, etc. |

## WoS publication data (4 staging tables)

- raw_wos_publications: 6,019 DOE/NETL pubs with keywords, abstracts, categories
- raw_wos_natlab_publications: 224,081 other national lab pubs (21 cols, 91% with abstracts)
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
  ├── LoC LCSH ──────────┤──→ Parsers ──→ DuckDB (18 tables)
  ├── DOE OSTI ──────────┤           │
  ├── OpenAlex Topics ───┤     ┌─────┴──────────┐
  └── MeSH (NIH) ───────┘     │  Ontology Layer │
                               │  14 disciplines │
  WoS NETL (6K pubs) ───────→ │  422K senses    │
  WoS NatLabs (224K pubs) ──→ │  2.36M relations│
                               │  97 lens hats   │
                               └─────┬──────────┘
                                     │
                            Composed lens queries
                            (Role × Org × Disc × Interest)
```

### File structure
```
src/
  config.py, schema.py (18 tables), storage.py, http_client.py, graph.py, cli.py
  raw_writers.py, ontology.py (disciplines, senses, envelopes, lenses, hats)
  parsers/ — 8 parsers (nasa_gcmd, unesco, openalex, ncbi, loc, doe_osti, wos, mesh)
  enrichment/, alignment/, grants/
scripts/ — build_relationships.py, ingest_natlab_wos.py, enrich_wos_context.py, + data pulls
tests/ — 178 tests across 11 files
data/raw/, data/lake/ (gitignored)
```

## 14 Disciplines (Resolution Tiers)

- **T1 (DOE core):** Fossil Energy & Carbon (14,948), Coal Science (373), Natural Gas (305)
- **T2 (DOE adjacent):** EE/ME Engineering (98,790), Chemical Sciences (79,740), Materials (33,113), Earth & Environmental (31,493), Computation & Data (22,240)
- **T3 (Other national lab):** Biological & Medical (60,840), Policy & Economics (17,496), Nuclear & Particle (7,249), Renewable & Alternative (4,336), Space & Atmospheric (934)
- **T4 (Broad):** Mathematics & Physics (49,962)

## Performance notes

- **DuckDB executemany with VARCHAR[] columns is pathologically slow.** NEVER use VARCHAR[] in executemany. Use scalar temp tables + SQL expressions (`ARRAY[]::VARCHAR[]`, `string_split()`).
- **N-gram set intersection for text extraction.** Don't scan N labels against each text (O(labels×texts)). Extract n-grams from text, check set membership (O(words×max_ngram)). 37× faster. 207K abstracts in 102s.
- **Pure SQL INSERT...SELECT** for bulk relationship insertion — avoids executemany entirely.

## Next up

1. [ ] Orphan sense reduction (35K orphans, 8.3% — attack with additional relationship strategies)
2. [ ] Embedding-based fuzzy matching for non-exact labels
3. [ ] Grant agency entity resolution (2,986+ variants → ~200 canonical)
4. [ ] Build fossil energy T1 deep sub-ontology from publication keywords
5. [ ] Create lens query capability (the actual "look through the lens" feature)
6. [ ] Ingest OpenAlex pub-level data (7,983 works with keyword-to-paper mappings)

## Don't forget

- ALWAYS be devil's advocate — challenge assumptions, no glass castles
- Audit between large steps for errors and gaps
- Push before destructive operations (always)
- Cross-domain references are FIRST CLASS — not afterthoughts
- Keywords are vector bundles — same word, different meaning per context
- Org structures are cluster envelopes with temporal versioning
- Save WORKLOG.md proactively at natural breakpoints
