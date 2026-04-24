# Keyword Taxonomy Engine — Work Log

Full step-by-step history of all work done on this project.

---

## Phase 1: Foundation (2026-04-24, Session 1)

### Step 1.1: Project scaffolding
- Created `src/` package structure: config.py, schema.py, storage.py, cli.py, http_client.py, graph.py
- DuckDB as primary store, parquet as export format
- Unified `keywords` table with 13 fields, composite PK `(id, source)`

### Step 1.2: NASA GCMD parser
- `src/parsers/nasa_gcmd.py` — CSV bulk download + per-concept JSON API enrichment
- Gotcha: Row 0 is metadata, Row 1 is headers. Variable CSV structure per keyword type.
- Two-pass approach for parent UUID resolution
- 6 keyword types: sciencekeywords (3,770), locations (656), platforms (157), providers (138), instruments (120), projects (8)
- Total: 4,849 keywords ingested

### Step 1.3: UNESCO Thesaurus parser
- `src/parsers/unesco.py` — SKOS/RDF XML parsing
- Official UNESCO URL was 404; used mirror at skos.um.es
- Mirror strips exactMatch/closeMatch triples (0 alignment results)
- 4,408 English concepts ingested (583 roots, flat-ish structure)

### Step 1.4: OpenAlex parser
- `src/parsers/openalex.py` — REST API with cursor pagination
- 4 domains → 26 fields → 252 subfields → 4,516 topics → 27,197 keywords
- works_count and cited_by_count extracted as ranking signals
- Total: 31,995 keywords ingested

### Step 1.5: NCBI Taxonomy parser
- `src/parsers/ncbi.py` — FTP dump, pipe-delimited format
- Capped at Order rank (from 2.7M total nodes)
- Skips "no rank" intermediates, filtered_parent_id for clean hierarchy
- 22 kingdoms/superkingdoms → phyla → classes → orders
- Total: 3,044 keywords ingested

### Step 1.6: Library of Congress LCSH parser
- `src/parsers/loc.py` — Bulk SKOS N-Triples (101MB gz)
- Stream-parsed with BFS from 2 root IDs (Science sh85118553, Technology sh85133067)
- Depth 0-6, polyhierarchical (subjects can have multiple broader terms)
- Total: 29,731 subjects ingested

### Step 1.7: DOE OSTI parser
- `src/parsers/doe_osti.py` — API unreliable, hardcoded fallback list
- 9 groups → 45 categories + 9 group parents
- Total: 59 keywords ingested

### Step 1.8: Test suite
- 178 tests across 11 test files
- Each parser has its own test file with synthetic sample data (no mocks)
- Schema, storage, and integration tests

---

## Phase 2: Expansion (2026-04-24, Session 1)

### Step 2.1: GCMD Enrichment
- `src/enrichment/gcmd_enricher.py` — per-concept JSON API calls
- Added definitions, related concepts, alt labels
- 80% of GCMD concepts now have definitions

### Step 2.2: UNESCO alignment attempt
- `src/alignment/unesco_matches.py` — SKOS match harvester
- Result: 0 matches (mirror strips match triples)
- Known issue documented

### Step 2.3: Grant number extractor
- `src/grants/extractor.py` — regex patterns for NSF, NIH, DOE, NASA, EU, EPSRC, DFG, NSERC
- Separate module, not keyword engine

---

## Phase 2.1: Raw Table Population (2026-04-24, Session 1)

### Step 2.1.1: Raw table schema design
- 6 raw tables with full source-specific fields and data dictionaries
- `src/raw_writers.py` — per-source raw record builders
- Raw layer preserves full fidelity; unified layer enables cross-taxonomy queries

### Step 2.1.2: Raw table population
- All 6 raw tables populated: 46,889 raw records total
- Key fields preserved: OpenAlex works_count/cited_by_count, UNESCO multilingual labels, NCBI lineage/genetic codes, LoC broader/narrower/related arrays, NASA GCMD hierarchy columns

### Step 2.1.3: Data quality audit
- 0 empty labels, 0 orphans, 0 real PK violations
- 5 cross-source ID collisions (OpenAlex numeric vs NCBI tax_ids) — handled by composite PK
- Definition coverage: DOE 85%, NASA 80%, OpenAlex 15%, LoC 5%, UNESCO 0.5%, NCBI 0%
- Alias coverage: LoC 17%, OpenAlex 14%, UNESCO 6%, NCBI 4%, NASA 0.1%, DOE 0%

---

## Phase 2.5: Multi-Expert Bayesian Cross-Taxonomy Alignment (2026-04-24, Session 2)

### Step 2.5.0: Data exploration (completed)
- Verified 178 tests pass, 74,086 keywords across 6 sources
- Queried structural overview of each source:
  - OpenAlex: 4 domains (Physical Sciences 161M works, Social Sciences 115M, Health Sciences 63M, Life Sciences 51M) → 26 fields → 252 subfields
  - NASA GCMD: sciencekeywords (3,770), locations (656), platforms (157), providers (138), instruments (120), projects (8). Science keywords: EARTH SCIENCE (15 topics) + EARTH SCIENCE SERVICES (10 topics)
  - DOE OSTI: 9 groups (Alternative Fuels, Energy Systems, Fossil Fuels, Life/Environmental, Nuclear, Other, Physical Sciences, Physics, Renewable) → 45 categories
  - UNESCO: 583 root concepts (flat-ish), heavy on social science/education/culture
  - Library of Congress: 2 roots (Science, Technology) → 65 level-1 → 29,731 total. Many "X and science" headings.
  - NCBI: 22 kingdoms → phyla → classes → orders (3,044 total, capped at Order rank)
- Found 6,419 keywords already have cross_refs (NASA GCMD instrument→platform links)
- Alignment table exists but empty (0 records)
- Created `data/alignment/experts/` directory

### Step 2.5.1: 5-Expert analysis (COMPLETE)
- Initially launched 5 parallel subagents — all hit org usage limits before producing output
- Restarted all 5 expert analyses directly (inline), completed successfully
- Each expert queried DuckDB to map source branches to their proposed categories with confidence scores

#### Cross-Domain Integrator (completed first — foundational)
- Found **163 labels** appearing in 3+ sources (e.g., "Engineering" in 4 sources, "Physics" in 4)
- Found **1,442 labels** appearing in exactly 2 sources
- **1,605 total natural alignment points** from exact label matching
- Identified 18 bridge categories spanning multiple domains
- Found 19 concept tangles (interdisciplinary terms like "remote sensing", "bioinformatics", "climate")
- Output: `data/alignment/experts/cross_domain_integrator.json`

#### Physical Sciences Expert
- **18 categories**: Atmospheric Science, Solid Earth & Geophysics, Oceanography, Cryosphere, Astronomy & Astrophysics, Nuclear & Particle Physics, Condensed Matter, AMO Physics, Chemistry, Materials Science, Energy Science, Space & Planetary Science, Paleoscience, Hydrology, Land Surface, Classical & Quantum Mechanics, Radiation Sciences, Environmental Physical Sciences
- ~12,669 estimated keywords covered
- Key cross-domain bridges: geochemistry, biophysics, atmospheric chemistry, nuclear medicine
- Output: `data/alignment/experts/physical_sciences.json`

#### Life Sciences Expert
- **19 categories**: Animal Biology, Plant Science, Mycology, Microbiology, Virology, Molecular Biology & Genetics, Ecology & Environmental Biology, Marine & Aquatic Biology, Medicine & Clinical Science, Neuroscience, Immunology, Pharmacology & Toxicology, Agriculture & Food Science, Evolutionary Biology, Anatomy & Physiology, Entomology, Parasitology, Biotechnology & Bioengineering, Developmental Biology
- ~18,694 estimated keywords covered
- Special note: NCBI Taxonomy provides organism-based classification orthogonal to other sources' topic-based classification
- Output: `data/alignment/experts/life_sciences.json`

#### Applied Sciences & Engineering Expert
- **16 categories**: Aerospace Engineering, Electrical/Electronic Engineering, Computer Science & AI, Remote Sensing & Earth Observation, Mechanical Engineering, Civil & Structural Engineering, Biomedical Engineering, Chemical Engineering, Environmental Engineering, Nuclear Engineering, Energy Engineering, Instrumentation & Measurement, Industrial & Manufacturing, Particle Accelerators, Military & Defense, Scientific Data Services
- ~8,764 estimated keywords covered
- NASA GCMD instruments (120) and platforms (157) are pure applied science goldmine
- Output: `data/alignment/experts/applied_sciences.json`

#### Information & Social Sciences Expert
- **15 categories**: Education & Learning Sciences, Sociology & Political Science, Economics & Finance, Psychology & Behavioral Science, Arts/Humanities/Cultural Studies, Law & Legal Studies, Business & Management, Communication & Media, Library & Information Science, Demographics & Population, Linguistics, Science-Society Intersections, Geography & Urban Studies, Public Health & Epidemiology, Science & Energy Policy
- ~18,128 estimated keywords covered
- UNESCO Thesaurus is richest single source for social science concepts
- LoC's unique "X and science" headings capture science-society intersections
- Output: `data/alignment/experts/information_social_sciences.json`

### Step 2.5.2: Bayesian Consensus (COMPLETE)
- Merged 68 expert-proposed categories into **30 consensus domains**
- **12 domains** claimed by 2+ experts (strong consensus)
- **1 domain** claimed by 3 experts (Energy Science & Technology — Physical Sciences + Applied Sciences + Info/Social Sciences)
- Examples of multi-expert domains:
  - Ocean & Water Science (Physical + Life Sciences)
  - Nuclear & Particle Physics (Physical + Applied Sciences)
  - Ecology & Environmental Biology (Life + Physical Sciences)
  - Medicine & Health Sciences (Life + Info/Social Sciences)
  - Energy Science & Technology (Physical + Applied + Info/Social Sciences)
- Output: `data/alignment/experts/bayesian_consensus.json`

### Step 2.5.3: Alignment Table Population (COMPLETE)
- Populated `cross_taxonomy_alignment` table with **1,943 records**
- 501 high-confidence (0.85-0.95) from labels matching in 3+ sources
- 1,442 medium-confidence (0.75) from labels matching in 2 sources
- Top source pairs by alignment count:
  - Library of Congress → OpenAlex: 589
  - OpenAlex → UNESCO Thesaurus: 553
  - Library of Congress → UNESCO Thesaurus: 411
- All alignments method = "label_match_3plus" or "label_match_2", type = "exact"

### Files produced in Phase 2.5
```
data/alignment/experts/
  cross_domain_integrator.json      — 163+1442 label overlaps, 18 bridge categories, 19 concept tangles
  physical_sciences.json            — 18 categories, 6 cross-domain bridges
  life_sciences.json                — 19 categories, 5 cross-domain bridges
  applied_sciences.json             — 16 categories, 5 cross-domain bridges
  information_social_sciences.json  — 15 categories, 5 cross-domain bridges
  bayesian_consensus.json           — 30 consensus domains, full methodology
```

### Devil's Advocate Audit: Phase 2.5

**What went well:**
- Label matching is deterministic and high-quality — 163 exact label matches in 3+ sources is solid
- Expert categories are comprehensive — 68 categories covering all domains
- 30 consensus domains provide useful structural framework

**Gaps and concerns:**
1. **Only exact label matching used** — no fuzzy matching, no embedding similarity. "Remote sensing" won't match "Earth observation" or "Satellite imagery". Phase 2.5 catches the low-hanging fruit; embedding-based matching would dramatically increase coverage.
2. **No per-keyword category assignment yet** — experts proposed categories and mapped branches, but individual keywords aren't tagged with consensus domain labels. Need a pass to assign each of 74K keywords to one or more consensus domains.
3. **Cross-refs field in keywords table still mostly empty** — 6,419 have NASA instrument→platform cross_refs, but the 1,943 alignment records aren't reflected back into the keywords.cross_refs array yet.
4. **UNESCO match triples still unavailable** — the mirror strips them. The official UNESCO source could provide free high-quality cross-vocabulary alignments.
5. **Confidence calibration is heuristic** — "3 sources = 0.85, 2 sources = 0.75" is reasonable but not true Bayesian. A proper Bayesian model would weight by source authority and domain relevance.
6. **NCBI mapping is structural, not semantic** — NCBI's organism taxonomy is orthogonal to topic-based taxonomies. The alignments from NCBI are minimal (11 to OpenAlex, 8 to LoC) because organism names rarely appear as subject headings.

### Step 2.5.4: National Lab Expert Hats (COMPLETE)
- Added 3 new expert perspectives: NETL Lab, DOE Lab, All National Labs
- **NETL Lab** (10 categories): Fossil Fuel Science, Natural Gas/Unconventional, CCUS, Power Generation, Water-Energy Nexus, Materials for Extreme Environments, Computational Science for Energy, Subsurface Science, Environmental Remediation, Hydrogen/Fuel Cells. ~1,892 keywords.
  - Key gap found: no source has a dedicated "carbon management" or "CCUS" category — concepts scattered across Environmental Science, Chemical Engineering, Geology
- **DOE Lab** (12 categories): Nuclear Reactors, Particle Physics/Accelerators, Fusion, Renewables, Nuclear Weapons/Security, HPC, Materials/Nano, Bio/Environmental, Quantum, Energy Storage/Grid, Radiation/Isotopes, Chemistry. ~2,059 keywords.
  - Mapped all 17 DOE labs to their primary categories
  - Inter-lab overlaps documented (e.g., 5 labs do materials science, 5 labs do HPC)
- **All National Labs** (12 categories): DOE Basic Science, NIH Biomedical, NASA Earth/Space, NOAA Ocean/Atmosphere, NIST Standards, USDA Agriculture, DoD Defense, EPA Environmental, USGS Earth Science, plus 3 cross-agency (Climate, Cybersecurity, Advanced Manufacturing). ~14,291 keywords.
  - Key insight: most scientific domains studied by 3+ agencies simultaneously
  - Inter-agency tensions documented (DOE vs NIH genomics, NASA vs NOAA earth observation, etc.)
- Added 10 structural alignments from lab experts (DOE OSTI → OpenAlex field mappings)
- Total alignment records now: 1,953

### Files produced in Step 2.5.4
```
data/alignment/experts/
  netl_lab.json              — 10 categories, 4 cross-domain bridges, gaps identified
  doe_lab.json               — 12 categories, 4 bridges, inter-lab overlaps documented
  all_national_labs.json     — 12 categories, 6 bridges, inter-agency tensions documented
```

### Next steps for Phase 2.5 (not yet done)
- [ ] Fuzzy/embedding-based matching for non-exact label overlaps
- [ ] Assign each keyword to consensus domain(s)
- [ ] Update keywords.cross_refs from alignment table
- [ ] Explore official UNESCO source for match triples
- [ ] Per-keyword confidence scoring within each consensus domain

---

## Phase 3: Multi-Perspective Ontology Architecture (DESIGN PHASE)

### Step 3.0: Conceptual Design Decisions (2026-04-24, Session 2)

#### Core Insight: Keywords as Vector Bundles
Victor identified that keywords aren't points — they're **vector bundles**. The same keyword carries different meaning depending on origin, domain context, and who's asking. "Plasma" means ionized gas (DOE/physics), blood component (NIH/medicine), solar wind (NASA), and a general science term (UNESCO) simultaneously. Each "sense" of a keyword has:
1. **Origin vector** — which source/taxonomy/hierarchy branch it comes from
2. **Meaning vector** — definition, scope, disambiguation in that context
3. **Association vectors** — directional connections (toward same domain, across domains, away from confusable senses)

#### Circle/Sphere User Model
The ontology serves users standing around a circle/sphere, each looking in from their own perspective:
- **Center of gravity**: DOE/NETL/fossil energy (deepest resolution, Tier 1)
- **Inner ring**: National labs, DOE-adjacent sciences, CCUS, materials, subsurface
- **Middle ring**: All federal agencies, academia, broader physical/life/applied sciences
- **Outer ring**: Policy, social sciences, international perspectives, UNESCO/LoC broad coverage
- Every user can "see past their light cone" into other domains via cross-domain bridges

#### Resolution Tier Model
- **Tier 1** (deepest): Fossil energy, CCUS, natural gas, coal, power systems — street-level detail
- **Tier 2** (deep): DOE/NETL adjacent — materials, computation, env remediation, hydrogen, nuclear
- **Tier 3** (medium): Other national lab domains — renewables, basic physics, biology, HPC, fusion
- **Tier 4** (broad): External lenses — academia, LoC, UNESCO, NCBI

#### Named Lenses for User Perspectives
Each user type gets a lens that determines what's visible and how things are weighted:
- netl_researcher, doe_program_mgr, academic_chemist, policy_analyst, nasa_scientist, etc.

#### New Schema Tables Planned
1. `keyword_senses` — one row per keyword per context (sense_id, origin vector, meaning vector, disambiguation, relevance_tags, resolution_tier)
2. `sense_relationships` — directed typed edges between senses (equivalent_sense, cross_domain_bridge, method_for, measured_by, policy_governs, applied_in, conflated_with, enables, competes_with)
3. `ontology_lenses` — named user perspectives with priority domains, boosted tags, default resolution
4. `ontology_domains` — the 30+ consensus domains as a hierarchy with tiers

#### Victor's Publication Dataset (Incoming)
Victor has publication data with rich keyword fields from DOE/NETL context. Fields available:
- `Accession_Number_UT` — publication ID
- `_1st/_2nd/_3rd_Document_Type` — document classification
- `Background`, `Support` — funding/support context
- `Keywords` — author keywords
- `Keywords_Plus` — algorithmically derived keywords (Web of Science)
- `_1st/_2nd_Subject_Sub_Heading` — subject sub-headings
- `_1st/_2nd_Subject_Category_traditional` — traditional WoS categories
- `Subject_Category_extended` — extended WoS categories
- `_1st/_2nd_Category_Heading` — category headings
- `Abstract` — full text (needs NLP extraction for keyword vector bundling)
- `Source_title` — journal/conference name
- `Title` — publication title (can be parsed for keywords)
- `Grant_Agency_Name` — funding agency

This dataset will:
1. Provide the DOE/NETL lens ground truth — real keywords from real fossil energy publications
2. Feed the NLP pipeline for keyword extraction from abstracts/titles
3. Map WoS categories to our ontology domains
4. Grant agency names connect to our national lab expert hats
5. Keywords_Plus provides algorithmic cross-references we don't have yet

### Step 3.1: WoS Publication Dataset Analysis (2026-04-24, Session 3)

Source file: `data/WoS/WOS_research_keywords_2.xlsx` — 3 tabs

#### Tab 1: "keywords with Pub IDS" — 6,019 publications, 17 fields

| Field | Fill Rate | Parse Method | Unique Values | Notes |
|---|---|---|---|---|
| Accession_Number_UT | 100% | PK, no parse | 6,019 | WoS join key |
| Keywords (author) | 45% (2,702 pubs) | Comma-split | 7,765 unique | "carbon capture", "CFD", "coal", "DFT", "machine learning" top |
| Keywords_Plus | 66% (3,970 pubs) | Comma-split | 7,554 unique | WoS algorithmic, generic: "behavior", "model", "performance" |
| _1st_Subject_Category_traditional | 100% | Direct | 91 cats | Chemistry Multidisc (891), Energy & Fuels (778), Chemistry Physical (663) |
| _2nd_Subject_Category_traditional | 57% | Direct | ~91 cats | Secondary category assignment |
| Subject_Category_extended | 100% | Comma-split | 61 cats | Coarser: Engineering (2,356), Chemistry (1,896), Energy & Fuels (1,314) |
| _1st_Subject_Sub_Heading | 99% | Direct | 3 values | Technology (3,180), Physical Sciences (2,589), Life Sciences (201) |
| _2nd_Subject_Sub_Heading | 30% | Direct | 3 values | Secondary assignment |
| _1st_Category_Heading | 100% | Direct | 2 values | "Science & Technology" (6,012), "Social Sciences" (7) |
| _2nd_Category_Heading | 0.2% | Direct | 2 values | Barely used |
| Abstract | 78% (4,691 pubs) | NLP extraction | - | Full text, richest keyword source |
| Title | 100% | NLP extraction | 5,951 unique | Has HTML entities (`<sub>2</sub>` = CO₂) |
| Source_title | 100% | Direct = journal keyword | 1,201 journals | ACS Abstracts (634), Energy & Fuels (205), I&EC Research (153) |
| _1st_Document_Type | 100% | Direct | 11 types | Article (4,219), Proceedings (819), Meeting Abstract (684) |
| _2nd_Document_Type | 5% | Direct | 5 types | Barely used |
| Grant_Agency_Name | 47% (2,799 pubs) | Comma-split + entity resolution | 2,986 "unique" | DOE appears as 4+ variants (2,182 + 530 + 265 + 76 mentions) |
| dataacquiredmonth | 100% | Timestamp | 1 value | All 2026-03-02 (single batch) |

**Top author keywords (from 13,955 instances):** carbon capture (58), computational fluid dynamics (52), coal (52), density functional theory (50), machine learning (47), oxidation (47), carbon dioxide (45), CO2 capture (45), hydrogen (44), fluidization (43), solid oxide fuel cell (43), gasification (37), carbon sequestration (37)

**Top Keywords Plus (from 25,774 instances):** behavior (234), model (231), performance (217), carbon-dioxide (213), adsorption (201), temperature (194), water (154), flow (148), simulation (140), gas (140)

**Key parsing challenges identified:**
- Grant agencies need entity resolution: "DOE", "U.S. Department of Energy", "United States Department of Energy (DOE)" = same org
- Author keywords contain compound concepts ("polymer-derived amorphous aluminosilicates")
- Titles have HTML entities needing cleanup
- Keywords_Plus are ALL CAPS, author Keywords are mixed case — need normalization

#### Tab 2: "Keywords 2" — 7,488 unique Keywords Plus (flat list)

- Single column, all UPPERCASE, one keyword per row
- 30 empty rows at top, then header "Keywords Plus2" at row 30
- Every entry unique (7,488 = 7,488 unique)
- 3,230 single-word, 4,259 multi-word
- Some artifacts: "OXIDE)" with trailing paren
- Appears to be deduplicated Keywords_Plus from a broader/different pub set than Tab 1
- Value: vocabulary supplement, but no metadata or pub linkage

#### Tab 3: "Keywords 3" — 3,877 pubs with NETL Org/Tech Structure (8 fields)

**THIS IS THE GOLD.** NETL's internal organizational taxonomy mapped to publications.

| Field | Fill | Unique | Notes |
|---|---|---|---|
| Article Title | 100% | 3,872 | Join key to Tab 1 (only 329 overlap!) |
| Technology Area | 100% | 64 | Finest granularity: Post-Combustion Capture (399), University Training (316), Advanced Turbines (240) |
| Program Area++ | 100% | 9 | Top level: H2 w/ Carbon Mgmt (1,955), Carbon Transport & Storage (1,152), Advanced Remediation (402) |
| Sub-Program Area | 100% | 27 | Mid level: Advanced Energy Systems (725), Crosscutting Research (638), Carbon Storage (569) |
| Technology Area+ | 100% | 13 | Alternative grouping, 54% #N/A |
| Consolidated_Technology_Area | 100% | 63 | ~same as Technology Area |
| Consolidated_Technology_Area_Filter | 100% | 63 | ~same as Consolidated |
| Turbines Sub-Technology | 8% (304) | 6 | Only for turbine pubs: Low Emissions Combustion (92), Materials & Manufacturing (81) |

**NETL Organizational Hierarchy (from Tab 3):**
```
Program Area (9 top-level)
  └─ Sub-Program Area (27)
      └─ Technology Area (64)
          └─ Turbines Sub-Technology (6, turbines only)
```

**Full Program Area breakdown:**
- H2 with Carbon Management: 1,955 pubs (50%) — turbines, SOFC, gasification, capture, coal
- Carbon Transport & Storage: 1,152 pubs (30%) — geologic storage, MVA, infrastructure, FutureGen
- Advanced Remediation Technology: 402 pubs — methane hydrates, unconventional, EOR, water
- Mineral Sustainability: 152 pubs — coal upgrading, rare earth, carbon ore processing
- CO2 Conversion: 72 pubs — biological uptake, catalytic, mineralization
- Methane Mitigation Technologies: 60 pubs — emissions quantification, mitigation, natural gas
- #N/A: 36 pubs — unmapped
- Natural Gas Decarb & Hydrogen: 32 pubs
- CO2 Removal: 16 pubs — DAC

**Critical finding: Tab 1 ↔ Tab 3 overlap is only 329 titles.** These are mostly different publication sets. Tab 1 = WoS academic metadata view. Tab 3 = NETL programmatic/organizational view. Both describe the same research universe from different angles. The 329 overlapping pubs are a Rosetta Stone for mapping between worldviews.

**Key insight: Tab 3 IS a time-stamped org lens.** This is exactly the kind of structure a user would "load in" to create organizational keyword grouping. It changes year-to-year as NETL reorganizes. Needs versioning.

**Key insight: Every field is a keyword type.** Journal name, grant agency, technology area, sub-program — all are nodes in the relationship graph. "FUEL" the journal → "fuel" the keyword → "Energy & Fuels" the WoS category → "Gasification Systems" the NETL tech area.

### Step 3.2: Lens Composition Architecture (2026-04-24, Session 3, Discussion)

**Victor's key insight:** Lenses aren't monolithic — they're composed from stacked vectors:

```
User Lens = Role Vector ⊗ Org Vector ⊗ Domain Vector ⊗ Interest Vector
```

**Example:** LLNL Director vs NETL Director searching "Carbon Research"
- **Shared role vector ("director"):** boosts congressional, budget, policy, program-level keywords
- **Different org vectors:** LLNL = nuclear/computation/national security; NETL = fossil energy/CCUS/materials
- **Shared query keywords ("carbon", "research"):** overlap but each carries different webs of relationships due to org vector context
- **Result:** Same query, same role, different results because org vector reshapes the keyword graph traversal

**Org/tech hierarchies as cluster envelopes:**
- NETL org chart IS a lens template — its Program → Sub-Program → Technology hierarchy wraps keyword bundles into organizational clusters
- Technology structure IS a lens template — Post-Combustion Capture → sorbents, solvents, membranes
- These envelopes themselves have vector bundles: "Post-Combustion Capture" means different things to NETL (our core mission) vs an academic chemist (one research topic among many)
- Feeding in an org structure = defining a cluster envelope that groups and weights keywords

**Lens states (configuration space):**
- Same person, different lens: NETL director looking through "budget" lens vs "technical" lens
- Different people, same lens: NETL director and LLNL director both using "director" role lens
- Different people, different lenses: NETL researcher vs NASA program manager
- These are the states of the configuration engine

**Format-agnostic ingestion requirement:**
- Engine must accept Excel, JSON, XML, CSV for org/tech hierarchy input
- Normalize any parent-child structure into our internal format
- User uploads their org chart → engine creates a lens template from it
- This is how the tool becomes universal: any org can load their structure

**Persona templates (pre-built, testable):**
- Template a base group of selectors with predefined attributes
- Test them like models against real data to validate lens behavior
- Base set: netl_director, netl_researcher, doe_program_mgr, llnl_director, academic_chemist, congressional_staffer, etc.
- Each template = pre-configured Role + Org + Domain + Interest vectors
- Users select a template as starting point, then customize

**Temporal versioning (orgs change):**
- NETL just had a reorg — current Tab 3 structure is already outdated
- Must version org structures: "NETL FY2025" vs "NETL FY2026"
- Keywords retain their vector bundles from the source org version
- New org ingestion doesn't destroy old mappings — adds a new lens version
- Part of the vector bundle: keywords carry provenance of which org structure they came from

### Step 3.3: WoS Raw Ingestion (Step A) — COMPLETE (2026-04-24, Session 3)

Created `src/parsers/wos.py` with parsers for all 3 tabs. Added 3 new staging tables to `src/schema.py`:
- `raw_wos_publications` — 6,019 rows (17 fields, comma-delimited arrays parsed)
- `raw_wos_keywords_plus_vocab` — 7,488 rows (normalized to lowercase)
- `raw_wos_netl_tech` — 3,877 rows (8 fields, #N/A and "0" cleaned to NULL)

Ingestion results verified:
- 2,702 pubs with author keywords, 3,970 with Keywords Plus, 4,691 with abstracts, 2,799 with grant agencies
- 7,487 unique normalized Keywords Plus terms (1 collision from normalization)
- 8 distinct program areas, 63 distinct technology areas
- **505 title overlaps** between Tab 1 ↔ Tab 3 (up from 329 — SQL join caught case-insensitive matches)
- HTML entities stripped from titles/abstracts (`<sub>`, `<sup>`)
- 178 tests still passing (updated table count assertion from 8→11)

Files created/modified:
- `src/parsers/wos.py` (NEW) — 3 tab parsers + `ingest_all()` function
- `src/schema.py` (MODIFIED) — 3 new staging table DDLs + added to ALL_SCHEMAS
- `tests/test_schema.py` (MODIFIED) — table count 8→11

### Step 3.4: Devil's Advocate Audit & Lens Architecture Design (2026-04-24, Session 3)

#### Devil's Advocate Audit

**Is this real?** Yes. Multi-perspective ontology with weighted traversal has academic precedent (faceted classification, ontology alignment, perspective-aware KGs). Our novel contribution: composing 6 authoritative sources + org structures + role-based lenses into a single queryable graph with polysemy awareness. Nobody has done this.

**Is it valuable?** Extremely, if we ship something usable. Three defensible angles:
1. No existing tool does cross-taxonomy alignment at this scale with perspective awareness
2. DOE/NETL fossil energy ontology = underserved niche (most taxonomy work focuses on biomedical)
3. AI agents need structured domain knowledge to avoid hallucination — this IS the context layer

**Is it citable?** Yes — minimum JASIST or Scientometrics paper. Novel contributions: multi-source alignment methodology, vector bundle model, lens composition algebra, org-as-lens concept. The 74K aligned keyword dataset alone has publication value.

**What could kill it?**
1. Scope creep — keyword engine + ontology + lenses + publication analyzer + visualization = too many PhD theses at once
2. "Works on paper" trap — need working demo where two lenses produce visibly different results on same query
3. Maintenance burden — if updating requires manual expert analysis, it dies when we stop touching it

**Victor's use cases assessed:**
- Context layer for AI agents: HIGH feasibility (this is what we're building)
- Single journal lens builder: HIGH (1,201 journals in Tab 1, each is a lens)
- Cross-domain tech transfer discovery: MEDIUM (killer demo, needs typed relationships)
- Forward/backward citation spark visualization: LATER (needs citation graph data, Phase 5+)

#### Vocabulary Decisions (avoid "domain" collision)

- **"Discipline"** = the big swaths (12 primary groupings) — avoids collision with OpenAlex "domains"
- **"Lens"** = the composed view (Role × Org × Discipline)
- **"Hat"** = pre-built test personas
- **"Envelope"** = org/tech hierarchy structure wrapping keyword clusters

#### 14 Primary Disciplines

| # | Discipline | Example Keywords |
|---|---|---|
| 1 | Fossil Energy & Carbon | coal, CCUS, gasification, combustion |
| 2 | Coal Science & Technology | coal chemistry, coal-to-liquids, coal upgrading, ash |
| 3 | Natural Gas & Unconventional | shale gas, methane, LNG, hydraulic fracturing |
| 4 | Materials & Manufacturing | alloys, ceramics, nanomaterials, corrosion |
| 5 | Chemical Sciences | catalysis, thermodynamics, electrochemistry |
| 6 | Earth & Environmental | geology, hydrology, remediation, climate |
| 7 | Computation & Data | CFD, machine learning, simulation, HPC |
| 8 | Nuclear & Particle | fission, fusion, accelerators, isotopes |
| 9 | Biological & Medical | genomics, pharmacology, toxicology, biomedical |
| 10 | Electrical & Mechanical Engineering | turbines, sensors, power systems, controls |
| 11 | Policy & Economics | regulation, funding, workforce, cost analysis |
| 12 | Renewable & Alternative Energy | solar, wind, hydrogen, geothermal |
| 13 | Space & Atmospheric | remote sensing, satellite, climate modeling |
| 14 | Mathematics & Physics Fundamentals | quantum, fluid dynamics, statistical mechanics |

Disciplines 1-3 are DOE-space deep focus (Coal and Natural Gas broken out from general Fossil Energy per Victor's direction).

#### Three Altitudes per Discipline

| Altitude | Org Level | What They See |
|---|---|---|
| 100k ft (Executive) | Lab/Program Director | Budget, congressional, strategic, cross-domain, portfolio |
| 10k ft (Program Mgr) | Division lead, PM | Tech readiness, milestones, team capabilities, related programs |
| 1k ft (Researcher) | PI, Staff Scientist | Methods, materials, measurements, publications, citations |

**14 disciplines × 3 altitudes = 42 primary hats**

#### Venn Diagram Intersection Hats

~15-20 meaningful discipline pairs × 3 altitudes = ~45-60 intersection hats. Key intersections:
- Fossil Energy ∩ Materials (extreme environment alloys)
- Fossil Energy ∩ Computation (CFD combustion modeling)
- Fossil Energy ∩ Earth/Env (CO₂ storage characterization)
- Coal ∩ Chemical Sciences (coal chemistry, pyrolysis)
- Natural Gas ∩ Earth/Env (methane migration, subsurface)
- Chemical ∩ Materials (catalysis on novel substrates)
- Chemical ∩ Biological (biochemical CO₂ conversion)
- Computation ∩ Materials (ML for materials discovery)
- Earth/Env ∩ Biological (ecological remediation)
- Nuclear ∩ Materials (radiation-resistant materials)
- Renewable ∩ Fossil Energy (hydrogen from natural gas, transition)
- Policy ∩ Fossil Energy (carbon tax, regulation)
- Coal ∩ Renewable (coal-to-hydrogen, co-firing biomass)
- Natural Gas ∩ Nuclear (SMR hydrogen, hybrid systems)
- Computation ∩ Earth/Env (subsurface simulation, climate models)

**Total test hats: ~42 primary + ~45 intersection = ~87 hats**
All generated from composition algebra — each hat = weight configuration, not custom code.

#### Lens Composition Model (decided)

**3 base layers + up to 3 interest layers on top:**
- **Layer 1: Role** — director, program_mgr, researcher, congressional_staffer, etc.
- **Layer 2: Org** — NETL, LLNL, academic university, etc. (versioned, uploadable hierarchy)
- **Layer 3: Discipline** — one or more of the 14 primary disciplines
- **Layers 4-6: Interest** — optional add-on weights (e.g., "budget focus", "CCUS deep dive", "international collaboration")

Org and Discipline layers may carry sub-layers from parent/child relationships in uploaded hierarchies.

**Composition:** Additive within layers, multiplicative across layers (hybrid).
**Neutral lens:** All weights = 1.0, shows raw unweighted graph (baseline for testing).

#### Noted for Later: Citation Spark Visualization (Phase 5+)

Forward/backward in time. Each publication = node. Keywords attach as colored vectors. Follow citations forward (who cited?) and backward (what was cited?) — keyword bundles propagate, mutate, branch across layers. Visual = sparks flying forward/back from center, each citation-distance layer adding/removing keyword connections. OpenAlex has `referenced_works` and `cited_by` fields for this.

Victor's example: project using magnetic radar + LiDAR + ground confirmations to find 1900s wells leaking methane. Cross-domain tech transfer from completely different fields. This IS the killer demo for the system.

### Step 3.5: Ontology Schema & Population (Step B) — COMPLETE (2026-04-24, Session 3)

Created 5 new ontology tables in `src/schema.py`:
- `disciplines` — 14 primary disciplines with tier assignments (T1=DOE core, T2=adjacent, T3=other lab, T4=broad)
- `keyword_senses` — vector bundle core table (0 rows yet, ready for population)
- `sense_relationships` — typed directed edges between senses (0 rows yet)
- `hierarchy_envelopes` — versioned org/tech hierarchies (107 NETL nodes)
- `ontology_lenses` — composed user perspectives (97 template hats)

Created `src/ontology.py` with:
- `populate_disciplines()` — inserts 14 disciplines
- `populate_netl_envelope()` — extracts NETL org hierarchy from raw_wos_netl_tech Tab 3: 8 programs → 26 sub-programs → 63 tech areas → 10 turbine sub-techs = 107 nodes
- `populate_template_hats()` — generates 97 hats: 42 primary (14 disciplines × 3 altitudes) + 54 intersection (18 pairs × 3 altitudes) + 1 neutral baseline
- `init_ontology()` — runs all three

NETL envelope hierarchy sample:
```
H2 with Carbon Management [program, 1955 pubs]
  └─ Advanced Energy Systems [sub_program, 724 pubs]
      └─ Advanced Turbines [technology_area, 240 pubs]
          ├─ Low Emissions Combustion [sub_technology, 70 pubs]
          ├─ Supercritical CO2 [sub_technology, 45 pubs]
          ├─ Gas Turbine Heat Transfer [sub_technology, 44 pubs]
          ├─ Materials & Manufacturing [sub_technology, 43 pubs]
          └─ Pressure Gain Combustion [sub_technology, 30 pubs]
```

**Audit results:**
- 0 orphan envelope nodes (all parents resolve)
- 0 invalid discipline FKs in hats
- 4 duplicate labels at level 3 = CORRECT (turbine sub-techs appear under both Advanced Turbines and Hydrogen Turbines — different parents, same label)
- 178 tests passing (table count updated 11→16)

**Full database state after Step B:**

| Table | Rows |
|---|---|
| keywords (unified) | 74,086 |
| cross_taxonomy_alignment | 1,953 |
| disciplines | 14 |
| hierarchy_envelopes | 107 |
| ontology_lenses | 97 |
| keyword_senses | 0 (ready) |
| sense_relationships | 0 (ready) |
| 6 raw pillar tables | 46,889 |
| 3 raw WoS tables | 17,384 |
| **Total rows** | **~140,530** |

Files created/modified:
- `src/ontology.py` (NEW) — disciplines, envelope builder, hat generator
- `src/schema.py` (MODIFIED) — 5 new ontology table DDLs
- `tests/test_schema.py` (MODIFIED) — table count 11→16

### Step 3.6: Keyword Sense Generation — COMPLETE (2026-04-24, Session 3)

Generated 74,086 keyword senses (one per keyword per source appearance) with discipline assignments.

**Discipline mapping rules built for each source:**
- OpenAlex: mapped via field (level 1) → discipline lookup (26 fields → 14 disciplines)
- NASA GCMD: mapped via keyword_type + topic → discipline (instruments→EE/ME, science keywords by topic)
- DOE OSTI: mapped via group → discipline (Fossil Fuels→fossil_energy, etc.)
- Library of Congress: mapped via full_path keyword matching (biology→biological_medical, physics→math_physics, etc.)
  - **Bug found and fixed:** initial mapping sent ALL 29,731 LoC keywords to math_physics. Added path-based matching for 30+ keyword patterns.
- UNESCO: mapped via label keyword matching (30+ patterns)
- NCBI: all → biological_medical (organism taxonomy)

**Discipline distribution after fix:**

| Tier | Discipline | Senses |
|---|---|---|
| T1 | Fossil Energy & Carbon | 223 |
| T1 | Coal Science & Technology | 4 |
| T1 | Natural Gas & Unconventional | 6 |
| T2 | EE & ME Engineering | 10,900 |
| T2 | Earth & Environmental | 6,243 |
| T2 | Computation & Data | 2,434 |
| T2 | Materials & Manufacturing | 974 |
| T2 | Chemical Sciences | 931 |
| T3 | Biological & Medical | 24,839 |
| T3 | Policy & Economics | 13,405 |
| T3 | Space & Atmospheric | 931 |
| T3 | Renewable & Alternative | 17 |
| T3 | Nuclear & Particle | 14 |
| T4 | Math & Physics Fundamentals | 13,165 |

**Polysemy tagging:** 1,605 labels appear in 2+ sources → disambiguation tags added (e.g., "Radar (Library)", "Radar (NASA)")
- 3,436 individual senses tagged with disambiguation
- Labels like "Radar" span 4 disciplines, "Engineering" spans 3, "Oceanography" spans 3

**Sense relationships:** 1,953 created from cross_taxonomy_alignment records
- 1,545 cross_domain_bridge (across disciplines)
- 408 equivalent_sense (within same discipline)

**Audit notes:**
- T1 disciplines (fossil_energy, coal_science, natural_gas) are thin (233 total) because our 6 pillar sources don't focus on fossil energy. Victor's WoS publication data will fill this gap massively in the next step.
- T3 biological_medical is heavy (24,839) because NCBI (3,044) + LoC biology/medicine branches + OpenAlex health/life domains all pile up there.

### Step 3.7: WoS Publication Sense Generation — COMPLETE (2026-04-24, Session 3)

Generated senses from all WoS data sources:

| Source | New Senses | What |
|---|---|---|
| WoS_publication (author + Keywords_Plus) | 14,046 | Unique keywords from 6,019 pubs |
| WoS_category | 102 | WoS traditional subject categories |
| WoS_journal | 200 | Top 200 journals by pub count |
| WoS_grant_agency | 98 | Top 100 grant agencies |
| WoS_keywords_plus_vocab (Tab 2) | 3,587 | Vocab supplement (3,901 already existed) |
| **Total new** | **18,033** | |

2,068 WoS keywords overlap with existing pillar senses → 3,079 new cross-references created.

**Bug found and fixed:** Initial classifier defaulted unknown keywords to "fossil_energy", inflating T1 from 233 to 14,886 with junk like "model", "behavior", "growth". Fixed by:
1. Passing WoS category through to classifier as fallback
2. Changing default from fossil_energy to ee_me_engineering (most WoS pubs are tech/engineering)
3. Expanding coal/natgas keyword sets with real-world variants

**Discipline distribution after fix (all 92,119 senses):**

| Tier | Discipline | Count |
|---|---|---|
| T1 | Fossil Energy & Carbon | 4,771 |
| T1 | Coal Science | 21 |
| T1 | Natural Gas | 23 |
| T2 | EE & ME Engineering | 13,871 |
| T2 | Earth & Environmental | 7,871 |
| T2 | Chemical Sciences | 5,480 |
| T2 | Materials & Manufacturing | 3,428 |
| T2 | Computation & Data | 2,919 |
| T3 | Biological & Medical | 25,186 |
| T3 | Policy & Economics | 13,457 |
| T3 | Space & Atmospheric | 931 |
| T3 | Renewable & Alternative | 468 |
| T3 | Nuclear & Particle | 50 |
| T4 | Math & Physics | 13,643 |

**Known remaining issues:**
- Coal (21) and NatGas (23) still thin — authors use compound phrases ("coal gasification", "marcellus shale gas production") not standalone. Substring matching would help but risks false positives.
- Some generic terms ("model", "stability") still in fossil_energy — defensible since they come from Energy & Fuels category pubs, but borderline.

**Full database: 92,119 senses, 5,442 relationships, 178 tests passing.**

### Step 3.8: Substring Matching Fix for T1 — COMPLETE (2026-04-24, Session 3)

Switched coal/natgas/fossil classification from exact match to substring matching. Added `COAL_SUBSTRINGS` (32 patterns), `NATGAS_SUBSTRINGS` (27 patterns), `FOSSIL_SUBSTRINGS` (38 patterns).

Results:
- Coal: 21 → **176 senses** (samples: "pulverized coal", "bituminous coal", "coal-gasification", "coal mine drainage")
- Natural Gas: 16 → **116 senses** (samples: "marcellus shale", "wellbore integrity", "clathrate", "hydraulic fracturing fluids")
- Fossil Energy: 4,771 → **4,945 senses** (slight increase from reclassification)

### Step 3.9: External Data Source Pull — IN PROGRESS (2026-04-24, Session 3)

Launched 4 parallel agents to pull free academic data sources:

| Source | Status | Result |
|---|---|---|
| MeSH (NIH) | ✅ COMPLETE | 31,110 descriptors, 235,902 synonyms, 99.5% with definitions. Tree hierarchy. data/raw/mesh/ |
| OpenAlex Pubs | ✅ COMPLETE | 7,983 unique works (fossil fuels, carbon capture, coal, NETL). 190MB raw. data/raw/openalex_pubs/ |
| CrossRef | ✅ COMPLETE | 1,600 works, 330 journals. Subject field deprecated 2024 — less useful. data/raw/crossref/ |
| Semantic Scholar | ✅ COMPLETE | 500 papers (5/5 queries), 16 ext fields, 32 S2 fields. Used bulk endpoint to beat rate limits. data/raw/semantic_scholar/ |

**Key findings:**
- MeSH is a goldmine: 31K controlled terms with definitions and hierarchical tree numbers. Will be a new pillar source for biomedical cross-domain bridges.
- OpenAlex pubs give us keyword-to-publication mappings we didn't have — connects our existing 31,995 OpenAlex taxonomy keywords to actual papers.
- CrossRef deprecated subject classification — journal names are the only useful signal. Low priority for ingestion.

### Step 3.10: Context Rollover + MeSH Ingestion Prep — (2026-04-24, Session 4)

Session hit context limit. Continued in new session.

**State at rollover:**
- 16 tables, 92,119 senses, 5,448 relationships, 178 tests passing
- Semantic Scholar agent still running (rate-limited, got 100 papers from query 1, retrying with 60s gaps)
- All other external pulls complete

**Session 4 actions:**
1. Checked Semantic Scholar agent — still battling 429 rate limits. Has partial cache (148KB, ~100 papers from query 1). Running with 60s inter-query delays.

2. Analyzed MeSH data structure for ingestion:
   - 31,110 descriptors with: ui (PK), heading, tree_numbers (array, polyhierarchical), scope_note (97% fill), entries (avg 7.6 synonyms), category
   - Category distribution: D=Chemicals/Drugs (10,688), C=Diseases (5,069), B=Organisms (3,967), E=Techniques (3,141), G=Phenomena (2,513), N=Health Care (2,020), A=Anatomy (1,908), F=Psychiatry (1,267), I=Anthro/Education (759), J=Technology/Industry (657), L=Info Science (501), H=Disciplines (479), Z=Geographicals (406), M=Named Groups (361), K=Humanities (208), V=Publication Characteristics (194)
   - Tree depth distribution enables full hierarchy reconstruction
   - 30,960 have scope_notes (definitions) — 99.5% coverage

3. Analyzed OpenAlex pubs summary:
   - 7,983 unique works with: openalex_id, doi, title, publication_year, cited_by_count, topic_ids (semicolon-delimited), topic_labels, keyword_labels, type
   - These map existing OpenAlex taxonomy keywords to actual publications
   - Gives us keyword-to-paper frequency signals we didn't have before

4. Added `raw_mesh` table to schema.py (17th table):
   - Fields: ui, heading, tree_numbers[], scope_note, entries[], mesh_category, tree_depth, parent_uis[], ingested_at
   - Indexes on heading, category, depth
   - Updated ALL_SCHEMAS list and test assertion (16→17)

5. **CrossRef evaluation**: Subject classification deprecated 2024. Journal names are only useful signal. 1,600 works, 330 container-titles, 244 journals. Low priority — may skip formal ingestion and use journal names as metadata enrichment only.

6. **Semantic Scholar status**: Agent retrying with progressive backoff (10s→60s waits). S2 API imposes aggressive rate limits without API key (100 req/5min). Got 100 papers from first query (fossil+energy+carbon+capture). Remaining 4 queries pending. Partial data saved to cache file.

### Step 3.11: MeSH Ingestion — COMPLETE (2026-04-24, Session 4)

MeSH is now the 7th pillar source.

- Created `src/parsers/mesh.py` — parses pre-downloaded JSON into raw + unified records
- Added `raw_mesh` table to schema (17th table): ui, heading, tree_numbers[], scope_note, entries[], mesh_category, tree_depth, parent_uis[]
- Raw ingestion: 31,110 descriptors into `raw_mesh`
- Unified ingestion: 31,110 into `keywords` (source="MeSH")
- Added MeSH discipline mapping to `ontology.py`: category letter → discipline, with energy keyword overrides
- **Performance note**: Python executemany with array columns was pathological (12+ min, killed). SQL INSERT...SELECT from raw table completed in seconds.

**Source counts after MeSH:**
| Source | Count |
|---|---|
| OpenAlex | 31,995 |
| MeSH | 31,110 |
| Library of Congress | 29,731 |
| NASA GCMD | 4,849 |
| UNESCO Thesaurus | 4,408 |
| NCBI Taxonomy | 3,044 |
| DOE OSTI | 59 |
| **Total** | **105,196** |

**Semantic Scholar complete:**
- 500 papers from 5/5 queries (fossil+energy, coal+gasification, natural+gas, CO2 capture, SOFC)
- 16 external fields (top: Environmental Science 131, Materials Science 94, Chemistry 67, Medicine 66)
- 32 S2-native fields (top: Environmental Science 421, Engineering 354, Chemistry 121)
- Agent switched from `/paper/search` to `/paper/search/bulk` endpoint to bypass aggressive rate limits

### Step 3.12: Sense Regeneration with MeSH — COMPLETE (2026-04-24, Session 4)

Regenerated all senses to include MeSH keywords. Fixed critical performance bottleneck.

**Performance fix:** Rewrote `populate_keyword_senses()` to use temp table + SQL INSERT...SELECT instead of Python executemany. DuckDB's executemany with array columns (VARCHAR[]) is pathologically slow — 12+ min for 105K rows. SQL approach completes in seconds.

**Results after MeSH integration:**
| Metric | Before | After | Change |
|---|---|---|---|
| Unified keywords | 74,086 | 105,196 | +31,110 |
| Total senses | 92,119 | 123,202 | +31,083 |
| Polysemous labels | 1,605 | **5,205** | +3,600 (3.2×) |
| Relationships | 5,448 | 5,541 | +93 |

**MeSH discipline distribution:**
- biological_medical: 15,315 (49%)
- chemical_sciences: 10,586 (34%)
- policy_economics: 2,213, math_physics: 1,841
- earth_environmental: 418, computation_data: 309, ee_me_engineering: 254
- nuclear_particle: 145, natural_gas: 10, coal_science: 8

**Key insight:** Polysemy tripled (1,605→5,205) because MeSH shares thousands of terms with existing pillars. Terms like "Carbon Dioxide", "Methane", "Combustion", "Electrode", "Catalysis" now have senses in both MeSH and T1/T2 disciplines — creating exactly the cross-domain bridges the lens system needs.

### Next steps for Phase 3
- [x] Build MeSH parser + ingest 31K descriptors as 7th pillar source
- [ ] Generate MeSH keyword senses with discipline mapping
- [ ] Ingest OpenAlex pub-level keyword mappings (frequency signals)
- [ ] Process Semantic Scholar results when agent completes
- [ ] NLP pipeline for abstract keyword extraction (4,691 abstracts)
- [ ] Title keyword extraction
- [ ] Build fossil energy Tier 1 deep sub-ontology from publication keywords
- [ ] Create initial lens profiles with actual query capability
- [ ] Type relationships (upgrade to sense-level directed edges with richer types)
- [ ] Grant agency entity resolution (2,986 variants → ~200 canonical)
- [ ] Embedding-based fuzzy matching for non-exact labels
