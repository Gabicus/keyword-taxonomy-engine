"""Database schema definitions for the keyword data lake.

Two-layer architecture:
  Raw Layer  — source-specific tables preserving full fidelity from each source
  Curated Layer — unified `keywords` table for cross-taxonomy queries

Each raw table has a data dictionary in its docstring explaining every field,
its source, and what values to expect.
"""

# =============================================================================
# RAW LAYER — Source-Specific Tables
# =============================================================================

RAW_NASA_GCMD = """
-- ============================================================
-- raw_nasa_gcmd: NASA Global Change Master Directory keywords
-- ============================================================
-- Source: https://gcmd.earthdata.nasa.gov/kms/
-- Format: CSV bulk + per-concept JSON API enrichment
-- Coverage: ~4,800 keywords across 6 types
-- Update frequency: Quarterly (version number increments)
--
-- FIELD DICTIONARY:
--   uuid           — GCMD's native UUID for this concept (primary key)
--   pref_label     — Canonical keyword name, ALL CAPS for science keywords,
--                     mixed case for instruments/platforms
--   keyword_type   — Which GCMD scheme: sciencekeywords, instruments,
--                     platforms, projects, providers, locations
--   category       — Top-level hierarchy bucket (e.g., "EARTH SCIENCE",
--                     "Earth Remote Sensing Instruments")
--   topic          — Second-level hierarchy (science keywords only)
--   term           — Third-level hierarchy (science keywords only)
--   var_level_1    — Fourth-level (science keywords only)
--   var_level_2    — Fifth-level (science keywords only)
--   var_level_3    — Sixth-level (science keywords only)
--   detailed_var   — Seventh-level, most specific (science keywords only)
--   short_name     — Abbreviated name (instruments, platforms, providers)
--                     e.g., "MODIS", "NOAA", "ISS"
--   long_name      — Full expanded name, often the official instrument/platform
--                     name e.g., "Moderate Resolution Imaging Spectroradiometer"
--   definition     — Prose definition with scientific context. ~30% of concepts
--                     have definitions. From JSON API enrichment, not CSV.
--   def_reference  — Citation for the definition (author, year, publication).
--                     Valuable for provenance tracking.
--   is_leaf        — Boolean. True = no child concepts. From JSON API.
--                     Useful for knowing which keywords are terminal/specific.
--   parent_uuid    — UUID of direct parent in GCMD hierarchy. Resolved from
--                     CSV hierarchy columns, not API.
--   broader_json   — JSON array of {uuid, prefLabel, scheme} for broader
--                     concepts. From API. May differ from parent_uuid when
--                     concepts have multiple broader terms.
--   narrower_json  — JSON array of {uuid, prefLabel, scheme} for narrower
--                     concepts. From API. Useful for building complete trees.
--   related_json   — JSON array of related concepts (cross-scheme links
--                     within GCMD, e.g., instrument→platform relationships)
--   alt_labels     — Array of alternative names from API altLabels field
--   resources_json — JSON array of external resource links (URLs to docs,
--                     standards, reference materials)
--   full_path      — Computed hierarchical path using " > " separator
--                     e.g., "EARTH SCIENCE > ATMOSPHERE > AEROSOLS"
--   hierarchy_level — Integer depth (0=root category, 6=detailed_variable)
--   last_modified  — Per-concept modification date from API (ISO format)
--                     Different from schema version — tracks individual edits
--   keyword_version — GCMD schema version (e.g., "23.7"). All concepts share
--                     the same version within a release.
--   ingested_at    — When our system last fetched/processed this record
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_nasa_gcmd (
    uuid VARCHAR PRIMARY KEY,
    pref_label VARCHAR NOT NULL,
    keyword_type VARCHAR NOT NULL,
    category VARCHAR,
    topic VARCHAR,
    term VARCHAR,
    var_level_1 VARCHAR,
    var_level_2 VARCHAR,
    var_level_3 VARCHAR,
    detailed_var VARCHAR,
    short_name VARCHAR,
    long_name VARCHAR,
    definition TEXT,
    def_reference TEXT,
    is_leaf BOOLEAN,
    parent_uuid VARCHAR,
    broader_json JSON,
    narrower_json JSON,
    related_json JSON,
    alt_labels VARCHAR[],
    resources_json JSON,
    full_path VARCHAR,
    hierarchy_level INTEGER,
    last_modified VARCHAR,
    keyword_version VARCHAR,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""

RAW_UNESCO = """
-- ============================================================
-- raw_unesco: UNESCO Thesaurus concepts
-- ============================================================
-- Source: https://vocabularies.unesco.org/exports/thesaurus/
-- Format: SKOS/RDF (XML)
-- Coverage: ~4,400 concepts in 7 micro-thesauri
-- Update frequency: Irregular, typically annual
--
-- FIELD DICTIONARY:
--   concept_uri    — Full URI, e.g., "http://vocabularies.unesco.org/thesaurus/concept123"
--                     Acts as globally unique identifier (primary key)
--   pref_label_en  — English preferred label (skos:prefLabel xml:lang="en")
--   pref_label_fr  — French preferred label (UNESCO is bilingual EN/FR)
--   pref_label_es  — Spanish preferred label
--   pref_label_ru  — Russian preferred label
--   pref_label_ar  — Arabic preferred label
--   pref_label_zh  — Chinese preferred label
--   definition_en  — English definition (skos:definition). ~40% of concepts.
--   scope_note_en  — English scope note (skos:scopeNote). Usage guidance,
--                     disambiguation. Sometimes present when definition is not.
--   notation       — Classification notation code (skos:notation).
--                     Alphanumeric codes from UNESCO's classification system.
--   alt_labels_en  — Array of English alternative labels (skos:altLabel).
--                     Synonyms, variant spellings, abbreviations.
--   hidden_labels  — Array of hidden labels (skos:hiddenLabel).
--                     Common misspellings, search aids not shown to users.
--   broader_uris   — Array of broader concept URIs (skos:broader).
--                     Most concepts have 1, some have 2+ (polyhierarchy).
--   narrower_uris  — Array of narrower concept URIs (skos:narrower).
--   related_uris   — Array of related concept URIs (skos:related).
--                     Associative (non-hierarchical) relationships.
--   exact_match    — Array of URIs from OTHER vocabularies that mean exactly
--                     the same thing (skos:exactMatch). FREE CROSS-TAXONOMY
--                     ALIGNMENT. May link to LCSH, AGROVOC, EuroVoc, etc.
--   close_match    — Array of URIs from other vocabularies with similar but
--                     not identical meaning (skos:closeMatch).
--   broad_match    — Array of URIs from other vocabularies that are broader
--                     (skos:broadMatch).
--   narrow_match   — Array of URIs from other vocabularies that are narrower
--                     (skos:narrowMatch).
--   in_scheme      — Array of scheme URIs this concept belongs to.
--                     UNESCO has micro-thesauri (sub-schemes).
--   top_concept_of — Array of scheme URIs where this is a top concept.
--                     Identifies root nodes of each micro-thesaurus.
--   modified       — Last modification date (dcterms:modified). ISO date.
--   created        — Creation date (dcterms:created). ISO date.
--   full_path      — Computed hierarchical path (walked from broader chain)
--   hierarchy_level — Computed depth from root
--   ingested_at    — When our system last processed this record
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_unesco (
    concept_uri VARCHAR PRIMARY KEY,
    pref_label_en VARCHAR NOT NULL,
    pref_label_fr VARCHAR,
    pref_label_es VARCHAR,
    pref_label_ru VARCHAR,
    pref_label_ar VARCHAR,
    pref_label_zh VARCHAR,
    definition_en TEXT,
    scope_note_en TEXT,
    notation VARCHAR,
    alt_labels_en VARCHAR[],
    hidden_labels VARCHAR[],
    broader_uris VARCHAR[],
    narrower_uris VARCHAR[],
    related_uris VARCHAR[],
    exact_match VARCHAR[],
    close_match VARCHAR[],
    broad_match VARCHAR[],
    narrow_match VARCHAR[],
    in_scheme VARCHAR[],
    top_concept_of VARCHAR[],
    modified VARCHAR,
    created VARCHAR,
    full_path VARCHAR,
    hierarchy_level INTEGER,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""

RAW_OPENALEX = """
-- ============================================================
-- raw_openalex: OpenAlex topic/concept taxonomy
-- ============================================================
-- Source: https://api.openalex.org/
-- Format: JSON REST API with cursor pagination
-- Coverage: 4 domains, 26 fields, 254 subfields, ~4,500 topics,
--           ~26,000 keywords
-- Update frequency: Continuous (OpenAlex updates weekly)
--
-- FIELD DICTIONARY:
--   openalex_id    — OpenAlex entity ID, e.g., "T12345" for topic,
--                     "https://openalex.org/domains/1" for domain.
--                     Primary key.
--   display_name   — Human-readable name (e.g., "Artificial Intelligence")
--   description    — Brief description of the topic/field. Most domains and
--                     fields have descriptions. Topics may not.
--   entity_type    — One of: domain, field, subfield, topic, keyword
--   works_count    — NUMBER OF PAPERS tagged with this topic. This is
--                     enormously valuable — tells you which topics actually
--                     matter in the literature vs. dead taxonomy entries.
--                     Ranges from 0 to millions.
--   cited_by_count — Total citations across all works in this topic.
--                     Proxy for topic importance/impact.
--   updated_date   — When OpenAlex last updated this entity (ISO date).
--                     Topics update as new papers are classified.
--   parent_id      — OpenAlex ID of parent entity.
--                     domain→None, field→domain, subfield→field, topic→subfield
--   parent_name    — Display name of parent (denormalized for convenience)
--   domain_id      — Domain ancestor ID (for quick domain-level queries)
--   domain_name    — Domain ancestor name
--   field_id       — Field ancestor ID (null for domains)
--   field_name     — Field ancestor name
--   subfield_id    — Subfield ancestor ID (null for domains/fields)
--   subfield_name  — Subfield ancestor name
--   siblings_json  — JSON array of sibling entity IDs (same parent).
--                     From API response.
--   keywords_json  — JSON array of keyword objects associated with this topic.
--                     Each has {keyword, display_name, works_count}.
--                     Only present for topics (level 3).
--   full_path      — Computed path: "domain > field > subfield > topic"
--   hierarchy_level — 0=domain, 1=field, 2=subfield, 3=topic, 4=keyword
--   ingested_at    — When our system last fetched this record
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_openalex (
    openalex_id VARCHAR PRIMARY KEY,
    display_name VARCHAR NOT NULL,
    description TEXT,
    entity_type VARCHAR NOT NULL,
    works_count BIGINT,
    cited_by_count BIGINT,
    updated_date VARCHAR,
    parent_id VARCHAR,
    parent_name VARCHAR,
    domain_id VARCHAR,
    domain_name VARCHAR,
    field_id VARCHAR,
    field_name VARCHAR,
    subfield_id VARCHAR,
    subfield_name VARCHAR,
    siblings_json JSON,
    keywords_json JSON,
    full_path VARCHAR,
    hierarchy_level INTEGER,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""

RAW_NCBI = """
-- ============================================================
-- raw_ncbi: NCBI Taxonomy (filtered to Order rank)
-- ============================================================
-- Source: https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz
-- Format: Pipe-delimited dump files (nodes.dmp, names.dmp)
-- Coverage: ~1,200 nodes (superkingdom through order)
--           Full dump has 2.4M+ but we cap at order rank
-- Update frequency: Daily (but high-level ranks rarely change)
--
-- FIELD DICTIONARY:
--   tax_id         — NCBI taxonomy ID (integer, primary key).
--                     Globally unique, stable, widely referenced in
--                     GenBank, PubMed, UniProt, etc.
--   scientific_name — Formal taxonomic name (e.g., "Homo", "Primates",
--                     "Eukaryota"). From names.dmp where name_class =
--                     "scientific name".
--   rank           — Taxonomic rank: superkingdom, kingdom, phylum,
--                     class, order. Our filter — original dump has 40+
--                     rank types including "no rank", "clade", etc.
--   parent_tax_id  — Direct parent in NCBI's full tree. May point to
--                     a "no rank" intermediate we've filtered out.
--   filtered_parent_id — Parent after skipping filtered-out ranks.
--                     This is the parent_id we use in the unified schema.
--                     Connects to the nearest ancestor that's in our
--                     rank filter (superkingdom-order).
--   division_id    — NCBI division code. Maps to divisions like:
--                     0=Bacteria, 1=Invertebrates, 2=Mammals, 3=Phages,
--                     4=Plants, 5=Primates, 6=Rodents, 7=Synthetic,
--                     8=Unassigned, 9=Viruses, 10=Vertebrates, 11=Environmental
--   genetic_code   — Standard genetic code ID used by this taxon.
--                     1=Standard, 2=Vertebrate Mitochondrial, etc.
--                     Relevant for bioinformatics tools.
--   mito_genetic_code — Mitochondrial genetic code ID
--   synonyms       — Array of synonym names from names.dmp
--                     (name_class = "synonym")
--   common_names   — Array of common/vernacular names from names.dmp
--                     (name_class = "genbank common name" or "common name")
--   other_names    — Array of other name types (includes, equivalent names,
--                     authority names, etc.)
--   lineage        — Full taxonomic lineage string from the dump.
--                     e.g., "Eukaryota; Metazoa; Chordata; Mammalia; Primates"
--   full_path      — Our computed path using filtered hierarchy
--   hierarchy_level — 0=superkingdom, 1=kingdom, 2=phylum, 3=class, 4=order
--   ingested_at    — When our system last processed this record
--
-- NOTE: NCBI dump does NOT include definitions or descriptions.
--       Could be enriched via Entrez API (efetch) in a future pass,
--       but that's 1,200+ API calls with NCBI's rate limits (3/sec
--       without API key, 10/sec with).
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_ncbi (
    tax_id INTEGER PRIMARY KEY,
    scientific_name VARCHAR NOT NULL,
    rank VARCHAR NOT NULL,
    parent_tax_id INTEGER,
    filtered_parent_id INTEGER,
    division_id INTEGER,
    genetic_code INTEGER,
    mito_genetic_code INTEGER,
    synonyms VARCHAR[],
    common_names VARCHAR[],
    other_names VARCHAR[],
    lineage TEXT,
    full_path VARCHAR,
    hierarchy_level INTEGER,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""

RAW_LOC = """
-- ============================================================
-- raw_loc: Library of Congress Subject Headings (Science & Tech)
-- ============================================================
-- Source: https://id.loc.gov/authorities/subjects/
-- Format: JSON-LD via Linked Data API
-- Coverage: Science (sh85118553) and Technology (sh85133067)
--           branches. ~2,000-5,000 subjects depending on depth.
-- Update frequency: Continuous (LoC updates as catalogers work)
--
-- FIELD DICTIONARY:
--   loc_id         — LoC subject heading ID (e.g., "sh85118553").
--                     Primary key. Stable identifier used in MARC records
--                     worldwide.
--   auth_label     — Authoritative heading (madsrdf:authoritativeLabel).
--                     The "official" form of the subject heading.
--                     e.g., "Science", "Physics--Study and teaching"
--   pref_label     — Preferred label (skos:prefLabel). Usually same as
--                     auth_label but sometimes simplified.
--   scope_note     — Usage guidance for catalogers (skos:note or
--                     madsrdf:editorialNote). Explains when to use this
--                     heading vs. a related one.
--   definition     — Definition if available (skos:definition).
--                     Rare in LCSH — scope notes are more common.
--   broader_ids    — Array of broader subject IDs (madsrdf:hasBroaderAuthority
--                     or skos:broader). LCSH is polyhierarchical — a subject
--                     can have multiple broader terms.
--   narrower_ids   — Array of narrower subject IDs.
--   related_ids    — Array of related subject IDs (see-also references).
--                     Non-hierarchical associations.
--   variants       — Array of variant forms (madsrdf:hasVariant).
--                     Includes: UF (used-for) references, earlier forms,
--                     alternate spellings. Each may have a qualifier.
--                     e.g., ["Astrophysics", "Physical sciences"]
--   variant_types  — Array of variant type codes (parallel to variants).
--                     e.g., ["UF", "BT", "NT"]
--   subdivisions   — Array of applicable subdivisions.
--                     LCSH uses "--" for subdivisions:
--                     "Physics--Study and teaching"
--                     "Chemistry--Experiments"
--   lcc_class      — Library of Congress Classification number if linked.
--                     e.g., "QC" for Physics, "QD" for Chemistry.
--   source_vocab   — Source vocabulary identifier from LoC
--                     (usually "lcsh" for subject headings)
--   marc_field     — MARC field code (typically "150" for topical subjects)
--   full_path      — Computed hierarchical path from broader chain
--   hierarchy_level — Computed depth from Science/Technology root
--   crawl_depth    — How deep in our recursive crawl this was found
--                     (may differ from hierarchy_level due to API structure)
--   ingested_at    — When our system last fetched this record
--
-- NOTE: LoC API is rate-limited and slow (~200ms minimum between requests).
--       Full Science+Technology crawl may take 15-30 minutes.
--       Responses are cached via requests-cache (7-day TTL).
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_loc (
    loc_id VARCHAR PRIMARY KEY,
    auth_label VARCHAR NOT NULL,
    pref_label VARCHAR,
    scope_note TEXT,
    definition TEXT,
    broader_ids VARCHAR[],
    narrower_ids VARCHAR[],
    related_ids VARCHAR[],
    variants VARCHAR[],
    variant_types VARCHAR[],
    subdivisions VARCHAR[],
    lcc_class VARCHAR,
    source_vocab VARCHAR,
    marc_field VARCHAR,
    full_path VARCHAR,
    hierarchy_level INTEGER,
    crawl_depth INTEGER,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""

RAW_DOE_OSTI = """
-- ============================================================
-- raw_doe_osti: DOE OSTI Subject Categories
-- ============================================================
-- Source: https://www.osti.gov/
-- Format: API (unreliable) or hardcoded fallback list
-- Coverage: ~45 subject categories in 9 groups
-- Update frequency: Rarely (last major revision ~2010s)
--
-- FIELD DICTIONARY:
--   category_code  — Two-digit numeric code (e.g., "01", "42", "99").
--                     Primary key. These codes appear in DOE technical
--                     reports and are used for document classification.
--   category_name  — Full category name
--                     (e.g., "Coal, Lignite, and Peat", "Nuclear Physics")
--   description    — Prose description of what the category covers.
--                     Lists specific sub-topics and scope.
--   group_code     — Our assigned group identifier
--                     (e.g., "FOSSIL", "NUCLEAR", "BIO_ENV")
--   group_name     — Human-readable group name
--                     (e.g., "Fossil Energy", "Nuclear Science", "Biology & Environment")
--   group_description — Description of the group's scope
--   related_codes  — Array of other category codes that frequently
--                     co-occur on DOE documents. From our analysis, not
--                     from OSTI directly.
--   doe_program    — DOE program office primarily responsible
--                     (e.g., "Office of Science", "NNSA", "EERE").
--                     Approximate — many categories span programs.
--   active         — Whether this category is still actively used
--                     for new document classification. Some legacy codes
--                     exist but are rarely assigned.
--   full_path      — "group_name > category_name"
--   hierarchy_level — 0 for groups, 1 for categories
--   ingested_at    — When our system last processed this record
--
-- NOTE: DOE OSTI's subject categories are a flat classification system,
--       not a deep hierarchy. Groups are our own organization layer.
--       The real value of OSTI comes when we link publications to these
--       categories — each DOE report is tagged with 1-3 category codes.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_doe_osti (
    category_code VARCHAR PRIMARY KEY,
    category_name VARCHAR NOT NULL,
    description TEXT,
    group_code VARCHAR,
    group_name VARCHAR,
    group_description TEXT,
    related_codes VARCHAR[],
    doe_program VARCHAR,
    active BOOLEAN DEFAULT true,
    full_path VARCHAR,
    hierarchy_level INTEGER,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""

RAW_MESH = """
-- ============================================================
-- raw_mesh: NIH Medical Subject Headings (MeSH)
-- ============================================================
-- Source: https://nlm.nih.gov/databases/download/mesh.html
-- Format: XML descriptor dump (desc2026.gz), parsed to JSON
-- Coverage: ~31,110 descriptors across 16 MeSH categories
-- Update frequency: Annual (major), weekly (minor revisions)
--
-- FIELD DICTIONARY:
--   ui             — MeSH Unique Identifier (e.g., "D000001"). PK.
--   heading        — Preferred term / descriptor name.
--   tree_numbers   — Array of MeSH tree numbers defining position(s)
--                    in the hierarchy. A descriptor can appear in
--                    multiple trees (polyhierarchy). Format: "X01.234.567"
--                    where X = category letter.
--   scope_note     — Official definition/usage guidance from NLM.
--                    97% fill rate. Often multi-sentence.
--   entries        — Array of entry terms (synonyms, alternate forms).
--                    Avg 7.6 per descriptor. Valuable for fuzzy matching.
--   mesh_category  — Top-level MeSH category letter + name:
--                    A=Anatomy, B=Organisms, C=Diseases,
--                    D=Chemicals/Drugs, E=Techniques/Equipment,
--                    F=Psychiatry/Psychology, G=Phenomena/Processes,
--                    H=Disciplines, I=Anthropology/Education,
--                    J=Technology/Industry/Agriculture, K=Humanities,
--                    L=Information Science, M=Named Groups,
--                    N=Health Care, V=Publication Characteristics,
--                    Z=Geographicals
--   tree_depth     — Minimum depth across all tree_numbers (0=category root)
--   parent_uis     — Array of parent descriptor UIs derived from tree_numbers
--   ingested_at    — When ingested
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_mesh (
    ui VARCHAR PRIMARY KEY,
    heading VARCHAR NOT NULL,
    tree_numbers VARCHAR[],
    scope_note TEXT,
    entries VARCHAR[],
    mesh_category VARCHAR,
    tree_depth INTEGER,
    parent_uis VARCHAR[],
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mesh_heading ON raw_mesh(heading);
CREATE INDEX IF NOT EXISTS idx_mesh_category ON raw_mesh(mesh_category);
CREATE INDEX IF NOT EXISTS idx_mesh_depth ON raw_mesh(tree_depth);
"""

# =============================================================================
# CURATED LAYER — Unified Cross-Taxonomy Table
# =============================================================================

UNIFIED_KEYWORDS = """
-- ============================================================
-- keywords: Unified cross-taxonomy keyword table
-- ============================================================
-- Populated by transforming raw_* tables into a common schema.
-- This is the table used for cross-taxonomy queries, the
-- association engine, and the graph builder.
--
-- FIELD DICTIONARY:
--   id         — Unique identifier from source system. Format varies:
--                 NASA=UUID, UNESCO=URI, OpenAlex=ID, NCBI=tax_id,
--                 LoC=sh-ID, OSTI=category_code
--   label      — Primary human-readable keyword name
--   definition — Official definition or scope note (may be NULL)
--   parent_id  — ID of direct parent in hierarchy (NULL for roots)
--   source     — Which pillar: "NASA GCMD", "UNESCO Thesaurus",
--                 "OpenAlex", "NCBI Taxonomy", "Library of Congress",
--                 "DOE OSTI"
--   type       — Category within source (e.g., "sciencekeywords",
--                 "domain", "order", "subject_heading")
--   uri        — Persistent link to source system
--   full_path  — Full hierarchical path with " > " separator
--   aliases    — Array of alternative names, abbreviations, synonyms
--   level      — Integer depth in hierarchy (0 = root)
--   cross_refs — Array of IDs in OTHER sources that map to this concept.
--                 Populated in Phase 2.5 (alignment).
--   last_updated — When this record was last ingested/updated
--   version    — Source system version (e.g., GCMD "23.7")
--
-- COMPOSITE PRIMARY KEY: (id, source) because IDs are only unique
-- within a source, not globally.
-- ============================================================

CREATE TABLE IF NOT EXISTS keywords (
    id VARCHAR NOT NULL,
    label VARCHAR NOT NULL,
    definition TEXT,
    parent_id VARCHAR,
    source VARCHAR NOT NULL,
    type VARCHAR,
    uri VARCHAR,
    full_path VARCHAR,
    aliases VARCHAR[],
    level INTEGER,
    cross_refs VARCHAR[],
    last_updated TIMESTAMP WITH TIME ZONE,
    version VARCHAR,
    PRIMARY KEY (id, source)
);

CREATE INDEX IF NOT EXISTS idx_keywords_source ON keywords(source);
CREATE INDEX IF NOT EXISTS idx_keywords_label ON keywords(label);
CREATE INDEX IF NOT EXISTS idx_keywords_parent ON keywords(parent_id);
"""

# =============================================================================
# CROSS-TAXONOMY ALIGNMENT TABLE
# =============================================================================

ALIGNMENT_TABLE = """
-- ============================================================
-- cross_taxonomy_alignment: Maps equivalent concepts across sources
-- ============================================================
-- Populated in Phase 2.5 via:
--   1. UNESCO exactMatch/closeMatch (free from RDF data)
--   2. Embedding similarity (sentence-transformers)
--   3. Manual mapping for top ~200 nodes
--   4. LLM spot-check for ambiguous cases
--
-- FIELD DICTIONARY:
--   source_id      — ID from source taxonomy
--   source_name    — Source taxonomy name (e.g., "NASA GCMD")
--   target_id      — ID from target taxonomy
--   target_name    — Target taxonomy name (e.g., "UNESCO Thesaurus")
--   match_type     — How this alignment was established:
--                     "exact" = identical meaning (skos:exactMatch or manual)
--                     "close" = similar but not identical (skos:closeMatch)
--                     "broad" = target is broader than source
--                     "narrow" = target is narrower than source
--                     "related" = associative link, not hierarchical
--   confidence     — Float 0.0-1.0. Confidence in the alignment:
--                     1.0 = from authoritative source (UNESCO exactMatch)
--                     0.9+ = manual human mapping
--                     0.7-0.9 = high embedding similarity
--                     0.5-0.7 = moderate similarity, needs review
--                     <0.5 = weak, flagged for verification
--   method         — How the alignment was generated:
--                     "skos_match" = from UNESCO/LoC SKOS data
--                     "embedding" = sentence-transformer cosine similarity
--                     "manual" = human-curated mapping
--                     "llm" = LLM-assisted mapping
--   source_label   — Denormalized label from source (for quick display)
--   target_label   — Denormalized label from target
--   reviewed       — Boolean. Has a human reviewed this alignment?
--   review_note    — Reviewer's note on why accepted/rejected
--   created_at     — When this alignment was first created
--   updated_at     — When this alignment was last modified
-- ============================================================

CREATE TABLE IF NOT EXISTS cross_taxonomy_alignment (
    source_id VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    target_id VARCHAR NOT NULL,
    target_name VARCHAR NOT NULL,
    match_type VARCHAR NOT NULL,
    confidence FLOAT,
    method VARCHAR NOT NULL,
    source_label VARCHAR,
    target_label VARCHAR,
    reviewed BOOLEAN DEFAULT false,
    review_note TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (source_id, source_name, target_id, target_name)
);

CREATE INDEX IF NOT EXISTS idx_align_source ON cross_taxonomy_alignment(source_name, source_id);
CREATE INDEX IF NOT EXISTS idx_align_target ON cross_taxonomy_alignment(target_name, target_id);
CREATE INDEX IF NOT EXISTS idx_align_confidence ON cross_taxonomy_alignment(confidence);
CREATE INDEX IF NOT EXISTS idx_align_method ON cross_taxonomy_alignment(method);
"""

# =============================================================================
# WOS PUBLICATION DATA — Staging Tables
# =============================================================================

RAW_WOS_PUBLICATIONS = """
-- ============================================================
-- raw_wos_publications: Web of Science publication metadata
-- ============================================================
-- Source: Victor's WoS export (Tab 1: "keywords with Pub IDS")
-- Format: Excel (.xlsx), comma-delimited multi-value fields
-- Coverage: ~6,000 DOE/NETL-related publications
-- Update frequency: Manual batch export
--
-- FIELD DICTIONARY:
--   accession_number   — WoS unique ID (e.g., "WOS:001409639600386"). PK.
--   keywords_author    — Author-assigned keywords, stored as array.
--                        Original: comma-delimited string.
--                        45% fill rate. 7,765 unique across dataset.
--   keywords_plus      — WoS algorithmically-derived keywords, stored as array.
--                        Original: comma-delimited string, ALL CAPS.
--                        66% fill rate. 7,554 unique.
--   subject_sub_heading_1 — WoS broad heading: Technology, Physical Sciences,
--                          or Life Sciences & Biomedicine. 99% fill.
--   subject_sub_heading_2 — Secondary heading. 30% fill.
--   subject_cat_traditional_1 — WoS traditional category (91 unique).
--                              e.g., "Chemistry, Multidisciplinary", "Energy & Fuels"
--   subject_cat_traditional_2 — Secondary traditional category. 57% fill.
--   subject_cat_extended — Extended categories as array. Comma-delimited.
--                         Coarser than traditional (61 unique).
--   category_heading_1 — Top-level: "Science & Technology" or "Social Sciences"
--   category_heading_2 — Secondary. 0.2% fill (nearly unused).
--   abstract           — Full abstract text. 78% fill.
--                        Richest source for NLP keyword extraction.
--   source_title       — Journal or conference name. 1,201 unique.
--   title              — Publication title. May contain HTML entities.
--   doc_type_1         — Primary document type (Article, Proceedings, etc.)
--   doc_type_2         — Secondary type. 5% fill.
--   grant_agencies     — Funding agencies as array. Comma-delimited.
--                        Needs entity resolution (DOE appears as 4+ variants).
--   data_acquired      — When this record was exported from WoS.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_wos_publications (
    accession_number VARCHAR PRIMARY KEY,
    keywords_author VARCHAR[],
    keywords_plus VARCHAR[],
    subject_sub_heading_1 VARCHAR,
    subject_sub_heading_2 VARCHAR,
    subject_cat_traditional_1 VARCHAR,
    subject_cat_traditional_2 VARCHAR,
    subject_cat_extended VARCHAR[],
    category_heading_1 VARCHAR,
    category_heading_2 VARCHAR,
    abstract TEXT,
    source_title VARCHAR,
    title VARCHAR,
    doc_type_1 VARCHAR,
    doc_type_2 VARCHAR,
    grant_agencies VARCHAR[],
    data_acquired TIMESTAMP,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wos_pub_source ON raw_wos_publications(source_title);
CREATE INDEX IF NOT EXISTS idx_wos_pub_cat ON raw_wos_publications(subject_cat_traditional_1);
"""

RAW_WOS_KEYWORDS_PLUS = """
-- ============================================================
-- raw_wos_keywords_plus_vocab: Deduplicated Keywords Plus vocabulary
-- ============================================================
-- Source: Victor's WoS export (Tab 2: "Keywords 2")
-- Format: Single-column flat list, ALL CAPS
-- Coverage: 7,488 unique Keywords Plus terms
-- Note: Appears to be from a broader/different pub set than Tab 1.
--       No publication linkage — vocabulary only.
--
-- FIELD DICTIONARY:
--   keyword        — The Keywords Plus term, ALL CAPS. PK.
--   normalized     — Lowercase, trimmed, artifacts cleaned.
--   ingested_at    — When ingested.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_wos_keywords_plus_vocab (
    keyword VARCHAR PRIMARY KEY,
    normalized VARCHAR NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""

RAW_WOS_NETL_TECH = """
-- ============================================================
-- raw_wos_netl_tech: NETL organizational/technology taxonomy
-- ============================================================
-- Source: Victor's WoS export (Tab 3: "Keywords 3")
-- Format: Excel with 8 columns defining NETL program hierarchy
-- Coverage: ~3,877 publications mapped to NETL tech areas
-- Update frequency: Changes with NETL reorganizations
--
-- This table captures a POINT-IN-TIME snapshot of NETL's org
-- structure. The hierarchy: Program Area (9) → Sub-Program (27)
-- → Technology Area (64) → Turbines Sub-Tech (6).
-- Future reorgs should create new version rows, not overwrite.
--
-- FIELD DICTIONARY:
--   article_title     — Publication title (join key to Tab 1 via fuzzy match).
--                       Only 329 exact matches with raw_wos_publications.
--   technology_area   — Finest NETL technology classification (64 unique).
--                       e.g., "Post-Combustion Capture", "Advanced Turbines"
--   program_area      — Top-level NETL program (9 unique).
--                       e.g., "H2 with Carbon Management", "Carbon Transport & Storage"
--   sub_program_area  — Mid-level program division (27 unique).
--                       e.g., "Advanced Energy Systems", "Carbon Storage"
--   technology_area_alt — Alternative technology grouping (13 unique).
--                        54% are "#N/A". Seems to be older/parallel classification.
--   consolidated_tech_area — Consolidated technology area (63 unique).
--                           Nearly identical to technology_area.
--   consolidated_tech_filter — Filter version of consolidated (63 unique).
--                             Same values as consolidated_tech_area.
--   turbines_sub_tech — Sub-technology for turbine publications only (6 unique).
--                       8% fill. e.g., "Low Emissions Combustion", "Supercritical CO2"
--   org_version       — Version tag for this org structure snapshot.
--                       Default "NETL_pre2026" for this initial load.
--   ingested_at       — When ingested.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_wos_netl_tech (
    article_title VARCHAR NOT NULL,
    technology_area VARCHAR,
    program_area VARCHAR,
    sub_program_area VARCHAR,
    technology_area_alt VARCHAR,
    consolidated_tech_area VARCHAR,
    consolidated_tech_filter VARCHAR,
    turbines_sub_tech VARCHAR,
    org_version VARCHAR DEFAULT 'NETL_pre2026',
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_netl_tech_prog ON raw_wos_netl_tech(program_area);
CREATE INDEX IF NOT EXISTS idx_netl_tech_area ON raw_wos_netl_tech(technology_area);
CREATE INDEX IF NOT EXISTS idx_netl_tech_sub ON raw_wos_netl_tech(sub_program_area);
"""

RAW_WOS_NATLAB = """
-- ============================================================
-- raw_wos_natlab_publications: WoS pubs from other national labs
-- ============================================================
-- Source: Victor's WoS export (other national labs, non-NETL)
-- Format: Excel (.xlsx), single sheet, 21 columns
-- Coverage: ~227K publications across national lab system
--
-- FIELD DICTIONARY:
--   accession_number   — WoS unique ID. PK.
--   keywords_author    — Author-assigned keywords, array.
--   keywords_plus      — WoS-derived keywords, array.
--   category_heading_1 — Top-level WoS heading (Science & Technology, etc.)
--   doc_type_1         — Primary document type.
--   subject_cat_traditional_1 — WoS traditional category.
--   subject_sub_heading_1 — WoS sub-heading.
--   category_heading_2 — Secondary heading.
--   doc_type_2         — Secondary document type.
--   subject_cat_traditional_2 — Secondary traditional category.
--   subject_sub_heading_2 — Secondary sub-heading.
--   doc_type_3         — Tertiary document type.
--   abstract           — Full abstract text.
--   category           — WoS category field.
--   publisher          — Publisher full name.
--   source_title       — Journal/conference name.
--   sub_category       — Sub-category field.
--   subject_category   — Subject category field.
--   subject_cat_extended — Extended subject categories.
--   table_names        — Table names field.
--   title              — Publication title.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_wos_natlab_publications (
    accession_number VARCHAR PRIMARY KEY,
    keywords_author VARCHAR[],
    keywords_plus VARCHAR[],
    category_heading_1 VARCHAR,
    doc_type_1 VARCHAR,
    subject_cat_traditional_1 VARCHAR,
    subject_sub_heading_1 VARCHAR,
    category_heading_2 VARCHAR,
    doc_type_2 VARCHAR,
    subject_cat_traditional_2 VARCHAR,
    subject_sub_heading_2 VARCHAR,
    doc_type_3 VARCHAR,
    abstract TEXT,
    category VARCHAR,
    publisher VARCHAR,
    source_title VARCHAR,
    sub_category VARCHAR,
    subject_category VARCHAR,
    subject_cat_extended VARCHAR,
    table_names VARCHAR,
    title VARCHAR,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wos_natlab_cat ON raw_wos_natlab_publications(subject_cat_traditional_1);
CREATE INDEX IF NOT EXISTS idx_wos_natlab_source ON raw_wos_natlab_publications(source_title);
"""

# =============================================================================
# ONTOLOGY LAYER — Multi-Perspective Keyword Ontology
# =============================================================================

DISCIPLINES = """
-- ============================================================
-- disciplines: The 14 primary scientific disciplines
-- ============================================================
-- These are the "big swaths" users look through. Each keyword
-- sense maps to one or more disciplines. Lenses weight these.
--
-- FIELD DICTIONARY:
--   discipline_id  — Short stable identifier (e.g., "fossil_energy")
--   name           — Human-readable name
--   description    — What this discipline covers
--   parent_id      — For sub-disciplines (NULL for top-level 14)
--   tier           — Resolution tier: 1=DOE core, 2=DOE adjacent,
--                    3=other national lab, 4=broad external
--   sort_order     — Display ordering
-- ============================================================

CREATE TABLE IF NOT EXISTS disciplines (
    discipline_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    parent_id VARCHAR,
    tier INTEGER NOT NULL,
    sort_order INTEGER
);
"""

KEYWORD_SENSES = """
-- ============================================================
-- keyword_senses: One row per keyword per context
-- ============================================================
-- The core of the vector bundle model. A keyword label like
-- "plasma" has multiple senses — one per source/discipline
-- context where it carries distinct meaning.
--
-- FIELD DICTIONARY:
--   sense_id         — Unique ID: "{keyword_id}@{source}#{n}"
--   keyword_id       — FK to keywords.id
--   keyword_source   — FK to keywords.source (composite FK with keyword_id)
--   keyword_label    — Denormalized label for fast display
--   origin_source    — Which pillar/dataset this sense comes from
--   origin_path      — Hierarchical path in origin source
--   origin_level     — Depth in origin hierarchy
--   discipline_primary — Primary discipline this sense belongs to
--   disciplines_secondary — Additional disciplines (for cross-discipline senses)
--   resolution_tier  — 1-4, inherited from discipline or overridden
--   definition_in_context — What this keyword means in THIS context
--   scope_note       — Usage guidance for disambiguation
--   disambiguation   — Short tag distinguishing this sense from others
--                      e.g., "plasma (physics)" vs "plasma (biology)"
--   relevance_tags   — Freeform tags for lens filtering
--   confidence       — 0-1, how certain we are about this sense assignment
--   provenance       — How this sense was created (expert, label_match, nlp, manual)
--   created_at       — When created
-- ============================================================

CREATE TABLE IF NOT EXISTS keyword_senses (
    sense_id VARCHAR PRIMARY KEY,
    keyword_id VARCHAR NOT NULL,
    keyword_source VARCHAR NOT NULL,
    keyword_label VARCHAR NOT NULL,
    origin_source VARCHAR NOT NULL,
    origin_path VARCHAR,
    origin_level INTEGER,
    discipline_primary VARCHAR,
    disciplines_secondary VARCHAR[],
    resolution_tier INTEGER,
    definition_in_context TEXT,
    scope_note TEXT,
    disambiguation VARCHAR,
    relevance_tags VARCHAR[],
    confidence FLOAT,
    provenance VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sense_keyword ON keyword_senses(keyword_id, keyword_source);
CREATE INDEX IF NOT EXISTS idx_sense_label ON keyword_senses(keyword_label);
CREATE INDEX IF NOT EXISTS idx_sense_discipline ON keyword_senses(discipline_primary);
CREATE INDEX IF NOT EXISTS idx_sense_tier ON keyword_senses(resolution_tier);
"""

SENSE_RELATIONSHIPS = """
-- ============================================================
-- sense_relationships: Directed typed edges between senses
-- ============================================================
-- These are the "association vectors" in the vector bundle.
-- Each edge connects two specific senses (not raw keywords)
-- with a typed, directed relationship.
--
-- FIELD DICTIONARY:
--   source_sense_id     — FK to keyword_senses
--   target_sense_id     — FK to keyword_senses
--   relationship_type   — Edge type:
--     equivalent_sense  — same meaning in different sources
--     cross_domain_bridge — connects different disciplines
--     method_for        — source is a method used in target's domain
--     measured_by       — source is measured/characterized by target
--     applied_in        — source technique applied in target domain
--     policy_governs    — source policy controls target activity
--     enables           — source enables or is prerequisite for target
--     competes_with     — source and target are alternative approaches
--     conflated_with    — commonly confused but distinct
--     subtopic_of       — finer grain of target
--     related_to        — general association
--   direction           — "toward" (same discipline), "across" (cross-discipline),
--                         "away" (disambiguation)
--   confidence          — 0-1
--   provenance          — How established (expert, alignment, nlp, manual)
--   lens_contexts       — Which lenses this relationship is most relevant for
-- ============================================================

CREATE TABLE IF NOT EXISTS sense_relationships (
    source_sense_id VARCHAR NOT NULL,
    target_sense_id VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL,
    direction VARCHAR,
    confidence FLOAT,
    provenance VARCHAR,
    lens_contexts VARCHAR[],
    PRIMARY KEY (source_sense_id, target_sense_id, relationship_type)
);

CREATE INDEX IF NOT EXISTS idx_srel_source ON sense_relationships(source_sense_id);
CREATE INDEX IF NOT EXISTS idx_srel_target ON sense_relationships(target_sense_id);
CREATE INDEX IF NOT EXISTS idx_srel_type ON sense_relationships(relationship_type);
"""

HIERARCHY_ENVELOPES = """
-- ============================================================
-- hierarchy_envelopes: Versioned org/tech hierarchy structures
-- ============================================================
-- Any parent-child structure uploaded by a user becomes an
-- envelope. NETL's org chart, a technology taxonomy, a journal
-- classification — all stored here with versioning.
--
-- FIELD DICTIONARY:
--   envelope_id    — Unique ID for this node
--   envelope_name  — Which envelope this belongs to (e.g., "NETL_org")
--   version        — Version tag (e.g., "FY2025", "FY2026")
--   node_label     — Human-readable label for this node
--   parent_id      — Parent node in this envelope (NULL for roots)
--   level          — Depth (0=root)
--   full_path      — Hierarchy path with " > " separator
--   node_type      — What kind of node: "program", "sub_program",
--                    "technology_area", "sub_technology", "org_unit", etc.
--   metadata       — JSON blob for source-specific extra fields
--   active         — Whether this node exists in the current version
--   superseded_by  — If reorganized, points to replacement node
--   created_at     — When first ingested
--   retired_at     — When this node was removed from the hierarchy (NULL if active)
-- ============================================================

CREATE TABLE IF NOT EXISTS hierarchy_envelopes (
    envelope_id VARCHAR NOT NULL,
    envelope_name VARCHAR NOT NULL,
    version VARCHAR NOT NULL,
    node_label VARCHAR NOT NULL,
    parent_id VARCHAR,
    level INTEGER,
    full_path VARCHAR,
    node_type VARCHAR,
    metadata JSON,
    active BOOLEAN DEFAULT true,
    superseded_by VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    retired_at TIMESTAMP,
    PRIMARY KEY (envelope_id, envelope_name, version)
);

CREATE INDEX IF NOT EXISTS idx_envelope_name ON hierarchy_envelopes(envelope_name, version);
CREATE INDEX IF NOT EXISTS idx_envelope_parent ON hierarchy_envelopes(parent_id, envelope_name);
CREATE INDEX IF NOT EXISTS idx_envelope_type ON hierarchy_envelopes(node_type);
"""

ONTOLOGY_LENSES = """
-- ============================================================
-- ontology_lenses: Composed user perspective configurations
-- ============================================================
-- A lens = Role × Org × Discipline × Interest stack.
-- Each layer contributes weights that shape graph traversal.
-- Pre-built "hats" are lenses with is_template=true.
--
-- FIELD DICTIONARY:
--   lens_id          — Unique identifier
--   name             — Human-readable name (e.g., "NETL Director")
--   description      — What this lens represents
--   is_template      — True for pre-built hats, false for user-customized
--   altitude         — 100000 (executive), 10000 (program mgr), 1000 (researcher)
--   role_type        — Role layer: director, program_mgr, researcher,
--                      congressional_staffer, policy_analyst, etc.
--   org_envelope     — FK to hierarchy_envelopes.envelope_name (e.g., "NETL_org")
--   org_version      — Version of org envelope to use
--   org_node_id      — Specific node in org hierarchy (e.g., a division)
--   discipline_primary — Primary discipline FK
--   disciplines_secondary — Additional disciplines for intersection hats
--   discipline_weights — JSON: {"fossil_energy": 0.95, "materials": 0.6, ...}
--   interest_weights — JSON: {"budget": 0.8, "congressional": 0.7, ...}
--                      Up to 3 interest layers stacked here.
--   role_weights     — JSON: {"technical_detail": 0.3, "strategic": 0.9, ...}
--   created_at       — When created
--   created_by       — Who created (user ID or "system")
-- ============================================================

CREATE TABLE IF NOT EXISTS ontology_lenses (
    lens_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    is_template BOOLEAN DEFAULT false,
    altitude INTEGER,
    role_type VARCHAR,
    org_envelope VARCHAR,
    org_version VARCHAR,
    org_node_id VARCHAR,
    discipline_primary VARCHAR,
    disciplines_secondary VARCHAR[],
    discipline_weights JSON,
    interest_weights JSON,
    role_weights JSON,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    created_by VARCHAR DEFAULT 'system'
);

CREATE INDEX IF NOT EXISTS idx_lens_template ON ontology_lenses(is_template);
CREATE INDEX IF NOT EXISTS idx_lens_discipline ON ontology_lenses(discipline_primary);
CREATE INDEX IF NOT EXISTS idx_lens_role ON ontology_lenses(role_type);
"""

# =============================================================================
# ALL SCHEMAS — for init_all_tables()
# =============================================================================

ALL_SCHEMAS = [
    RAW_NASA_GCMD,
    RAW_UNESCO,
    RAW_OPENALEX,
    RAW_NCBI,
    RAW_LOC,
    RAW_DOE_OSTI,
    UNIFIED_KEYWORDS,
    ALIGNMENT_TABLE,
    RAW_MESH,
    RAW_WOS_PUBLICATIONS,
    RAW_WOS_KEYWORDS_PLUS,
    RAW_WOS_NETL_TECH,
    RAW_WOS_NATLAB,
    DISCIPLINES,
    KEYWORD_SENSES,
    SENSE_RELATIONSHIPS,
    HIERARCHY_ENVELOPES,
    ONTOLOGY_LENSES,
]


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL line comments (--) from schema text."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        if "--" in line:
            line = line[:line.index("--")]
        lines.append(line)
    return "\n".join(lines)


def init_all_tables(conn):
    """Create all tables in the database."""
    for schema in ALL_SCHEMAS:
        cleaned = _strip_sql_comments(schema)
        for stmt in cleaned.split(";"):
            stmt = stmt.strip()
            if not stmt:
                continue
            try:
                conn.execute(stmt)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"Schema error: {e}")
                    print(f"Statement: {stmt[:100]}...")
