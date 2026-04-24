# Handoff Prompt — Paste This Into New Claude Code Session

Open Claude Code in `/home/victor/Desktop/Projects/keywords/` and paste everything below the line.

---

## Context

You're picking up an active project: the **Keyword Taxonomy Engine** — a universal scientific keyword taxonomy and cross-taxonomy alignment system. Think "VOSviewer x1000."

**Read `primer.md` and `CLAUDE.md` first.** They contain the full architecture, data lake stats, file structure, execution plan, known issues, and coding conventions.

## What's been done (Phases 1, 2, 2.1 — ALL COMPLETE)

- 6 authoritative data pillars ingested into DuckDB: NASA GCMD (4,849), UNESCO Thesaurus (4,408), OpenAlex (31,995), NCBI Taxonomy (3,044), Library of Congress LCSH (29,731), DOE OSTI (59) = **74,086 keywords** in the unified `keywords` table
- All 6 raw source tables populated with source-specific fields (46,889 raw records) — things like OpenAlex works_count, UNESCO French/Spanish/Russian labels, NCBI lineage and genetic codes, LoC broader/narrower/related arrays, NASA GCMD hierarchy columns
- 178 tests passing across 11 test files
- Grant number extractor exists (NSF, NIH, DOE, NASA, EU, EPSRC, DFG, NSERC patterns)
- GCMD enriched via per-concept JSON API (80% have definitions now)
- Full data quality audit done: 0 empty labels, 0 orphans, 0 PK violations

## What's next: Phase 2.5 — Multi-Expert Bayesian Cross-Taxonomy Alignment

This is the hard and creative part. Victor's design:

### The Multi-Hat Approach

Create **5 expert personas**, each viewing the 74K keywords through a different domain lens. Each expert independently designs the structural architecture for cross-taxonomy alignment. Then use **Bayesian agreement** across their 5 overlapping designs to settle on a consensus structure.

### The 5 Expert Hats

1. **Physical Sciences Expert** — Physics, chemistry, materials, energy, astronomy, earth science. Primary sources: NASA GCMD earth science hierarchy, DOE OSTI energy categories, OpenAlex physical sciences domain, NCBI molecular-level overlap.

2. **Life Sciences Expert** — Biology, ecology, taxonomy, medicine, genetics. Primary sources: NCBI taxonomic ranks, OpenAlex health/life sciences domains, LoC biological subject headings, UNESCO environmental science concepts.

3. **Applied Sciences & Engineering Expert** — Technology, engineering, computation, instrumentation. Primary sources: LoC Technology subtree, DOE OSTI engineering/materials categories, OpenAlex applied fields, NASA GCMD instruments/platforms.

4. **Information & Social Sciences Expert** — Knowledge organization, classification theory, social dimensions of science, policy. Primary sources: UNESCO thesaurus structure (education, social science, communication), LoC cross-references, OpenAlex social sciences domain, DOE OSTI policy categories.

5. **Cross-Domain Integrator** — Explicitly looks for concepts that span multiple domains. Interdisciplinary fields (bioinformatics, environmental engineering, science policy), boundary objects, methods that cross fields. Identifies the "tangles" where keyword nets from different taxonomies overlap and interweave.

### What each expert must produce

For each hat:
1. Examine all 6 source taxonomies through their domain lens
2. Propose top-level structural categories (aim for 15-30 per expert)
3. Map which source keywords belong to each category (with confidence 0-1)
4. Identify cross-references to concepts OUTSIDE their domain — these are critical
5. Document reasoning and confidence levels
6. **Save their analysis** — we need provenance of who decided what

### Bayesian Consensus Process

After all 5 experts produce their designs:
1. Weight each expert's structural proposals by their confidence scores
2. Find consensus clusters where 3+ experts agree on groupings
3. Handle disagreements explicitly — document them, don't sweep under the rug
4. Build the consensus taxonomy with cross-domain edges as FIRST CLASS citizens
5. Populate the `cross_taxonomy_alignment` table with: source_id, target_id, match_type, confidence, method (which expert(s) proposed it), review notes

### Critical requirement from Victor

> "I want that final structure to be built with cross domain and category references to other things in other areas based on all the tangles of keyword nets you surmise"

This means: the alignment isn't just "NASA term X = LoC term Y". It's a **web of cross-references** where a concept like "remote sensing" connects to instruments (NASA GCMD), earth observation technology (LoC), satellite platforms (NASA), environmental monitoring (UNESCO, NCBI ecology), signal processing (OpenAlex engineering), and energy applications (DOE OSTI). Every concept should have tendrils reaching into other domains where relevant.

## How to execute

1. Read `primer.md` and `CLAUDE.md`
2. Run `python -m pytest tests/ -v` to verify everything works
3. Run `python -m src.cli stats` to see current data lake state
4. Query the actual data to understand what's in each source:
   ```python
   import duckdb
   conn = duckdb.connect("data/lake/keywords.duckdb", read_only=True)
   # Explore root nodes per source
   conn.execute("SELECT source, label, level FROM keywords WHERE level = 0 ORDER BY source, label").fetchdf()
   # Explore a specific source's hierarchy
   conn.execute("SELECT label, full_path, level FROM keywords WHERE source = 'NASA GCMD' AND level <= 1 ORDER BY full_path").fetchdf()
   ```
5. Build the 5 expert analyses — use subagents in parallel if possible
6. Run Bayesian consensus
7. Write results to `cross_taxonomy_alignment` table
8. Update `cross_refs` field in unified `keywords` table

## Permanent rules (from Victor, apply to ALL sessions)

- **ALWAYS be devil's advocate** — challenge assumptions, no glass castles
- **Audit between large steps** for errors and gaps
- **Push before destructive operations** — always, no exceptions
- **Caveman mode** — terse responses, no filler, no preamble, no postamble. Full technical accuracy.
- When Victor corrects you, update CLAUDE.md with a rule that would have prevented the mistake

## Known gotchas

- UNESCO mirror (skos.um.es) strips exactMatch/closeMatch triples — the alignment harvester at `src/alignment/unesco_matches.py` returns 0 results. Need official UNESCO source or alternative approach.
- OpenAlex IDs (numeric) collide with NCBI tax_ids — handled by composite PK `(id, source)` but be aware when joining.
- LoC depth filter caps at level 6 (29,731 of 52,951 total subjects). Could increase if needed.
- Data is cached in `data/raw/` and database in `data/lake/` — both gitignored. If missing, run `python -m src.cli ingest` to re-download and re-ingest.
