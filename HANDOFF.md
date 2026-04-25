# Handoff — Session 7 Startup Prompt

Open Claude Code in `/home/victor/Desktop/Projects/keywords/` and paste everything below the line.

---

## Context

You're picking up the **Keyword Taxonomy Engine** — a universal scientific keyword taxonomy with vector bundle semantics. Read `primer.md` and `CLAUDE.md` first. Read `WORKLOG.md` for full history.

## Where we are (end of Session 6, 2026-04-25 2:30am EDT)

**Quality: 97.8/100 (A+). Lens query engine is live.**

```
421,848 senses
2,444,185 relationships (5.79 rels/sense)
142 true orphans (0.03%)
14,206 polysemous labels (3.5%)
7,983 OpenAlex pubs (7,761 with DOIs)
68,855 keyword-paper mappings
97 template lenses, 14 disciplines
4 lens query CLI commands operational
178 tests passing, 20 DuckDB tables
```

## What was built (Session 6)

1. **Agentic orphan resolution** — AI-curated reasoning on 6,678 orphans. 8-strategy pipeline: abbreviation expansion (200+ entries), PACS code detection, substring/contains matching, discipline anchoring. Result: 6,535 connected (with provenance `ai_reasoning:{strategy}`), 143 true orphans tagged. Orphan rate 1.6% → 0.03%.

2. **Lens query engine** — THE product feature. 4 CLI commands:
   - `lens` — query through composed Role × Discipline lens
   - `lens-explore` — follow keyword relationships through a lens
   - `lens-compare` — side-by-side multi-lens comparison (killer demo)
   - `lens-list` — discover 97 template lenses
   - Functions: `compose_lens()`, `query_through_lens()`, `explore_from_keyword()`, `compare_lenses()`, `list_lenses()` in `src/ontology.py`

3. **OpenAlex pub ingestion (PARTIAL)** — 7,983 pubs + 68,855 keyword-paper mappings loaded. Co-occurrence edges and new sense creation NOT completed (script killed during Python dict build). Tables created: `raw_openalex_publications`, `openalex_pub_keywords`.

4. **Quality scorecard** — 10 metrics, 97.8/100. Polysemy jumped to 3.5% after lab source tagging.

## What to do next (in order)

### Immediate (finish Session 6 incomplete work)
1. **OpenAlex co-occurrence edges** — build keyword co-occurrence from `openalex_pub_keywords` using SQL-native JOINs (NOT Python fetchall → dict — that approach killed the process last time). Something like:
   ```sql
   SELECT a.keyword_label, b.keyword_label, COUNT(*) as cooccur
   FROM openalex_pub_keywords a
   JOIN openalex_pub_keywords b ON a.openalex_id = b.openalex_id AND a.keyword_label < b.keyword_label
   WHERE a.relevance_score >= 0.3 AND b.relevance_score >= 0.3
   GROUP BY 1, 2 HAVING COUNT(*) >= 2
   ```
2. **Create senses for new OpenAlex keywords** not already in taxonomy. Filter `openalex_pub_keywords` labels against existing `keyword_senses`.

### Product features
3. **Paper discovery** — "show me papers about X through lens Y." Join pub-keyword mappings with lens-scored senses to rank papers.
4. **Interactive visualization** — even simple D3.js force-directed graph of lens-compare output would be powerful demo.

### Publication/release
5. **WoS DOI backfill** — user re-exports with DOI column → `ALTER TABLE ADD COLUMN doi VARCHAR` + `UPDATE` on `accession_number`. User can't do this right now, defer.
6. **Versioning + Zenodo DOI** — release tag, README, Zenodo submission for citable artifact.
7. **Methodology paper** — (1) vector bundle model, (2) 7-pillar unification, (3) lens composition, (4) AI curation layer, (5) benchmarks vs VOSviewer/CiteSpace.
8. **API** — REST or GraphQL for external tool integration.

## Errors to avoid (learned the hard way)

| Pattern | What happens | Fix |
|---|---|---|
| `NOT EXISTS` on sense_relationships | CPU burn forever (anti-join on 2.4M rows) | Use `LEFT JOIN` with CTE: `WITH connected AS (SELECT DISTINCT sid...)` |
| Python fetchall() for 422K rows → dict | Process killed (too slow/memory) | SQL-native JOINs instead |
| Python one-liners for DB queries | Gets backgrounded, locks conflict | Use `duckdb` CLI (v1.5.2 installed): `duckdb data/lake/keywords.duckdb -readonly -c "SQL"` |
| `executemany` with VARCHAR[] columns | Pathologically slow | Temp tables + `INSERT...SELECT` |
| Background Python scripts without `-u` | Zero output until exit (buffered stdout) | `python3 -u script.py` or `flush=True` |
| Multiple stale background queries | 100% CPU, lock conflicts | `ps aux \| grep 'duckdb\|python3' \| grep -v grep` then kill |

## Permanent rules

- ALWAYS be devil's advocate — challenge assumptions, no glass castles
- Audit between large steps for errors and gaps
- Push before destructive operations
- Use SQL-first for all DuckDB queries
- Save WORKLOG.md at natural breakpoints
- Keywords are vector bundles — same word, different meaning per context
- Cross-domain references are FIRST CLASS
