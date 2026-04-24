# Keyword Taxonomy Engine — Project Instructions

## Project Overview
Universal scientific keyword taxonomy engine. 6 data pillars ingested into DuckDB (74K unified + 47K raw records). Building cross-taxonomy alignment via multi-expert Bayesian consensus.

## Key Commands
```bash
python -m src.cli ingest --sources nasa_gcmd unesco openalex ncbi loc doe_osti
python -m src.cli ingest --raw          # also populate raw source tables
python -m src.cli populate-raw          # populate raw tables only (from cached data)
python -m src.cli stats                 # show counts by source
python -m src.cli search "climate"      # search keywords
python -m src.cli enrich --source nasa_gcmd  # enrich GCMD with API definitions
python -m src.cli export                # export to parquet
python -m pytest tests/ -v              # run tests (178 passing)
```

## Architecture Rules
- **Two-layer schema**: raw tables (full source fidelity) + unified `keywords` table (cross-taxonomy queries)
- DuckDB is primary store (ACID). Parquet is export format.
- Parsers return unified-schema records. `raw_writers.py` builds raw-format records.
- `cross_taxonomy_alignment` table stores cross-references with confidence, method, provenance.
- Composite primary key `(id, source)` on unified table — IDs are only unique within source.

## Coding Conventions
- Python 3.14, type hints on function signatures
- Tests in `tests/` using pytest, each parser has its own test file
- No mocks for data format tests — use synthetic sample data (see test_loc_parser.py pattern)
- Source data cached in `data/raw/` (gitignored)
- DuckDB database at `data/lake/keywords.duckdb` (gitignored)

## Devil's Advocate Rule (PERMANENT)
- ALWAYS challenge assumptions before building on them
- Audit between large steps for errors and gaps
- No glass castles — verify data quality after every ingestion
- If something seems too easy, it probably is. Check.

## Data Sources
| Source | Parser | Raw Table | Key Gotchas |
|---|---|---|---|
| NASA GCMD | `parsers/nasa_gcmd.py` | `raw_nasa_gcmd` | Row 0=metadata, Row 1=headers. Variable CSV structure per keyword type. Two-pass for parent resolution. |
| UNESCO | `parsers/unesco.py` | `raw_unesco` | Mirror at skos.um.es strips match triples. Official UNESCO URL was 404. |
| OpenAlex | `parsers/openalex.py` | `raw_openalex` | Cursor-based pagination. Keywords extracted from topics. works_count is ranking signal. |
| NCBI | `parsers/ncbi.py` | `raw_ncbi` | Pipe-delimited `\t|\t` format. Capped at Order rank. Skips "no rank" intermediates. |
| LoC LCSH | `parsers/loc.py` | `raw_loc` | Bulk SKOS N-Triples (101MB gz). Stream-parsed, BFS from 2 root IDs. |
| DOE OSTI | `parsers/doe_osti.py` | `raw_doe_osti` | API unreliable, hardcoded fallback. 45 categories in 9 groups. |
