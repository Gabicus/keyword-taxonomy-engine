"""Ingest expanded national lab WoS data from Tableau Hyper extract.

Source: data/WoS/natlab_expanded/Other Labs Part 2.hyper
  - 255,849 rows, 61 columns (richer than the original 21-column Excel)
  - Comma-delimited keywords (not semicolons)
  - ~227K unique accession numbers (dupes from multi-row publisher entries)

Pipeline:
  1. Read Hyper via pantab
  2. Deduplicate on accession number (keep first)
  3. Parse keywords into arrays
  4. Write to parquet (data/raw/natlab_expanded.parquet)
  5. Load parquet into DuckDB raw_wos_natlab_expanded table
"""

import sys
import time
from pathlib import Path

import duckdb
import pantab
import pyarrow as pa

HYPER_PATH = Path("data/WoS/natlab_expanded/Other Labs Part 2.hyper")
PARQUET_PATH = Path("data/raw/natlab_expanded.parquet")
DB_PATH = Path("data/lake/keywords.duckdb")

# Column rename map: Hyper column → DuckDB column
COLUMN_MAP = {
    "Accession Number (UT)": "accession_number",
    "WOS Exporter": "lab",
    "DOI": "doi",
    "XREF DOI": "doi_xref",
    "Pubmed ID": "pubmed_id",
    "Title": "title",
    "Abstract": "abstract",
    "Keywords": "keywords_author_raw",
    "Keywords Plus": "keywords_plus_raw",
    "Published Year": "published_year",
    "Times Cited": "times_cited_raw",
    "Number of Cited References": "num_cited_refs",
    "1st Subject Category (traditional)": "subject_cat_1",
    "2nd Subject Category (traditional)": "subject_cat_2",
    "Subject Category (extended)": "subject_cat_extended",
    "1st Category Heading": "category_heading_1",
    "2nd Category Heading": "category_heading_2",
    "1st Document Type": "doc_type",
    "Source title": "source_title",
    "ISSN": "issn",
    "Primary Language": "primary_language",
    "Publisher Full Name": "publisher",
    "Cover date": "cover_date",
    # Remaining columns preserved with snake_case names
    "Database": "database",
    "Editions": "editions",
    "2nd Document Type": "doc_type_2",
    "3rd Document Type": "doc_type_3",
    "All Document Types (comma-separated)": "all_doc_types",
    "Citing items with citation context": "citing_with_context",
    "Background": "background",
    "Support": "support",
    "Differ": "differ",
    "Discuss": "discuss",
    "Basis": "basis",
    "Bib Type": "bib_type",
    "Page count": "page_count",
    "Bib ID": "bib_id",
    "1st Subject Sub-Heading": "subject_sub_heading_1",
    "2nd Subject Sub-Heading": "subject_sub_heading_2",
    "Subject Category": "subject_category",
    "Vol": "vol",
    "Sort date": "sort_date",
    "Published month": "published_month",
    "Published Type": "published_type",
    "Journal OA Gold": "journal_oa_gold",
    "EISSN": "eissn",
    "ISBN": "isbn",
    "EISBN": "eisbn",
    "Parent Book DOI": "parent_book_doi",
    "Source Abbreviation": "source_abbreviation",
    "Abbreviation ISO": "abbreviation_iso",
    "Abbreviation 11": "abbreviation_11",
    "Abbreviation 29": "abbreviation_29",
    "Publisher SeqNo": "publisher_seqno",
    "Publisher Role": "publisher_role",
    "Publisher Address No": "publisher_address_no",
    "Publisher Display name": "publisher_display_name",
    "Source Row Number": "source_row_number",
    "Table Names": "table_names",
    "Category": "category",
    "Sub-Category": "sub_category",
}


def p(msg: str) -> None:
    print(msg, flush=True)


def step1_read_hyper() -> "pd.DataFrame":
    """Read Hyper file and return DataFrame."""
    p(f"Reading {HYPER_PATH} ...")
    t0 = time.time()
    frames = pantab.frames_from_hyper(str(HYPER_PATH))
    df = frames[("Extract", "Extract")]
    p(f"  Read {len(df):,} rows x {len(df.columns)} cols in {time.time()-t0:.1f}s")
    return df


def step2_transform(df: "pd.DataFrame") -> "pd.DataFrame":
    """Rename columns, deduplicate, parse keywords."""
    import pandas as pd

    p("Transforming...")

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Check for unmapped columns (shouldn't happen but be safe)
    unmapped = [c for c in df.columns if c not in COLUMN_MAP.values()]
    if unmapped:
        p(f"  WARNING: unmapped columns dropped: {unmapped}")
        df = df.drop(columns=unmapped)

    # Deduplicate on accession_number (keep first occurrence)
    before = len(df)
    df = df.drop_duplicates(subset=["accession_number"], keep="first")
    p(f"  Deduped: {before:,} → {len(df):,} ({before - len(df):,} dupes removed)")

    # Drop rows with null accession_number
    df = df.dropna(subset=["accession_number"])

    # Parse keywords: comma-delimited → list of stripped strings
    def split_keywords(series: pd.Series) -> pd.Series:
        """Split comma-delimited keyword strings into lists."""
        def _split(val):
            if pd.isna(val) or not str(val).strip():
                return None
            parts = [k.strip() for k in str(val).split(",") if k.strip()]
            return parts if parts else None
        return series.apply(_split)

    df["keywords_author"] = split_keywords(df["keywords_author_raw"])
    df["keywords_plus"] = split_keywords(df["keywords_plus_raw"])

    # Parse times_cited to int (it's all-null string in this file, but handle gracefully)
    df["times_cited"] = pd.to_numeric(df.get("times_cited_raw"), errors="coerce").astype("Int64")

    # Drop raw intermediaries
    df = df.drop(columns=["keywords_author_raw", "keywords_plus_raw", "times_cited_raw"], errors="ignore")

    p(f"  Final shape: {df.shape}")
    return df


def step3_write_parquet(df: "pd.DataFrame") -> None:
    """Write DataFrame to parquet for efficient DuckDB loading."""
    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Convert list columns to proper format for parquet
    # DuckDB can read list columns from parquet natively
    p(f"Writing parquet to {PARQUET_PATH} ...")
    t0 = time.time()
    df.to_parquet(str(PARQUET_PATH), index=False, engine="pyarrow")
    size_mb = PARQUET_PATH.stat().st_size / 1024 / 1024
    p(f"  Wrote {size_mb:.1f} MB in {time.time()-t0:.1f}s")


def step4_load_duckdb(df: "pd.DataFrame") -> int:
    """Create table and load from parquet into DuckDB."""
    p(f"Loading into DuckDB at {DB_PATH} ...")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(str(DB_PATH))
            break
        except duckdb.IOException as e:
            if attempt < max_retries - 1:
                wait = 10 * (attempt + 1)
                p(f"  DB locked (attempt {attempt+1}/{max_retries}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                p(f"ERROR: DB locked after {max_retries} attempts — {e}")
                p("Close other connections (check: lsof data/lake/keywords.duckdb) and retry.")
                sys.exit(1)

    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS raw_wos_natlab_expanded")

    t0 = time.time()
    conn.execute(f"""
        CREATE TABLE raw_wos_natlab_expanded AS
        SELECT
            *,
            now() AS ingested_at
        FROM read_parquet('{PARQUET_PATH}')
    """)

    # Add primary key constraint via unique index
    conn.execute("""
        CREATE UNIQUE INDEX idx_natlab_expanded_pk
        ON raw_wos_natlab_expanded (accession_number)
    """)

    count = conn.execute("SELECT COUNT(*) FROM raw_wos_natlab_expanded").fetchone()[0]
    elapsed = time.time() - t0
    p(f"  Loaded {count:,} rows in {elapsed:.1f}s")

    conn.close()
    return count


def step5_print_stats() -> None:
    """Print summary statistics."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)

    total = conn.execute("SELECT COUNT(*) FROM raw_wos_natlab_expanded").fetchone()[0]
    cols = conn.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='raw_wos_natlab_expanded'").fetchone()[0]

    p(f"\n{'='*60}")
    p(f"raw_wos_natlab_expanded: {total:,} rows, {cols} columns")
    p(f"{'='*60}")

    # DOI coverage
    doi_count = conn.execute(
        "SELECT COUNT(*) FROM raw_wos_natlab_expanded WHERE doi IS NOT NULL AND doi != ''"
    ).fetchone()[0]
    doi_xref = conn.execute(
        "SELECT COUNT(*) FROM raw_wos_natlab_expanded WHERE doi_xref IS NOT NULL AND doi_xref != ''"
    ).fetchone()[0]
    any_doi = conn.execute(
        "SELECT COUNT(*) FROM raw_wos_natlab_expanded WHERE COALESCE(doi, doi_xref) IS NOT NULL"
    ).fetchone()[0]
    pubmed = conn.execute(
        "SELECT COUNT(*) FROM raw_wos_natlab_expanded WHERE pubmed_id IS NOT NULL"
    ).fetchone()[0]
    p(f"\nIdentifier coverage:")
    p(f"  DOI:          {doi_count:>8,} ({100*doi_count/total:.1f}%)")
    p(f"  XREF DOI:     {doi_xref:>8,} ({100*doi_xref/total:.1f}%)")
    p(f"  Any DOI:      {any_doi:>8,} ({100*any_doi/total:.1f}%)")
    p(f"  PubMed ID:    {pubmed:>8,} ({100*pubmed/total:.1f}%)")

    # Lab distribution
    p(f"\nLab distribution:")
    for lab, cnt in conn.execute("""
        SELECT lab, COUNT(*) as cnt FROM raw_wos_natlab_expanded
        GROUP BY lab ORDER BY cnt DESC
    """).fetchall():
        p(f"  {lab:<8} {cnt:>8,} ({100*cnt/total:.1f}%)")

    # Keyword coverage
    kw_auth = conn.execute(
        "SELECT COUNT(*) FROM raw_wos_natlab_expanded WHERE keywords_author IS NOT NULL AND len(keywords_author) > 0"
    ).fetchone()[0]
    kw_plus = conn.execute(
        "SELECT COUNT(*) FROM raw_wos_natlab_expanded WHERE keywords_plus IS NOT NULL AND len(keywords_plus) > 0"
    ).fetchone()[0]
    any_kw = conn.execute("""
        SELECT COUNT(*) FROM raw_wos_natlab_expanded
        WHERE (keywords_author IS NOT NULL AND len(keywords_author) > 0)
           OR (keywords_plus IS NOT NULL AND len(keywords_plus) > 0)
    """).fetchone()[0]
    abstract = conn.execute(
        "SELECT COUNT(*) FROM raw_wos_natlab_expanded WHERE abstract IS NOT NULL AND abstract != ''"
    ).fetchone()[0]

    p(f"\nContent coverage:")
    p(f"  Author KW:    {kw_auth:>8,} ({100*kw_auth/total:.1f}%)")
    p(f"  Keywords Plus: {kw_plus:>8,} ({100*kw_plus/total:.1f}%)")
    p(f"  Any keywords: {any_kw:>8,} ({100*any_kw/total:.1f}%)")
    p(f"  Abstracts:    {abstract:>8,} ({100*abstract/total:.1f}%)")

    # Year range
    yr_min, yr_max = conn.execute(
        "SELECT MIN(published_year), MAX(published_year) FROM raw_wos_natlab_expanded WHERE published_year > 0"
    ).fetchone()
    p(f"\nYear range: {yr_min}–{yr_max}")

    # Compare with existing table
    try:
        old_count = conn.execute("SELECT COUNT(*) FROM raw_wos_natlab_publications").fetchone()[0]
        overlap = conn.execute("""
            SELECT COUNT(*) FROM raw_wos_natlab_expanded e
            WHERE e.accession_number IN (SELECT accession_number FROM raw_wos_natlab_publications)
        """).fetchone()[0]
        new_only = total - overlap
        p(f"\nComparison with raw_wos_natlab_publications ({old_count:,} rows):")
        p(f"  Overlap:      {overlap:>8,}")
        p(f"  New papers:   {new_only:>8,}")
    except Exception:
        pass

    conn.close()


def main():
    p(f"Ingesting expanded natlab WoS from {HYPER_PATH}")
    p(f"{'='*60}")

    df = step1_read_hyper()
    df = step2_transform(df)
    step3_write_parquet(df)
    count = step4_load_duckdb(df)
    step5_print_stats()

    p(f"\nDone. {count:,} rows in raw_wos_natlab_expanded.")


if __name__ == "__main__":
    main()
