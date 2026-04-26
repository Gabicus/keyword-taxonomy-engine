#!/usr/bin/env python3
"""Backfill DOI and other columns from WoS Hyper/Excel exports into DuckDB tables.

Sources:
  - data/WoS/natlab_expanded/Other Labs Part 2.hyper  → raw_wos_natlab_publications
  - data/WoS/natlab_expanded/NETL_Aug2025_with_DOI.xlsx → raw_wos_publications

New columns added to both tables:
  doi, doi_xref, pubmed_id, published_year, times_cited,
  primary_language, num_cited_refs
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import duckdb
import pandas as pd
import pantab

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "lake" / "keywords.duckdb"
HYPER_PATH = PROJECT / "data" / "WoS" / "natlab_expanded" / "Other Labs Part 2.hyper"
NETL_PATH = PROJECT / "data" / "WoS" / "natlab_expanded" / "NETL_Aug2025_with_DOI.xlsx"

# Columns to backfill and their SQL types
NEW_COLS = {
    "doi": "VARCHAR",
    "doi_xref": "VARCHAR",
    "pubmed_id": "VARCHAR",
    "published_year": "INTEGER",
    "times_cited": "INTEGER",
    "primary_language": "VARCHAR",
    "num_cited_refs": "INTEGER",
}

# Mapping from source column names to our column names
SRC_COL_MAP = {
    "Accession Number (UT)": "accession_number",
    "DOI": "doi",
    "XREF DOI": "doi_xref",
    "Pubmed ID": "pubmed_id",
    "Published Year": "published_year",
    "Times Cited": "times_cited",
    "Primary Language": "primary_language",
    "Number of Cited References": "num_cited_refs",
}


def load_hyper() -> pd.DataFrame:
    """Load the Tableau Hyper file, return a normalized DataFrame."""
    print("Loading Hyper file...", flush=True)
    frames = pantab.frames_from_hyper(str(HYPER_PATH))
    # There's one table in the hyper file
    df = next(iter(frames.values()))
    print(f"  Hyper: {len(df):,} rows, {len(df.columns)} columns", flush=True)
    return _normalize(df)


def load_netl() -> pd.DataFrame:
    """Load the NETL Excel file, return a normalized DataFrame."""
    print("Loading NETL Excel...", flush=True)
    df = pd.read_excel(str(NETL_PATH))
    print(f"  NETL: {len(df):,} rows, {len(df.columns)} columns", flush=True)
    return _normalize(df)


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Select and rename columns from source data."""
    # Only keep columns we have mappings for
    available = {k: v for k, v in SRC_COL_MAP.items() if k in df.columns}
    df = df[list(available.keys())].rename(columns=available)

    # Convert types - make everything string-safe for VARCHAR cols
    for col in ("doi", "doi_xref", "pubmed_id", "primary_language", "accession_number"):
        if col in df.columns:
            df[col] = df[col].astype(object).where(df[col].notna(), None)
            # Convert to plain Python strings (strip pyarrow wrapper if present)
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    for col in ("published_year", "times_cited", "num_cited_refs"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Convert to nullable int
            df[col] = df[col].astype("Int64")

    # Drop rows with no accession number
    df = df.dropna(subset=["accession_number"])
    print(f"  Normalized: {len(df):,} rows with accession_number", flush=True)
    return df


def backfill_table(con: duckdb.DuckDBPyConnection, table: str, source_df: pd.DataFrame) -> None:
    """Add new columns to table and update from source DataFrame."""
    print(f"\nBackfilling {table}...", flush=True)

    # 1. Add columns if they don't exist
    for col, dtype in NEW_COLS.items():
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
            print(f"  Added column: {col} ({dtype})", flush=True)
        except duckdb.CatalogException:
            print(f"  Column exists: {col}", flush=True)

    # 2. Get current row count
    (count_before,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    print(f"  Table has {count_before:,} rows", flush=True)

    # 3. Write source data to a temp parquet, then load into DuckDB temp table
    # This is MUCH faster than executemany
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    source_df.to_parquet(tmp_path, index=False)
    print(f"  Wrote {len(source_df):,} rows to temp parquet", flush=True)

    con.execute("DROP TABLE IF EXISTS _backfill_tmp")
    con.execute(f"CREATE TEMP TABLE _backfill_tmp AS SELECT * FROM read_parquet('{tmp_path}')")
    (tmp_count,) = con.execute("SELECT COUNT(*) FROM _backfill_tmp").fetchone()
    print(f"  Temp table: {tmp_count:,} rows", flush=True)

    # 4. Check overlap
    (overlap,) = con.execute(f"""
        SELECT COUNT(*) FROM {table} t
        JOIN _backfill_tmp s ON t.accession_number = s.accession_number
    """).fetchone()
    print(f"  Matching accession numbers: {overlap:,}", flush=True)

    # 5. Update each column
    update_cols = [c for c in NEW_COLS if c in source_df.columns]
    for col in update_cols:
        # Only update where source has a non-null value
        result = con.execute(f"""
            UPDATE {table} AS t
            SET {col} = s.{col}
            FROM _backfill_tmp AS s
            WHERE t.accession_number = s.accession_number
              AND s.{col} IS NOT NULL
        """)
        affected = result.fetchone()
        # Count non-null values we now have
        (filled,) = con.execute(f"SELECT COUNT({col}) FROM {table}").fetchone()
        print(f"  {col}: {filled:,} non-null values", flush=True)

    con.execute("DROP TABLE IF EXISTS _backfill_tmp")

    # Clean up temp file
    Path(tmp_path).unlink(missing_ok=True)
    print(f"  Done with {table}.", flush=True)


def main() -> int:
    # Validate source files exist
    for p in (HYPER_PATH, NETL_PATH):
        if not p.exists():
            print(f"ERROR: Source file not found: {p}", flush=True)
            return 1

    # Load source data first (no DB lock needed)
    hyper_df = load_hyper()
    netl_df = load_netl()

    # Connect to DuckDB
    print("\nConnecting to DuckDB...", flush=True)
    try:
        con = duckdb.connect(str(DB_PATH))
    except duckdb.IOException as e:
        print(f"ERROR: Could not open database (locked?): {e}", flush=True)
        print("Close any other process using the database and retry.", flush=True)
        return 1

    try:
        # Backfill natlab table from hyper
        backfill_table(con, "raw_wos_natlab_publications", hyper_df)

        # Backfill NETL table from Excel
        backfill_table(con, "raw_wos_publications", netl_df)

        # Summary
        print("\n=== Summary ===", flush=True)
        for table in ("raw_wos_natlab_publications", "raw_wos_publications"):
            (total,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            (with_doi,) = con.execute(f"SELECT COUNT(doi) FROM {table}").fetchone()
            pct = (with_doi / total * 100) if total > 0 else 0
            print(f"  {table}: {with_doi:,}/{total:,} rows with DOI ({pct:.1f}%)", flush=True)

    except Exception as e:
        print(f"ERROR: {e}", flush=True)
        raise
    finally:
        con.close()

    print("\nDone.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
