#!/usr/bin/env python3
"""Ingest WoS subject category mappings into DuckDB.

Reads three Excel files from data/WoS/natlab_expanded/ and creates
a wos_category_mapping table that maps WoS subject categories to
our 14-discipline taxonomy.

Primary source: categorized_subjects.xlsx (best hierarchy, 84 rows)
Gap-fill source: categories_T3.xlsx (200 rows, coarser hierarchy)
Validation: Category List from all labs.xlsx (201 unique categories)
"""

from pathlib import Path
import sys
import time

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT / "data" / "WoS" / "natlab_expanded"
DB_PATH = PROJECT / "data" / "lake" / "keywords.duckdb"

# ── Discipline mapping ──────────────────────────────────────────────
# Maps (main_category, sub_category) from categorized_subjects.xlsx
# to our 14 discipline IDs.

CATEGORIZED_DISCIPLINE_MAP: dict[tuple[str, str], str] = {
    # Natural Sciences
    ("Natural Sciences", "Physics"): "math_physics",
    ("Natural Sciences", "Chemistry"): "chemical_sciences",
    ("Natural Sciences", "Biology"): "biological_medical",
    ("Natural Sciences", "Earth & Environmental Sciences"): "earth_environmental",
    ("Natural Sciences", "Materials Science"): "materials",
    # Engineering & Technology
    ("Engineering & Technology", "Mechanical Engineering"): "ee_me_engineering",
    ("Engineering & Technology", "Electrical Engineering"): "ee_me_engineering",
    ("Engineering & Technology", "Civil Engineering"): "ee_me_engineering",
    ("Engineering & Technology", "Computer Science"): "computation_data",
    # Medical & Health Sciences
    ("Medical & Health Sciences", "General Medicine"): "biological_medical",
    ("Medical & Health Sciences", "Specialized Medicine"): "biological_medical",
    ("Medical & Health Sciences", "Public Health"): "biological_medical",
    # Social Sciences
    ("Social Sciences", "Psychology"): "biological_medical",
    ("Social Sciences", "Economics"): "policy_economics",
    ("Social Sciences", "Sociology"): "policy_economics",
    # Humanities
    ("Humanities", "General Humanities"): "policy_economics",
    # Multidisciplinary
    ("Multidisciplinary", "Multidisciplinary Sciences"): "general_science",
}

# Maps Category from categories_T3.xlsx (used for gap-fill rows)
T3_CATEGORY_DISCIPLINE_MAP: dict[str, str] = {
    "Physical & Mathematical Sciences": "math_physics",
    "Engineering & Technology": "ee_me_engineering",
    "Life & Health Sciences": "biological_medical",
    "Social Sciences & Humanities": "policy_economics",
    "Applied Sciences & Services": "earth_environmental",
    "Information & Communication": "computation_data",
}

# Energy-related subject categories get fossil_energy instead of default
ENERGY_KEYWORDS = {
    "energy & fuels", "nuclear science & technology", "engineering, petroleum",
    "mining & mineral processing", "geochemistry & geophysics",
}


def _discipline_for_t3_row(category: str, subject: str) -> str:
    """Pick discipline for a T3 row, with energy override."""
    if subject.lower().strip() in ENERGY_KEYWORDS:
        return "fossil_energy"
    return T3_CATEGORY_DISCIPLINE_MAP.get(category, "general_science")


def load_categorized_subjects() -> pd.DataFrame:
    """Load categorized_subjects.xlsx — primary hierarchy source."""
    path = DATA_DIR / "categorized_subjects.xlsx"
    df = pd.read_excel(path)
    print(f"  categorized_subjects.xlsx: {len(df)} rows", flush=True)

    records = []
    for _, row in df.iterrows():
        main = str(row["Main Category"]).strip()
        sub = str(row["Sub Category"]).strip()
        subsub = str(row["Sub Sub Category"]).strip() if pd.notna(row["Sub Sub Category"]) else None
        subject = str(row["Subject Category"]).strip()
        disc = CATEGORIZED_DISCIPLINE_MAP.get((main, sub), "general_science")
        records.append({
            "subject_category": subject,
            "main_category": main,
            "sub_category": sub,
            "sub_sub_category": subsub,
            "discipline_id": disc,
        })
    return pd.DataFrame(records)


def load_t3_categories() -> pd.DataFrame:
    """Load categories_T3.xlsx — gap-fill source."""
    path = DATA_DIR / "categories_T3.xlsx"
    df = pd.read_excel(path)
    print(f"  categories_T3.xlsx: {len(df)} rows", flush=True)

    records = []
    for _, row in df.iterrows():
        cat = str(row["Category"]).strip()
        subcat = str(row[" Sub-Category"]).strip()  # note leading space in col name
        subject = str(row["Subjects Category"]).strip()
        disc = _discipline_for_t3_row(cat, subject)
        records.append({
            "subject_category": subject,
            "main_category": cat,
            "sub_category": subcat,
            "sub_sub_category": None,
            "discipline_id": disc,
        })
    return pd.DataFrame(records)


def load_all_labs_list() -> set[str]:
    """Load Category List from all labs.xlsx — validation set."""
    path = DATA_DIR / "Category List from all labs.xlsx"
    df = pd.read_excel(path)
    print(f"  Category List from all labs.xlsx: {len(df)} rows", flush=True)
    return set(df["1st Subject Category (traditional)"].dropna().str.strip())


def merge_sources(primary: pd.DataFrame, gap_fill: pd.DataFrame, all_labs: set[str]) -> pd.DataFrame:
    """Merge: primary wins, T3 fills gaps, report coverage of all_labs list."""
    # Primary takes precedence
    seen = set(primary["subject_category"].str.lower())
    gap_rows = gap_fill[~gap_fill["subject_category"].str.lower().isin(seen)]
    merged = pd.concat([primary, gap_rows], ignore_index=True)

    # Deduplicate on subject_category (case-insensitive), keep first
    merged["_lower"] = merged["subject_category"].str.lower()
    merged = merged.drop_duplicates(subset="_lower", keep="first").drop(columns="_lower")

    # Coverage check
    merged_lower = set(merged["subject_category"].str.lower())
    all_labs_lower = {s.lower() for s in all_labs}
    covered = all_labs_lower & merged_lower
    missing = all_labs_lower - merged_lower
    print(f"  Coverage: {len(covered)}/{len(all_labs_lower)} all-labs categories mapped", flush=True)
    if missing:
        print(f"  Unmapped ({len(missing)}): {sorted(missing)[:10]}{'...' if len(missing)>10 else ''}", flush=True)

    return merged.reset_index(drop=True)


def write_to_duckdb(df: pd.DataFrame, max_retries: int = 10) -> None:
    """Write mapping table to DuckDB, handling lock errors."""
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(str(DB_PATH))
            conn.execute("""
                CREATE OR REPLACE TABLE wos_category_mapping (
                    subject_category VARCHAR NOT NULL,
                    main_category VARCHAR,
                    sub_category VARCHAR,
                    sub_sub_category VARCHAR,
                    discipline_id VARCHAR,
                    PRIMARY KEY (subject_category)
                )
            """)
            conn.execute("DELETE FROM wos_category_mapping")
            conn.register("df_mapping", df)
            conn.execute("""
                INSERT INTO wos_category_mapping
                SELECT subject_category, main_category, sub_category,
                       sub_sub_category, discipline_id
                FROM df_mapping
            """)
            count = conn.execute("SELECT COUNT(*) FROM wos_category_mapping").fetchone()[0]
            print(f"  Wrote {count} rows to wos_category_mapping", flush=True)

            # Quick discipline distribution
            dist = conn.execute("""
                SELECT discipline_id, COUNT(*) as n
                FROM wos_category_mapping
                GROUP BY discipline_id
                ORDER BY n DESC
            """).fetchall()
            print("  Discipline distribution:", flush=True)
            for disc, n in dist:
                print(f"    {disc}: {n}", flush=True)

            conn.close()
            return
        except duckdb.IOException as e:
            if "lock" in str(e).lower() and attempt < max_retries - 1:
                wait = min(5 + attempt * 3, 30)
                print(f"  DB locked, retrying in {wait}s... ({attempt+1}/{max_retries})", flush=True)
                time.sleep(wait)
            else:
                raise


def main():
    print("Loading Excel files...", flush=True)
    primary = load_categorized_subjects()
    gap_fill = load_t3_categories()
    all_labs = load_all_labs_list()

    print("Merging sources...", flush=True)
    merged = merge_sources(primary, gap_fill, all_labs)

    print(f"Writing {len(merged)} mappings to DuckDB...", flush=True)
    write_to_duckdb(merged)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
