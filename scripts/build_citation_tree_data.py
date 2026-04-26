#!/usr/bin/env python3
"""Build citation tree JSON data for visualization.

For each example DOI, extracts:
- Center paper metadata + keywords
- Roots (papers it cites) with metadata
- Branches (papers that cite it) with metadata

Outputs JSON files to data/viz/citation_tree_*.json
"""

import json
import re
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent.parent / "data" / "lake" / "keywords.duckdb"
OUT_DIR = Path(__file__).parent.parent / "data" / "viz"

EXAMPLE_DOIS = [
    "10.1016/bs.acat.2019.10.002",
    "10.1016/j.biotechadv.2014.03.011",
]


def doi_slug(doi: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", doi)


def build_tree(con: duckdb.DuckDBPyConnection, doi: str) -> dict:
    # 1. Find OpenAlex ID via bridge
    row = con.execute(
        "SELECT openalex_id, doi FROM openalex_wos_bridge WHERE doi = ?", [doi]
    ).fetchone()
    if not row:
        raise ValueError(f"DOI {doi} not found in openalex_wos_bridge")
    oa_id = row[0]

    # 2. Get center paper metadata from WoS
    wos = con.execute(
        """SELECT w.accession_number, p.title, p.published_year,
                  w.keywords_author, w.keywords_plus
           FROM openalex_wos_bridge b
           JOIN raw_wos_natlab_publications p ON b.accession_number = p.accession_number
           JOIN raw_wos_natlab_expanded w ON b.accession_number = w.accession_number
           WHERE b.openalex_id = ?""",
        [oa_id],
    ).fetchone()

    keywords = []
    if wos:
        if wos[3]:  # author keywords
            keywords.extend(wos[3])
        if wos[4]:  # keywords plus
            keywords.extend(wos[4])

    center = {
        "openalex_id": oa_id,
        "doi": doi,
        "title": wos[1] if wos else doi,
        "year": int(wos[2]) if wos else None,
        "keywords": keywords,
    }

    # 3. Get roots (papers this one cites)
    roots_raw = con.execute(
        """SELECT c.cited_id,
                  p.title, p.publication_year, p.doi,
                  CASE WHEN b.openalex_id IS NOT NULL THEN true ELSE false END as in_corpus
           FROM openalex_citations c
           LEFT JOIN raw_openalex_publications p ON c.cited_id = p.openalex_id
           LEFT JOIN openalex_wos_bridge b ON c.cited_id = b.openalex_id
           WHERE c.citing_id = ?""",
        [oa_id],
    ).fetchall()

    # For roots in bridge but not in raw_openalex_publications, get WoS metadata
    roots = []
    for r in roots_raw:
        cited_id, title, year, r_doi, in_corpus = r
        # If not in raw_openalex_publications, try WoS via bridge
        if title is None:
            wos_meta = con.execute(
                """SELECT p.title, p.published_year, b.doi
                   FROM openalex_wos_bridge b
                   JOIN raw_wos_natlab_publications p ON b.accession_number = p.accession_number
                   WHERE b.openalex_id = ?""",
                [cited_id],
            ).fetchone()
            if wos_meta:
                title, year, r_doi = wos_meta[0], int(wos_meta[1]), wos_meta[2]
                in_corpus = True

        roots.append({
            "openalex_id": cited_id,
            "title": title,
            "year": int(year) if year else None,
            "doi": r_doi,
            "in_corpus": bool(in_corpus),
        })

    # 4. Get branches (papers that cite this one)
    branches_raw = con.execute(
        """SELECT c.citing_id,
                  p.title, p.publication_year, p.doi,
                  CASE WHEN b.openalex_id IS NOT NULL THEN true ELSE false END as in_corpus
           FROM openalex_citations c
           LEFT JOIN raw_openalex_publications p ON c.citing_id = p.openalex_id
           LEFT JOIN openalex_wos_bridge b ON c.citing_id = b.openalex_id
           WHERE c.cited_id = ?""",
        [oa_id],
    ).fetchall()

    branches = []
    for r in branches_raw:
        citing_id, title, year, r_doi, in_corpus = r
        if title is None:
            wos_meta = con.execute(
                """SELECT p.title, p.published_year, b.doi
                   FROM openalex_wos_bridge b
                   JOIN raw_wos_natlab_publications p ON b.accession_number = p.accession_number
                   WHERE b.openalex_id = ?""",
                [citing_id],
            ).fetchone()
            if wos_meta:
                title, year, r_doi = wos_meta[0], int(wos_meta[1]), wos_meta[2]
                in_corpus = True

        branches.append({
            "openalex_id": citing_id,
            "title": title,
            "year": int(year) if year else None,
            "doi": r_doi,
            "in_corpus": bool(in_corpus),
        })

    n_roots_in = sum(1 for r in roots if r["in_corpus"])
    n_branches_in = sum(1 for b in branches if b["in_corpus"])

    return {
        "center": center,
        "roots": roots,
        "branches": branches,
        "stats": {
            "n_roots": len(roots),
            "n_branches": len(branches),
            "n_roots_in_corpus": n_roots_in,
            "n_branches_in_corpus": n_branches_in,
        },
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)

    for doi in EXAMPLE_DOIS:
        print(f"Building tree for {doi}...")
        tree = build_tree(con, doi)
        slug = doi_slug(doi)
        out_path = OUT_DIR / f"citation_tree_{slug}.json"
        with open(out_path, "w") as f:
            json.dump(tree, f, indent=2)
        print(
            f"  -> {out_path.name}: {tree['stats']['n_roots']} roots, "
            f"{tree['stats']['n_branches']} branches"
        )

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
