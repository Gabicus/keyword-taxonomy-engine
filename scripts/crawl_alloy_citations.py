#!/usr/bin/env python3
"""Crawl forward+backward citations for the HEA review paper from OpenAlex API.

Paper: "Microstructures and properties of high-entropy alloys"
DOI: 10.1016/j.pmatsci.2013.10.001
OpenAlex: W2039061417

Fetches:
- All ~308 backward refs (referenced_works)
- Top ~600 forward citations (most-cited papers that cite this work)

Saves to data/viz/citation_tree_alloy_hea.json
"""

import json
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import quote

MAILTO = "gabe.dewitt@gmail.com"
UA = f"KeywordTaxonomyEngine/1.0 (mailto:{MAILTO})"
BASE = "https://api.openalex.org"
CENTER_ID = "W2039061417"
OUT = Path(__file__).parent.parent / "data" / "viz" / "citation_tree_alloy_hea.json"


def api_get(url: str) -> dict:
    """Fetch JSON from OpenAlex API with polite headers."""
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_center() -> dict:
    """Get the center paper's full record."""
    url = f"{BASE}/works/{CENTER_ID}?mailto={MAILTO}"
    return api_get(url)


def fetch_batch(oa_ids: list[str]) -> list[dict]:
    """Fetch metadata for a batch of OpenAlex IDs (max 50)."""
    # Strip URL prefix to get bare IDs
    bare = [oid.replace("https://openalex.org/", "") for oid in oa_ids]
    filter_val = "|".join(bare)
    url = (
        f"{BASE}/works?filter=openalex:{filter_val}"
        f"&per_page=50&select=id,doi,title,publication_year,cited_by_count"
        f"&mailto={MAILTO}"
    )
    data = api_get(url)
    return data.get("results", [])


def fetch_forward_page(page: int) -> dict:
    """Fetch one page of forward citations, sorted by cited_by_count desc."""
    url = (
        f"{BASE}/works?filter=cites:{CENTER_ID}"
        f"&per_page=200&page={page}"
        f"&select=id,doi,title,publication_year,cited_by_count"
        f"&sort=cited_by_count:desc"
        f"&mailto={MAILTO}"
    )
    return api_get(url)


def normalize(rec: dict) -> dict:
    """Normalize an OpenAlex record to our schema."""
    doi = rec.get("doi") or ""
    if doi.startswith("https://doi.org/"):
        doi = doi[len("https://doi.org/"):]
    return {
        "openalex_id": rec.get("id", ""),
        "doi": doi,
        "title": rec.get("title", ""),
        "year": rec.get("publication_year"),
        "cited_by_count": rec.get("cited_by_count", 0),
    }


def main():
    print("=== Citation Crawl: HEA Review Paper ===\n")

    # Step 1: Fetch center paper
    print("Fetching center paper...")
    center_raw = fetch_center()
    referenced_works = center_raw.get("referenced_works", [])
    cited_by = center_raw.get("cited_by_count", 0)
    print(f"  Title: {center_raw['title']}")
    print(f"  Year: {center_raw['publication_year']}")
    print(f"  Backward refs: {len(referenced_works)}")
    print(f"  Forward citations: {cited_by}")
    time.sleep(0.1)

    # Step 2: Fetch backward citations in batches of 50
    print(f"\nFetching {len(referenced_works)} backward refs in batches of 50...")
    roots = []
    for i in range(0, len(referenced_works), 50):
        batch = referenced_works[i : i + 50]
        batch_num = i // 50 + 1
        total_batches = (len(referenced_works) + 49) // 50
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} IDs)...")
        try:
            results = fetch_batch(batch)
            roots.extend([normalize(r) for r in results])
        except Exception as e:
            print(f"    ERROR: {e}")
        time.sleep(0.1)

    print(f"  Got {len(roots)} backward refs")

    # Step 3: Fetch forward citations (top 600 by citation count)
    print("\nFetching top forward citations (3 pages x 200)...")
    branches = []
    for page in range(1, 4):
        print(f"  Page {page}/3...")
        try:
            data = fetch_forward_page(page)
            results = data.get("results", [])
            branches.extend([normalize(r) for r in results])
            print(f"    Got {len(results)} results (total so far: {len(branches)})")
            if len(results) < 200:
                print("    (last page)")
                break
        except Exception as e:
            print(f"    ERROR: {e}")
        time.sleep(0.1)

    print(f"  Got {len(branches)} forward citations")

    # Compute stats
    root_years = [r["year"] for r in roots if r["year"]]
    branch_years = [b["year"] for b in branches if b["year"]]

    center_doi = center_raw.get("doi", "")
    if center_doi.startswith("https://doi.org/"):
        center_doi = center_doi[len("https://doi.org/"):]

    tree = {
        "center": {
            "openalex_id": f"https://openalex.org/{CENTER_ID}",
            "doi": center_doi,
            "title": center_raw["title"],
            "year": center_raw["publication_year"],
            "cited_by_count": cited_by,
            "lab": "NETL",
        },
        "roots": sorted(roots, key=lambda r: r.get("year") or 0),
        "branches": branches,  # already sorted by cited_by_count desc
        "stats": {
            "n_roots": len(roots),
            "n_branches_total": cited_by,
            "n_branches_fetched": len(branches),
            "year_range_roots": [min(root_years), max(root_years)] if root_years else [],
            "year_range_branches": [min(branch_years), max(branch_years)] if branch_years else [],
        },
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(tree, indent=2, ensure_ascii=False))
    print(f"\nSaved to {OUT}")
    print(f"  Roots: {len(roots)}")
    print(f"  Branches: {len(branches)}")
    if root_years:
        print(f"  Root year range: {min(root_years)}-{max(root_years)}")
    if branch_years:
        print(f"  Branch year range: {min(branch_years)}-{max(branch_years)}")
    print("\nDone.")


if __name__ == "__main__":
    main()
