#!/usr/bin/env python3
"""Crawl 2 additional levels of citations in both directions for the HEA review paper.

Level 0: center paper (already have)
Level -1: 293 roots (already have) → Level -2: roots of roots (NEW)
Level +1: 600 branches (already have) → Level +2: branches of branches (NEW)

For level -2: fetch referenced_works for each root (sample top 50 by citation count)
For level +2: fetch top forward citations for each branch (sample top 50 by citation count)

Saves to data/viz/citation_tree_alloy_hea_deep.json
"""

import json
import time
from pathlib import Path
from urllib.request import Request, urlopen

MAILTO = "gabe.dewitt@gmail.com"
UA = f"KeywordTaxonomyEngine/1.0 (mailto:{MAILTO})"
BASE = "https://api.openalex.org"
OUT = Path(__file__).parent.parent / "data" / "viz" / "citation_tree_alloy_hea_deep.json"
EXISTING = Path(__file__).parent.parent / "data" / "viz" / "citation_tree_alloy_hea.json"

# Limits per level to keep it manageable
MAX_ROOTS_TO_EXPAND = 50       # expand top 50 roots (by citation count)
MAX_BRANCHES_TO_EXPAND = 50    # expand top 50 branches
MAX_REFS_PER_NODE = 20         # max backward refs per root
MAX_CITES_PER_NODE = 20        # max forward cites per branch


def api_get(url: str) -> dict:
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_work(oa_id: str) -> dict:
    bare = oa_id.replace("https://openalex.org/", "")
    url = f"{BASE}/works/{bare}?mailto={MAILTO}"
    return api_get(url)


def fetch_batch(oa_ids: list[str]) -> list[dict]:
    bare = [oid.replace("https://openalex.org/", "") for oid in oa_ids]
    filter_val = "|".join(bare)
    url = (
        f"{BASE}/works?filter=openalex:{filter_val}"
        f"&per_page=50&select=id,doi,title,publication_year,cited_by_count"
        f"&mailto={MAILTO}"
    )
    return api_get(url).get("results", [])


def fetch_forward_top(oa_id: str, n: int = 20) -> list[dict]:
    bare = oa_id.replace("https://openalex.org/", "")
    url = (
        f"{BASE}/works?filter=cites:{bare}"
        f"&per_page={n}&page=1"
        f"&select=id,doi,title,publication_year,cited_by_count"
        f"&sort=cited_by_count:desc"
        f"&mailto={MAILTO}"
    )
    return api_get(url).get("results", [])


def normalize(rec: dict) -> dict:
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
    print("=== Deep Citation Crawl: 2 More Levels ===\n")

    with open(EXISTING) as f:
        tree = json.load(f)

    center = tree["center"]
    roots = tree["roots"]
    branches = tree["branches"]
    print(f"Existing: {len(roots)} roots, {len(branches)} branches")

    # Select top roots/branches to expand (by citation count)
    roots_sorted = sorted(roots, key=lambda r: r.get("cited_by_count", 0), reverse=True)
    branches_sorted = sorted(branches, key=lambda b: b.get("cited_by_count", 0), reverse=True)

    expand_roots = [r for r in roots_sorted[:MAX_ROOTS_TO_EXPAND] if r.get("openalex_id")]
    expand_branches = [b for b in branches_sorted[:MAX_BRANCHES_TO_EXPAND] if b.get("openalex_id")]
    print(f"Expanding: {len(expand_roots)} roots, {len(expand_branches)} branches")

    # --- Level -2: roots of roots ---
    print(f"\n--- Level -2: Fetching refs for {len(expand_roots)} roots ---")
    level_minus2 = {}  # parent_id -> [children]
    seen_ids = set()

    for i, root in enumerate(expand_roots):
        rid = root["openalex_id"]
        print(f"  [{i+1}/{len(expand_roots)}] {root['title'][:60]}...")
        try:
            work = fetch_work(rid)
            refs = work.get("referenced_works", [])[:MAX_REFS_PER_NODE]
            if refs:
                # Fetch metadata in one batch (max 50)
                batch_results = fetch_batch(refs[:50])
                children = [normalize(r) for r in batch_results]
                level_minus2[rid] = children
                for c in children:
                    seen_ids.add(c["openalex_id"])
                print(f"    → {len(children)} refs")
            else:
                level_minus2[rid] = []
                print(f"    → 0 refs")
            time.sleep(0.12)
        except Exception as e:
            print(f"    ERROR: {e}")
            level_minus2[rid] = []
            time.sleep(0.5)

    total_m2 = sum(len(v) for v in level_minus2.values())
    print(f"  Total level -2 nodes: {total_m2}")

    # --- Level +2: branches of branches ---
    print(f"\n--- Level +2: Fetching cites for {len(expand_branches)} branches ---")
    level_plus2 = {}  # parent_id -> [children]

    for i, branch in enumerate(expand_branches):
        bid = branch["openalex_id"]
        print(f"  [{i+1}/{len(expand_branches)}] {branch['title'][:60]}...")
        try:
            cites = fetch_forward_top(bid, MAX_CITES_PER_NODE)
            children = [normalize(r) for r in cites]
            level_plus2[bid] = children
            for c in children:
                seen_ids.add(c["openalex_id"])
            print(f"    → {len(children)} cites")
            time.sleep(0.12)
        except Exception as e:
            print(f"    ERROR: {e}")
            level_plus2[bid] = []
            time.sleep(0.5)

    total_p2 = sum(len(v) for v in level_plus2.values())
    print(f"  Total level +2 nodes: {total_p2}")

    # --- Year anomaly report ---
    print("\n--- Year Anomalies ---")
    anomalies = []
    center_year = center["year"]
    for r in roots:
        if r.get("year") and r["year"] > center_year:
            anomalies.append({
                "type": "root_after_center",
                "node": r["title"][:80],
                "year": r["year"],
                "center_year": center_year,
            })
    for parent_id, children in level_minus2.items():
        parent = next((r for r in roots if r["openalex_id"] == parent_id), None)
        if parent:
            for c in children:
                if c.get("year") and parent.get("year") and c["year"] > parent["year"]:
                    anomalies.append({
                        "type": "deep_root_after_parent",
                        "node": c["title"][:80],
                        "year": c["year"],
                        "parent_year": parent["year"],
                    })

    print(f"  {len(anomalies)} anomalies found")
    for a in anomalies[:10]:
        print(f"    {a['type']}: {a['node']} ({a['year']})")

    # --- Save ---
    deep_tree = {
        "center": center,
        "roots": roots,
        "branches": branches,
        "level_minus2": level_minus2,
        "level_plus2": level_plus2,
        "anomalies": anomalies,
        "stats": {
            "n_roots": len(roots),
            "n_branches": len(branches),
            "n_roots_expanded": len(expand_roots),
            "n_branches_expanded": len(expand_branches),
            "n_level_minus2": total_m2,
            "n_level_plus2": total_p2,
            "total_nodes": 1 + len(roots) + len(branches) + total_m2 + total_p2,
        },
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(deep_tree, indent=2, ensure_ascii=False))
    print(f"\nSaved to {OUT}")
    print(f"Total nodes: {deep_tree['stats']['total_nodes']}")
    print("Done.")


if __name__ == "__main__":
    main()
