#!/usr/bin/env python3
"""Build 3D citation tree data by positioning papers in keyword domain space.

For each paper in the deep citation tree:
1. Fetch OpenAlex topics/concepts (batched)
2. Match topic names to our keyword UMAP coordinates
3. Average matched positions → paper's 3D location
4. Fallback: discipline centroid if no keyword matches

Outputs data/viz/citation_tree_3d.json for Three.js visualization.
"""

import json
import time
import re
from pathlib import Path
from urllib.request import Request, urlopen
from collections import defaultdict

MAILTO = "gabe.dewitt@gmail.com"
UA = f"KeywordTaxonomyEngine/1.0 (mailto:{MAILTO})"
BASE = "https://api.openalex.org"

TREE_FILE = Path(__file__).parent.parent / "data" / "viz" / "citation_tree_alloy_hea_deep.json"
NEBULA_FILE = Path(__file__).parent.parent / "data" / "viz" / "nebula_data.json"
OUT = Path(__file__).parent.parent / "data" / "viz" / "citation_tree_3d.json"


def api_get(url: str) -> dict:
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def load_keyword_index(nebula_data: list[dict]) -> tuple[dict, dict]:
    """Build keyword→position index and discipline→centroid map."""
    kw_index = {}  # normalized_term → {x, y, z, discipline}
    disc_points = defaultdict(list)

    for kw in nebula_data:
        term = kw["term"].lower().strip()
        pos = {"x": kw["x"], "y": kw["y"], "z": kw["z"], "discipline": kw["discipline"]}
        kw_index[term] = pos
        if kw["discipline"] != "unassigned":
            disc_points[kw["discipline"]].append((kw["x"], kw["y"], kw["z"]))

    # Compute discipline centroids
    disc_centroids = {}
    for disc, pts in disc_points.items():
        n = len(pts)
        disc_centroids[disc] = {
            "x": sum(p[0] for p in pts) / n,
            "y": sum(p[1] for p in pts) / n,
            "z": sum(p[2] for p in pts) / n,
        }

    # Global centroid for fallback
    all_pts = [(kw["x"], kw["y"], kw["z"]) for kw in nebula_data]
    n = len(all_pts)
    disc_centroids["_global"] = {
        "x": sum(p[0] for p in all_pts) / n,
        "y": sum(p[1] for p in all_pts) / n,
        "z": sum(p[2] for p in all_pts) / n,
    }

    return kw_index, disc_centroids


def fetch_topics_batch(oa_ids: list[str]) -> dict:
    """Fetch topics for a batch of OpenAlex IDs. Returns id→topics mapping."""
    bare = [oid.replace("https://openalex.org/", "") for oid in oa_ids]
    filter_val = "|".join(bare)
    url = (
        f"{BASE}/works?filter=openalex:{filter_val}"
        f"&per_page=50&select=id,topics,concepts"
        f"&mailto={MAILTO}"
    )
    try:
        data = api_get(url)
        result = {}
        for work in data.get("results", []):
            wid = work["id"]
            topics = []
            for t in work.get("topics", []):
                topics.append(t.get("display_name", ""))
                if t.get("subfield", {}).get("display_name"):
                    topics.append(t["subfield"]["display_name"])
                if t.get("field", {}).get("display_name"):
                    topics.append(t["field"]["display_name"])
                if t.get("domain", {}).get("display_name"):
                    topics.append(t["domain"]["display_name"])
            # Also grab concepts if available
            for c in work.get("concepts", []):
                if c.get("display_name"):
                    topics.append(c["display_name"])
            result[wid] = list(set(t for t in topics if t))
        return result
    except Exception as e:
        print(f"    batch error: {e}")
        return {}


def position_paper(topics: list[str], kw_index: dict, disc_centroids: dict) -> dict:
    """Find 3D position by matching topics to keyword index."""
    matched = []
    matched_discs = []

    for topic in topics:
        term = topic.lower().strip()
        if term in kw_index:
            pos = kw_index[term]
            matched.append((pos["x"], pos["y"], pos["z"]))
            matched_discs.append(pos["discipline"])
            continue
        # Try partial match: split multi-word, check each
        words = term.split()
        if len(words) > 1:
            for w in words:
                if len(w) > 3 and w in kw_index:
                    pos = kw_index[w]
                    matched.append((pos["x"], pos["y"], pos["z"]))
                    matched_discs.append(pos["discipline"])
                    break

    if matched:
        n = len(matched)
        return {
            "x": sum(p[0] for p in matched) / n,
            "y": sum(p[1] for p in matched) / n,
            "z": sum(p[2] for p in matched) / n,
            "n_matches": n,
            "disciplines": list(set(d for d in matched_discs if d != "unassigned")),
            "method": "keyword_avg",
        }

    # Fallback: global centroid with jitter
    import random
    c = disc_centroids["_global"]
    return {
        "x": c["x"] + random.uniform(-1, 1),
        "y": c["y"] + random.uniform(-1, 1),
        "z": c["z"] + random.uniform(-1, 1),
        "n_matches": 0,
        "disciplines": [],
        "method": "fallback",
    }


def main():
    print("=== 3D Citation Tree Builder ===\n")

    # Load data
    print("Loading nebula keyword index...")
    with open(NEBULA_FILE) as f:
        nebula = json.load(f)
    kw_index, disc_centroids = load_keyword_index(nebula["keywords"])
    print(f"  {len(kw_index)} keywords indexed, {len(disc_centroids)-1} discipline centroids")

    print("Loading citation tree...")
    with open(TREE_FILE) as f:
        tree = json.load(f)

    center = tree["center"]
    roots = tree["roots"]
    branches = tree["branches"]
    level_m2 = tree.get("level_minus2", {})
    level_p2 = tree.get("level_plus2", {})

    # Collect all unique OpenAlex IDs
    all_nodes = []
    all_nodes.append({"node": center, "type": "center"})
    for r in roots:
        all_nodes.append({"node": r, "type": "root"})
    for b in branches:
        all_nodes.append({"node": b, "type": "branch"})
    for parent_id, children in level_m2.items():
        for c in children:
            all_nodes.append({"node": c, "type": "deep_root", "parent_id": parent_id})
    for parent_id, children in level_p2.items():
        for c in children:
            all_nodes.append({"node": c, "type": "deep_branch", "parent_id": parent_id})

    # Deduplicate by openalex_id
    seen = set()
    unique_nodes = []
    for entry in all_nodes:
        oid = entry["node"].get("openalex_id", "")
        if oid and oid not in seen:
            seen.add(oid)
            unique_nodes.append(entry)
        elif not oid:
            unique_nodes.append(entry)

    print(f"  {len(all_nodes)} total, {len(unique_nodes)} unique nodes")

    # Fetch topics in batches
    oa_ids = [e["node"]["openalex_id"] for e in unique_nodes if e["node"].get("openalex_id")]
    print(f"\nFetching topics for {len(oa_ids)} papers...")

    id_topics = {}
    for i in range(0, len(oa_ids), 50):
        batch = oa_ids[i:i+50]
        batch_num = i // 50 + 1
        total_batches = (len(oa_ids) + 49) // 50
        print(f"  Batch {batch_num}/{total_batches}...")
        result = fetch_topics_batch(batch)
        id_topics.update(result)
        time.sleep(0.11)

    print(f"  Got topics for {len(id_topics)} papers")

    # Position each paper
    print("\nPositioning papers in 3D domain space...")
    positioned = []
    match_count = 0
    fallback_count = 0

    for entry in unique_nodes:
        node = entry["node"]
        oid = node.get("openalex_id", "")
        topics = id_topics.get(oid, [])

        # Also use title words as fallback topic source
        if node.get("title"):
            title_words = [w.lower() for w in re.split(r'\W+', node["title"]) if len(w) > 3]
            topics = topics + title_words

        pos = position_paper(topics, kw_index, disc_centroids)

        paper = {
            "openalex_id": oid,
            "doi": node.get("doi", ""),
            "title": node.get("title", ""),
            "year": node.get("year"),
            "cited_by_count": node.get("cited_by_count", 0),
            "type": entry["type"],
            "x": round(pos["x"], 4),
            "y": round(pos["y"], 4),
            "z": round(pos["z"], 4),
            "n_matches": pos["n_matches"],
            "disciplines": pos["disciplines"],
            "method": pos["method"],
        }
        if "parent_id" in entry:
            paper["parent_id"] = entry["parent_id"]

        positioned.append(paper)
        if pos["method"] == "keyword_avg":
            match_count += 1
        else:
            fallback_count += 1

    print(f"  Positioned: {match_count} by keywords, {fallback_count} by fallback")

    # Build edges
    edges = []
    center_id = center.get("openalex_id", "")

    for r in roots:
        rid = r.get("openalex_id", "")
        if rid:
            edges.append({"source": rid, "target": center_id, "type": "root"})

    for b in branches:
        bid = b.get("openalex_id", "")
        if bid:
            edges.append({"source": center_id, "target": bid, "type": "branch"})

    for parent_id, children in level_m2.items():
        for c in children:
            cid = c.get("openalex_id", "")
            if cid:
                edges.append({"source": cid, "target": parent_id, "type": "deep_root"})

    for parent_id, children in level_p2.items():
        for c in children:
            cid = c.get("openalex_id", "")
            if cid:
                edges.append({"source": parent_id, "target": cid, "type": "deep_branch"})

    print(f"  {len(edges)} edges")

    # Discipline centroids for reference spheres
    centroids_out = {k: v for k, v in disc_centroids.items() if k != "_global"}

    output = {
        "papers": positioned,
        "edges": edges,
        "centroids": centroids_out,
        "center_id": center_id,
        "stats": {
            "total_papers": len(positioned),
            "matched_by_keywords": match_count,
            "fallback_positioned": fallback_count,
            "total_edges": len(edges),
            "disciplines_represented": len(set(
                d for p in positioned for d in p["disciplines"]
            )),
        },
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\nSaved to {OUT}")
    print(f"  {output['stats']}")
    print("Done.")


if __name__ == "__main__":
    main()
