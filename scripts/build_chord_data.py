#!/usr/bin/env python3
"""Build chord diagram data: rich adjacency matrix of keyword flows.

Extracts multi-level flow data from the keyword taxonomy DuckDB:
  - 7 source pillars
  - 14 disciplines
  - Top subcategories per discipline
  - Source↔Discipline, Discipline↔Discipline, Source↔Source cross-links

Saves as JSON for the interactive D3 chord visualization.
"""

import json
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "lake" / "keywords.duckdb"
OUT_PATH = PROJECT / "data" / "viz" / "chord_data.json"

# Canonical source pillar names
SOURCE_MAP = {
    "NASA GCMD": "NASA GCMD",
    "UNESCO Thesaurus": "UNESCO",
    "OpenAlex": "OpenAlex",
    "MeSH": "MeSH",
    "Library of Congress": "LoC LCSH",
    "NCBI Taxonomy": "NCBI",
    "DOE OSTI": "DOE OSTI",
}

# Map keyword_senses.keyword_source → canonical pillar
SENSE_SOURCE_MAP = {
    "NASA GCMD": "NASA GCMD",
    "UNESCO Thesaurus": "UNESCO",
    "OpenAlex": "OpenAlex",
    "openalex": "OpenAlex",
    "MeSH": "MeSH",
    "Library of Congress": "LoC LCSH",
    "NCBI Taxonomy": "NCBI",
    "DOE OSTI": "DOE OSTI",
    "DOE_OSTI": "DOE OSTI",
    "WoS_natlab_pub": "WoS",
    "WoS_keywords_plus_vocab": "WoS",
    "WoS_publication": "WoS",
    "WoS_natlab_journal": "WoS",
    "WoS_journal": "WoS",
    "WoS_grant_agency": "WoS",
    "WoS_natlab_subcat": "WoS",
    "WoS_category": "WoS",
}

# Discipline display names
DISCIPLINE_NAMES = {
    "ee_me_engineering": "EE/ME Engineering",
    "chemical_sciences": "Chemical Sciences",
    "biological_medical": "Biological/Medical",
    "math_physics": "Math & Physics",
    "materials": "Materials",
    "earth_environmental": "Earth & Env",
    "computation_data": "Computing & Data",
    "policy_economics": "Policy & Econ",
    "fossil_energy": "Fossil Energy",
    "nuclear_particle": "Nuclear & Particle",
    "renewable_alternative": "Renewables",
    "space_atmospheric": "Space & Atmos",
    "coal_science": "Coal Science",
    "natural_gas": "Natural Gas",
    "general_science": "General Science",
}

# Color palette for sources
SOURCE_COLORS = {
    "NASA GCMD": "#4fc3f7",
    "UNESCO": "#81c784",
    "OpenAlex": "#ffb74d",
    "MeSH": "#f06292",
    "LoC LCSH": "#ba68c8",
    "NCBI": "#ff8a65",
    "DOE OSTI": "#4dd0e1",
    "WoS": "#aed581",
}

# Color palette for disciplines
DISCIPLINE_COLORS = {
    "fossil_energy": "#e53935",
    "materials": "#8e24aa",
    "earth_environmental": "#43a047",
    "chemical_sciences": "#fb8c00",
    "biological_medical": "#d81b60",
    "ee_me_engineering": "#1e88e5",
    "math_physics": "#00acc1",
    "computation_data": "#5e35b1",
    "policy_economics": "#c0ca33",
    "nuclear_particle": "#f4511e",
    "renewable_alternative": "#00897b",
    "space_atmospheric": "#3949ab",
    "coal_science": "#6d4c41",
    "natural_gas": "#546e7a",
    "general_science": "#78909c",
}


def connect_db() -> duckdb.DuckDBPyConnection:
    """Connect read-only, copying to temp if locked."""
    try:
        return duckdb.connect(str(DB_PATH), read_only=True)
    except duckdb.IOException:
        print("  DB locked — copying to temp file ...")
        tmp = Path(tempfile.mktemp(suffix=".duckdb"))
        shutil.copy2(DB_PATH, tmp)
        return duckdb.connect(str(tmp), read_only=True)


def main() -> None:
    print(f"Connecting to {DB_PATH} ...")
    con = connect_db()

    # ── 1. Source → Discipline flows ─────────────────────────────────
    print("Querying source → discipline flows ...")
    src_disc = con.execute("""
        SELECT k.source, ks.discipline_primary, COUNT(DISTINCT k.label) as cnt
        FROM keywords k
        JOIN keyword_senses ks ON LOWER(k.label) = LOWER(ks.keyword_label)
        WHERE ks.discipline_primary IS NOT NULL
        GROUP BY 1, 2
        ORDER BY cnt DESC
    """).fetchall()

    # Also get from keyword_senses directly for WoS-sourced senses
    src_disc_senses = con.execute("""
        SELECT keyword_source, discipline_primary, COUNT(DISTINCT keyword_label) as cnt
        FROM keyword_senses
        WHERE discipline_primary IS NOT NULL
        GROUP BY 1, 2
        ORDER BY cnt DESC
    """).fetchall()

    src_disc_map: dict[tuple[str, str], int] = {}
    for src, disc, cnt in src_disc:
        pillar = SOURCE_MAP.get(src, src)
        key = (pillar, disc)
        src_disc_map[key] = max(src_disc_map.get(key, 0), cnt)
    for src, disc, cnt in src_disc_senses:
        pillar = SENSE_SOURCE_MAP.get(src)
        if pillar is None:
            continue
        key = (pillar, disc)
        src_disc_map[key] = max(src_disc_map.get(key, 0), cnt)

    print(f"  Source→Discipline edges: {len(src_disc_map)}")

    # ── 2. Discipline → Discipline cross-links (polysemous) ─────────
    print("Querying discipline↔discipline cross-links ...")
    disc_disc = con.execute("""
        SELECT a.discipline_primary as d1, b.discipline_primary as d2,
               COUNT(DISTINCT a.keyword_label) as cnt
        FROM keyword_senses a
        JOIN keyword_senses b ON LOWER(a.keyword_label) = LOWER(b.keyword_label)
        WHERE a.discipline_primary < b.discipline_primary
          AND a.discipline_primary IS NOT NULL
          AND b.discipline_primary IS NOT NULL
        GROUP BY 1, 2
        ORDER BY cnt DESC
    """).fetchall()

    disc_disc_map = {(d1, d2): cnt for d1, d2, cnt in disc_disc}
    print(f"  Discipline↔Discipline edges: {len(disc_disc_map)}")

    # ── 3. Source → Source cross-links (shared keywords) ─────────────
    print("Querying source↔source cross-links ...")
    src_src = con.execute("""
        SELECT a.source as s1, b.source as s2, COUNT(DISTINCT LOWER(a.label)) as cnt
        FROM keywords a
        JOIN keywords b ON LOWER(a.label) = LOWER(b.label)
        WHERE a.source < b.source
        GROUP BY 1, 2
        ORDER BY cnt DESC
    """).fetchall()

    src_src_map: dict[tuple[str, str], int] = {}
    for s1, s2, cnt in src_src:
        p1 = SOURCE_MAP.get(s1, s1)
        p2 = SOURCE_MAP.get(s2, s2)
        if p1 == p2:
            continue
        key = tuple(sorted([p1, p2]))
        src_src_map[key] = src_src_map.get(key, 0) + cnt

    print(f"  Source↔Source edges: {len(src_src_map)}")

    # ── 4. Top subcategories per discipline ──────────────────────────
    print("Querying subcategories per discipline ...")
    # Try hierarchy_path or tier2/tier1 fields if available
    subcats = con.execute("""
        SELECT discipline_primary,
               CASE
                 WHEN origin_path LIKE '%>%>%'
                   THEN TRIM(split_part(origin_path, '>', 3))
                 WHEN origin_path LIKE '%>%'
                   THEN TRIM(split_part(origin_path, '>', 2))
                 ELSE TRIM(split_part(origin_path, '>', 1))
               END as subcat,
               COUNT(*) as cnt
        FROM keyword_senses
        WHERE discipline_primary IS NOT NULL
          AND origin_path IS NOT NULL
          AND origin_path != ''
        GROUP BY 1, 2
        HAVING cnt >= 50
        ORDER BY discipline_primary, cnt DESC
    """).fetchall()

    # Keep top 3 subcategories per discipline
    subcat_map: dict[str, list[tuple[str, int]]] = {}
    for disc, subcat, cnt in subcats:
        if disc not in subcat_map:
            subcat_map[disc] = []
        if len(subcat_map[disc]) < 3:
            subcat_map[disc].append((subcat, cnt))

    total_subcats = sum(len(v) for v in subcat_map.values())
    print(f"  Subcategories: {total_subcats} across {len(subcat_map)} disciplines")

    # ── 5. Discipline → Subcategory flows ────────────────────────────
    disc_subcat_map: dict[tuple[str, str], int] = {}
    for disc, subs in subcat_map.items():
        for subcat, cnt in subs:
            disc_subcat_map[(disc, subcat)] = cnt

    # ── 6. Global stats ─────────────────────────────────────────────
    total_kw = con.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
    total_senses = con.execute("SELECT COUNT(*) FROM keyword_senses").fetchone()[0]

    poly_disc = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT keyword_label FROM keyword_senses
            GROUP BY keyword_label
            HAVING COUNT(DISTINCT discipline_primary) >= 2
        )
    """).fetchone()[0]

    poly_src = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT LOWER(label) as lbl FROM keywords
            GROUP BY lbl
            HAVING COUNT(DISTINCT source) >= 2
        )
    """).fetchone()[0]

    # Source counts
    source_counts = {}
    for src, cnt in con.execute(
        "SELECT source, COUNT(*) FROM keywords GROUP BY source"
    ).fetchall():
        name = SOURCE_MAP.get(src, src)
        source_counts[name] = source_counts.get(name, 0) + cnt

    # Discipline counts
    disc_counts = {}
    for disc, cnt in con.execute(
        "SELECT discipline_primary, COUNT(*) FROM keyword_senses "
        "WHERE discipline_primary IS NOT NULL GROUP BY discipline_primary"
    ).fetchall():
        disc_counts[disc] = cnt

    # Add WoS from sense sources
    all_sources = set(source_counts.keys())
    for (pillar, _), _ in src_disc_map.items():
        all_sources.add(pillar)

    con.close()

    # ── 7. Build node list ───────────────────────────────────────────
    print("Building adjacency matrix ...")

    nodes = []
    node_idx = {}

    # Source nodes
    for src in sorted(all_sources):
        idx = len(nodes)
        node_id = f"src_{src.lower().replace(' ', '_').replace('/', '_')}"
        node_idx[node_id] = idx
        flow_total = sum(v for (s, _), v in src_disc_map.items() if s == src)
        nodes.append({
            "id": node_id,
            "name": src,
            "type": "source",
            "group": "Sources",
            "size": source_counts.get(src, flow_total),
            "color": SOURCE_COLORS.get(src, "#90a4ae"),
        })

    # Discipline nodes
    for disc in sorted(disc_counts.keys(), key=lambda d: -disc_counts[d]):
        if disc == "general_science" and disc_counts.get(disc, 0) < 5:
            continue
        idx = len(nodes)
        node_id = f"disc_{disc}"
        node_idx[node_id] = idx
        nodes.append({
            "id": node_id,
            "name": DISCIPLINE_NAMES.get(disc, disc),
            "type": "discipline",
            "group": "Disciplines",
            "size": disc_counts[disc],
            "color": DISCIPLINE_COLORS.get(disc, "#78909c"),
        })

    # Subcategory nodes
    for disc, subs in sorted(subcat_map.items()):
        disc_color = DISCIPLINE_COLORS.get(disc, "#78909c")
        for subcat, cnt in subs:
            idx = len(nodes)
            # Truncate long names
            display = subcat if len(subcat) <= 25 else subcat[:22] + "..."
            node_id = f"sub_{disc}_{subcat.lower().replace(' ', '_')[:30]}"
            node_idx[node_id] = idx
            nodes.append({
                "id": node_id,
                "name": display,
                "type": "subcategory",
                "group": DISCIPLINE_NAMES.get(disc, disc),
                "size": cnt,
                "color": disc_color,
                "parent_discipline": disc,
            })

    n = len(nodes)
    matrix = [[0] * n for _ in range(n)]

    # Fill source → discipline
    for (src, disc), cnt in src_disc_map.items():
        src_id = f"src_{src.lower().replace(' ', '_').replace('/', '_')}"
        disc_id = f"disc_{disc}"
        if src_id in node_idx and disc_id in node_idx:
            i, j = node_idx[src_id], node_idx[disc_id]
            matrix[i][j] = cnt
            matrix[j][i] = cnt

    # Fill discipline → discipline
    for (d1, d2), cnt in disc_disc_map.items():
        id1 = f"disc_{d1}"
        id2 = f"disc_{d2}"
        if id1 in node_idx and id2 in node_idx:
            i, j = node_idx[id1], node_idx[id2]
            matrix[i][j] = cnt
            matrix[j][i] = cnt

    # Fill source → source
    for (s1, s2), cnt in src_src_map.items():
        id1 = f"src_{s1.lower().replace(' ', '_').replace('/', '_')}"
        id2 = f"src_{s2.lower().replace(' ', '_').replace('/', '_')}"
        if id1 in node_idx and id2 in node_idx:
            i, j = node_idx[id1], node_idx[id2]
            matrix[i][j] = cnt
            matrix[j][i] = cnt

    # Fill discipline → subcategory
    for (disc, subcat), cnt in disc_subcat_map.items():
        disc_id = f"disc_{disc}"
        sub_id = f"sub_{disc}_{subcat.lower().replace(' ', '_')[:30]}"
        if disc_id in node_idx and sub_id in node_idx:
            i, j = node_idx[disc_id], node_idx[sub_id]
            matrix[i][j] = cnt
            matrix[j][i] = cnt

    # ── 8. Write output ──────────────────────────────────────────────
    data = {
        "nodes": nodes,
        "matrix": matrix,
        "stats": {
            "total_keywords": total_kw,
            "total_senses": total_senses,
            "polysemous_multi_discipline": poly_disc,
            "polysemous_multi_source": poly_src,
            "num_sources": len([n for n in nodes if n["type"] == "source"]),
            "num_disciplines": len([n for n in nodes if n["type"] == "discipline"]),
            "num_subcategories": len([n for n in nodes if n["type"] == "subcategory"]),
            "num_nodes": n,
            "total_connections": sum(
                1 for i in range(n) for j in range(i + 1, n) if matrix[i][j] > 0
            ),
        },
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nSaved to {OUT_PATH}")
    print(f"  Nodes: {n}")
    print(f"  Matrix size: {n}x{n}")
    print(f"  Non-zero connections: {data['stats']['total_connections']}")
    print(f"  Stats: {json.dumps(data['stats'], indent=2)}")


if __name__ == "__main__":
    main()
