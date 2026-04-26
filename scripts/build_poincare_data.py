#!/usr/bin/env python3
"""Build Poincaré disk embedding data from keyword taxonomy hierarchy.

Extracts discipline→keyword hierarchy from DuckDB and computes
hyperbolic coordinates for Poincaré disk visualization.
Includes cross-discipline edges for polysemous keywords.

Output: data/viz/poincare_data.json
"""

import json
import math
import os
import random
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("DB_OVERRIDE", PROJECT / "data" / "lake" / "keywords.duckdb"))
OUT_PATH = PROJECT / "data" / "viz" / "poincare_data.json"

KEYWORDS_PER_DISCIPLINE = 100

# Human-readable discipline labels
DISC_LABELS = {
    "biological_medical": "Biological & Medical",
    "chemical_sciences": "Chemical Sciences",
    "coal_science": "Coal Science",
    "computation_data": "Computation & Data",
    "earth_environmental": "Earth & Environmental",
    "ee_me_engineering": "EE/ME Engineering",
    "fossil_energy": "Fossil Energy",
    "general_science": "General Science",
    "materials": "Materials",
    "math_physics": "Math & Physics",
    "natural_gas": "Natural Gas",
    "nuclear_particle": "Nuclear & Particle",
    "policy_economics": "Policy & Economics",
    "renewable_alternative": "Renewable & Alternative",
    "space_atmospheric": "Space & Atmospheric",
}


def poincare_coords(level: float, angle: float, jitter: float = 0.0) -> tuple[float, float]:
    """Compute Poincaré disk coordinates using tanh mapping.

    level 0 = origin, level 1 ~ r=0.3, level 2 ~ r=0.6-0.9
    """
    r = math.tanh(level * 0.5)
    x = r * math.cos(angle) + jitter * (random.random() - 0.5)
    y = r * math.sin(angle) + jitter * (random.random() - 0.5)
    # Clamp inside unit disk
    mag = math.sqrt(x * x + y * y)
    if mag >= 0.98:
        x *= 0.97 / mag
        y *= 0.97 / mag
    return round(x, 6), round(y, 6)


def main():
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
    except duckdb.IOException:
        # DB locked — copy to temp and open from there
        print(f"DB locked, copying to temp file...")
        tmp = Path(tempfile.mktemp(suffix=".duckdb"))
        shutil.copy2(DB_PATH, tmp)
        con = duckdb.connect(str(tmp), read_only=True)

    # --- Get top keywords per discipline ---
    rows = con.execute("""
        WITH ranked AS (
            SELECT
                discipline_primary,
                keyword_label,
                COUNT(*) as rel_count,
                ROW_NUMBER() OVER (
                    PARTITION BY discipline_primary
                    ORDER BY COUNT(*) DESC
                ) as rn
            FROM keyword_senses ks
            JOIN sense_relationships sr ON ks.sense_id = sr.source_sense_id
            WHERE discipline_primary IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT discipline_primary, keyword_label, rel_count
        FROM ranked
        WHERE rn <= ?
        ORDER BY discipline_primary, rel_count DESC
    """, [KEYWORDS_PER_DISCIPLINE]).fetchall()

    # --- Get disciplines and their total keyword counts ---
    disc_counts = con.execute("""
        SELECT discipline_primary, COUNT(DISTINCT keyword_label) as kw_count
        FROM keyword_senses
        WHERE discipline_primary IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).fetchall()
    disc_count_map = {d: c for d, c in disc_counts}

    # --- Get cross-discipline polysemous keywords ---
    # Keywords appearing in multiple disciplines (among our selected keywords)
    selected_keywords = set((disc, kw) for disc, kw, _ in rows)
    selected_labels = set(kw for _, kw, _ in rows)

    cross_rows = con.execute("""
        SELECT a.keyword_label,
               a.discipline_primary as d1,
               b.discipline_primary as d2
        FROM keyword_senses a
        JOIN keyword_senses b ON LOWER(a.keyword_label) = LOWER(b.keyword_label)
        WHERE a.discipline_primary < b.discipline_primary
          AND a.sense_id != b.sense_id
          AND a.discipline_primary IS NOT NULL
          AND b.discipline_primary IS NOT NULL
    """).fetchall()

    # Build map: keyword_label -> set of disciplines it appears in
    kw_disciplines: dict[str, set[str]] = defaultdict(set)
    for kw_label, d1, d2 in cross_rows:
        kw_disciplines[kw_label].add(d1)
        kw_disciplines[kw_label].add(d2)

    # --- Get lenses ---
    lenses = con.execute("""
        SELECT lens_id, name, discipline_primary, role_type
        FROM ontology_lenses
        WHERE discipline_primary IS NOT NULL
        ORDER BY discipline_primary, role_type
    """).fetchall()

    con.close()

    # --- Build hierarchy ---
    disciplines = sorted(set(r[0] for r in rows))
    n_disc = len(disciplines)

    nodes = []
    edges = []

    # Root node
    nodes.append({
        "id": "root",
        "label": "Keyword Ontology",
        "level": 0,
        "x": 0.0,
        "y": 0.0,
        "type": "root",
    })

    # Discipline nodes at level 1
    disc_angles = {}
    for i, disc in enumerate(disciplines):
        angle = 2 * math.pi * i / n_disc
        disc_angles[disc] = angle
        x, y = poincare_coords(1, angle)
        disc_id = f"disc_{disc}"
        nodes.append({
            "id": disc_id,
            "label": DISC_LABELS.get(disc, disc.replace("_", " ").title()),
            "level": 1,
            "x": x,
            "y": y,
            "type": "discipline",
            "discipline": disc,
            "keyword_count": disc_count_map.get(disc, 0),
        })
        edges.append({"source": "root", "target": disc_id, "type": "hierarchy"})

    # Keyword nodes at level 2
    # Group rows by discipline
    disc_keywords: dict[str, list[tuple[str, int]]] = {}
    for disc, kw, rc in rows:
        disc_keywords.setdefault(disc, []).append((kw, rc))

    # Build keyword_id lookup: (discipline, keyword_label) -> node_id
    kw_node_ids: dict[tuple[str, str], str] = {}

    random.seed(42)  # Reproducible jitter

    for disc, kws in disc_keywords.items():
        base_angle = disc_angles[disc]
        n_kw = len(kws)
        # Spread keywords in a sector around the discipline angle
        sector_width = (2 * math.pi / n_disc) * 0.85  # 85% of allocated sector

        for j, (kw, rc) in enumerate(kws):
            # Angular position within sector
            if n_kw > 1:
                kw_angle = base_angle - sector_width / 2 + sector_width * j / (n_kw - 1)
            else:
                kw_angle = base_angle

            # Radius varies by rel_count rank: top keywords closer to discipline
            # Level ranges from ~1.8 (top) to ~2.5 (bottom)
            rank_frac = j / max(n_kw - 1, 1)
            level = 1.8 + rank_frac * 0.7

            x, y = poincare_coords(level, kw_angle, jitter=0.015)
            kw_id = f"kw_{disc}_{j}"
            kw_node_ids[(disc, kw)] = kw_id

            # Which other disciplines does this keyword appear in?
            other_discs = sorted(kw_disciplines.get(kw, set()) - {disc})

            nodes.append({
                "id": kw_id,
                "label": kw,
                "level": 2,
                "x": x,
                "y": y,
                "type": "keyword",
                "discipline": disc,
                "rel_count": rc,
                "other_disciplines": other_discs,
            })
            edges.append({"source": f"disc_{disc}", "target": kw_id, "type": "hierarchy"})

    # --- Cross-discipline edges ---
    cross_edges = []
    seen_pairs = set()
    for kw_label, d1, d2 in cross_rows:
        # Only add if both keyword nodes exist in our selected set
        id1 = kw_node_ids.get((d1, kw_label))
        id2 = kw_node_ids.get((d2, kw_label))
        if id1 and id2:
            pair_key = (min(id1, id2), max(id1, id2))
            if pair_key not in seen_pairs:
                seen_pairs.add(pair_key)
                cross_edges.append({
                    "source": id1,
                    "target": id2,
                    "type": "cross_discipline",
                    "keyword": kw_label,
                })

    edges.extend(cross_edges)

    # --- Format lenses ---
    lens_list = [
        {
            "lens_id": lid,
            "name": name,
            "discipline": disc,
            "role": role,
        }
        for lid, name, disc, role in lenses
    ]

    data = {
        "nodes": nodes,
        "edges": edges,
        "lenses": lens_list,
        "meta": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "hierarchy_edges": len(edges) - len(cross_edges),
            "cross_discipline_edges": len(cross_edges),
            "disciplines": len(disciplines),
            "keywords_per_discipline": KEYWORDS_PER_DISCIPLINE,
        },
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Wrote {OUT_PATH}")
    print(f"  {len(nodes)} nodes ({len(disciplines)} disciplines + {len(nodes) - len(disciplines) - 1} keywords)")
    print(f"  {len(edges)} edges ({len(edges) - len(cross_edges)} hierarchy + {len(cross_edges)} cross-discipline)")
    print(f"  {len(lens_list)} lenses")


if __name__ == "__main__":
    main()
