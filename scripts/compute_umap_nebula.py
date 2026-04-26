#!/usr/bin/env python3
"""Compute UMAP 3D projection of semantic embeddings for nebula visualization.

Reads ~63K embeddings from DuckDB, projects to 3D via UMAP, enriches with
discipline info from keyword_senses, and saves as JSON for Three.js / WebGL.

Usage:
    python3 -u scripts/compute_umap_nebula.py
"""

import json
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "lake" / "keywords.duckdb"
OUT_PATH = Path(__file__).resolve().parent.parent / "data" / "viz" / "nebula_data.json"


def main():
    t0 = time.time()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # ── Connect read-only ──────────────────────────────────────────────
    print(f"Connecting to {DB_PATH} (read-only)...")
    con = duckdb.connect(str(DB_PATH), read_only=True)

    # ── 1. Load embeddings from .npy + labels from .txt ──────────────
    lake_dir = DB_PATH.parent
    emb_path = lake_dir / "keyword_embeddings.npy"
    lbl_path = lake_dir / "keyword_labels.txt"
    print(f"Loading embeddings from {emb_path.name} + {lbl_path.name}...")
    embeddings = np.load(str(emb_path)).astype(np.float32)
    with open(lbl_path) as f:
        labels = [line.strip() for line in f if line.strip()]
    n = len(labels)
    assert n == embeddings.shape[0], f"Label count {n} != embedding rows {embeddings.shape[0]}"
    print(f"  → {n:,} embeddings loaded")
    print(f"  → Embedding matrix shape: {embeddings.shape}")

    # ── 2. Load discipline info per keyword_label ──────────────────────
    print("Loading discipline info from keyword_senses...")
    sense_rows = con.sql("""
        SELECT keyword_label, sense_id, discipline_primary
        FROM keyword_senses
    """).fetchall()

    # Build label → (discipline, sense_id) lookup (first match wins)
    label_discipline: dict[str, str] = {}
    label_sense_id: dict[str, str] = {}
    for kw_label, sense_id, disc in sense_rows:
        if kw_label not in label_discipline and disc:
            label_discipline[kw_label] = disc
            label_sense_id[kw_label] = sense_id
    print(f"  → {len(label_discipline):,} labels have discipline assignment")

    # ── 3. Load lenses ─────────────────────────────────────────────────
    print("Loading ontology lenses...")
    lens_rows = con.sql("""
        SELECT lens_id, name, discipline_primary, role_type
        FROM ontology_lenses
        ORDER BY discipline_primary, role_type, name
    """).fetchall()
    lenses = []
    for lens_id, name, disc, role in lens_rows:
        lenses.append({
            "lens_id": lens_id,
            "name": name,
            "discipline": disc or "unassigned",
            "role": role or "unknown",
        })
    print(f"  → {len(lenses)} lenses loaded")

    # ── 4. Build discipline → keyword indices for lens emphasis ────────
    print("Building discipline → keyword index mapping...")
    # For each lens, keywords emphasized = those whose discipline matches lens discipline
    disc_to_indices: dict[str, list[int]] = defaultdict(list)
    for i, lbl in enumerate(labels):
        disc = label_discipline.get(lbl)
        if disc:
            disc_to_indices[disc].append(i)

    # Count keywords per lens
    for lens in lenses:
        lens["keyword_count"] = len(disc_to_indices.get(lens["discipline"], []))

    # Build lens_id → list of point indices
    lens_keyword_map: dict[str, list[int]] = {}
    for lens in lenses:
        indices = disc_to_indices.get(lens["discipline"], [])
        if indices:
            lens_keyword_map[lens["lens_id"]] = indices

    # ── 5. Run UMAP ───────────────────────────────────────────────────
    print(f"Running UMAP (n={n:,}, 3D, cosine, n_neighbors=30, min_dist=0.3)...")
    import umap

    t_umap = time.time()
    reducer = umap.UMAP(
        n_components=3,
        n_neighbors=30,
        min_dist=0.3,
        metric="cosine",
        random_state=42,
        verbose=True,
    )
    coords = reducer.fit_transform(embeddings)
    print(f"  → UMAP done in {time.time() - t_umap:.1f}s")
    print(f"  → Output shape: {coords.shape}")

    # ── 6. Assemble output ─────────────────────────────────────────────
    print("Assembling JSON output...")
    all_disciplines = sorted(set(label_discipline.values()))

    points = []
    for i in range(n):
        lbl = labels[i]
        points.append({
            "label": lbl,
            "x": round(float(coords[i, 0]), 5),
            "y": round(float(coords[i, 1]), 5),
            "z": round(float(coords[i, 2]), 5),
            "discipline": label_discipline.get(lbl, "unassigned"),
            "sense_id": label_sense_id.get(lbl, ""),
        })

    output = {
        "points": points,
        "lenses": lenses,
        "disciplines": all_disciplines,
        "lens_keyword_indices": lens_keyword_map,
        "stats": {
            "n_points": n,
            "n_lenses": len(lenses),
            "n_disciplines": len(all_disciplines),
        },
    }

    # ── 7. Write JSON ──────────────────────────────────────────────────
    print(f"Writing {OUT_PATH}...")
    with open(OUT_PATH, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_mb = OUT_PATH.stat().st_size / (1024 * 1024)
    print(f"  → {size_mb:.1f} MB written")
    print(f"  → {n:,} points, {len(lenses)} lenses, {len(all_disciplines)} disciplines")
    print(f"Total time: {time.time() - t0:.1f}s")

    con.close()


if __name__ == "__main__":
    main()
