"""NIH Medical Subject Headings (MeSH) parser.

Reads pre-downloaded MeSH descriptor JSON (from scripts/download_parse_mesh.py)
and produces unified keyword records + raw table records.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

MESH_JSON = Path("data/raw/mesh/mesh_descriptors.json")

MESH_TREE_ROOTS = {
    "A": "Anatomy",
    "B": "Organisms",
    "C": "Diseases",
    "D": "Chemicals and Drugs",
    "E": "Analytical, Diagnostic and Therapeutic Techniques, and Equipment",
    "F": "Psychiatry and Psychology",
    "G": "Phenomena and Processes",
    "H": "Disciplines and Occupations",
    "I": "Anthropology, Education, Sociology, and Social Phenomena",
    "J": "Technology, Industry, and Agriculture",
    "K": "Humanities",
    "L": "Information Science",
    "M": "Named Groups",
    "N": "Health Care",
    "V": "Publication Characteristics",
    "Z": "Geographicals",
}


def _tree_depth(tree_numbers: list[str]) -> int:
    """Minimum depth across all tree numbers (number of dots + 1, minus 1 for 0-indexed)."""
    if not tree_numbers:
        return 0
    return min(tn.count(".") for tn in tree_numbers)


def _parent_tree(tree_number: str) -> str | None:
    """Get parent tree number by removing last segment."""
    parts = tree_number.rsplit(".", 1)
    return parts[0] if len(parts) > 1 else None


def _category_from_tree(tree_numbers: list[str]) -> str:
    """Extract primary MeSH category from first tree number."""
    if not tree_numbers:
        return "Unknown"
    letter = tree_numbers[0][0]
    name = MESH_TREE_ROOTS.get(letter, "Unknown")
    return f"{letter} ({name})"


def parse_mesh(path: Path | None = None) -> list[dict]:
    """Parse MeSH JSON into unified keyword records."""
    path = path or MESH_JSON
    with open(path) as f:
        descriptors = json.load(f)

    logger.info("Parsing %d MeSH descriptors", len(descriptors))

    records = []
    for desc in descriptors:
        tree_nums = desc.get("tree_numbers", [])
        depth = _tree_depth(tree_nums)

        parent_trees = set()
        for tn in tree_nums:
            pt = _parent_tree(tn)
            if pt:
                parent_trees.add(pt)

        category = _category_from_tree(tree_nums)
        entries = desc.get("entries", [])

        full_path = None
        if tree_nums:
            full_path = f"MeSH > {category} > {desc['heading']}"

        records.append({
            "id": desc["ui"],
            "label": desc["heading"],
            "definition": desc.get("scope_note"),
            "parent_id": None,
            "source": "MeSH",
            "type": category,
            "uri": f"https://meshb.nlm.nih.gov/record/ui?ui={desc['ui']}",
            "full_path": full_path,
            "aliases": entries if entries else None,
            "level": depth,
            "cross_refs": None,
            "last_updated": None,
            "version": "2026",
        })

    logger.info("Parsed %d MeSH keywords", len(records))
    return records


def parse_mesh_raw(path: Path | None = None) -> list[dict]:
    """Parse MeSH JSON into raw table records (full fidelity)."""
    path = path or MESH_JSON
    with open(path) as f:
        descriptors = json.load(f)

    tree_to_ui: dict[str, str] = {}
    for desc in descriptors:
        for tn in desc.get("tree_numbers", []):
            tree_to_ui[tn] = desc["ui"]

    records = []
    for desc in descriptors:
        tree_nums = desc.get("tree_numbers", [])
        depth = _tree_depth(tree_nums)

        parent_uis = set()
        for tn in tree_nums:
            pt = _parent_tree(tn)
            if pt and pt in tree_to_ui:
                parent_uis.add(tree_to_ui[pt])

        records.append({
            "ui": desc["ui"],
            "heading": desc["heading"],
            "tree_numbers": tree_nums if tree_nums else None,
            "scope_note": desc.get("scope_note"),
            "entries": desc.get("entries") or None,
            "mesh_category": _category_from_tree(tree_nums),
            "tree_depth": depth,
            "parent_uis": list(parent_uis) if parent_uis else None,
        })

    return records


def ingest_raw_mesh(conn, path: Path | None = None):
    """Insert MeSH raw records into raw_mesh table."""
    records = parse_mesh_raw(path)
    logger.info("Inserting %d MeSH raw records", len(records))

    conn.execute("DELETE FROM raw_mesh")

    batch_size = 2000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        conn.executemany(
            """INSERT INTO raw_mesh (ui, heading, tree_numbers, scope_note,
               entries, mesh_category, tree_depth, parent_uis)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [(r["ui"], r["heading"], r["tree_numbers"], r["scope_note"],
              r["entries"], r["mesh_category"], r["tree_depth"], r["parent_uis"])
             for r in batch]
        )

    count = conn.execute("SELECT COUNT(*) FROM raw_mesh").fetchone()[0]
    logger.info("Inserted %d raw MeSH records", count)
    return count
