"""NCBI Taxonomy parser.

Downloads taxdump.tar.gz, parses nodes.dmp and names.dmp,
and filters to high-level ranks (superkingdom through order).
"""

import io
import tarfile
from pathlib import Path

from ..config import SOURCES, RAW_DIR, NCBI_RANK_HIERARCHY
from ..http_client import get_session

NCBI_URL = SOURCES["ncbi"]["ftp_url"]
TARBALL_PATH = RAW_DIR / "taxdump.tar.gz"

# Ranks we keep, mapped to their depth level
ALLOWED_RANKS = set(NCBI_RANK_HIERARCHY)
RANK_LEVEL = {r: i for i, r in enumerate(NCBI_RANK_HIERARCHY)}

# name_class values to collect as aliases
ALIAS_CLASSES = {"synonym", "genbank common name"}


def _download_tarball(session=None) -> Path:
    """Download taxdump.tar.gz if not already cached."""
    if TARBALL_PATH.exists():
        return TARBALL_PATH
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    session = session or get_session(use_cache=False)
    resp = session.get(NCBI_URL, stream=True, timeout=120)
    resp.raise_for_status()
    with open(TARBALL_PATH, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 16):
            f.write(chunk)
    print(f"  NCBI: downloaded taxdump.tar.gz ({TARBALL_PATH.stat().st_size // 1_000_000} MB)")
    return TARBALL_PATH


def _extract_file(tarball: Path, member_name: str) -> str:
    """Extract a single file from the tarball and return its text."""
    with tarfile.open(tarball, "r:gz") as tf:
        f = tf.extractfile(member_name)
        if f is None:
            raise FileNotFoundError(f"{member_name} not found in tarball")
        return f.read().decode("utf-8")


def _parse_nodes_text(text: str) -> dict[str, tuple[str, str]]:
    """Parse nodes.dmp text -> {tax_id: (parent_id, rank)}."""
    nodes = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t|\t")
        if len(parts) < 3:
            parts = line.rstrip("\t|\n").split("\t|\t")
        tax_id = parts[0].strip().rstrip("|").strip()
        parent_id = parts[1].strip().rstrip("|").strip()
        rank = parts[2].strip().rstrip("|").strip()
        nodes[tax_id] = (parent_id, rank)
    return nodes


def _parse_names_text(text: str) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Parse names.dmp text.

    Returns:
        scientific_names: {tax_id: scientific_name}
        aliases: {tax_id: [alias1, alias2, ...]}
    """
    scientific_names: dict[str, str] = {}
    aliases: dict[str, list[str]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t|\t")
        if len(parts) < 4:
            continue
        tax_id = parts[0].strip().rstrip("|").strip()
        name_txt = parts[1].strip().rstrip("|").strip()
        name_class = parts[3].strip().rstrip("\t|").strip()
        if name_class == "scientific name":
            scientific_names[tax_id] = name_txt
        elif name_class in ALIAS_CLASSES:
            aliases.setdefault(tax_id, []).append(name_txt)
    return scientific_names, aliases


def _find_filtered_parent(tax_id: str, nodes: dict[str, tuple[str, str]],
                          allowed: set[str]) -> str | None:
    """Walk up the tree to find the nearest ancestor whose rank is in allowed set."""
    current = tax_id
    visited = set()
    while current in nodes:
        if current in visited:
            return None  # cycle (root)
        visited.add(current)
        parent_id, _rank = nodes[current]
        if parent_id == current:
            return None  # root
        if parent_id in nodes:
            p_rank = nodes[parent_id][1]
            if p_rank in allowed:
                return parent_id
        current = parent_id
    return None


def _build_full_path(tax_id: str, nodes: dict[str, tuple[str, str]],
                     scientific_names: dict[str, str],
                     allowed: set[str]) -> str:
    """Build full_path by walking up through filtered ancestors."""
    parts = []
    current = tax_id
    visited = set()
    while current in nodes:
        if current in visited:
            break
        visited.add(current)
        _parent, rank = nodes[current]
        if rank in allowed:
            name = scientific_names.get(current, current)
            parts.append(name)
        parent_id = nodes[current][0]
        if parent_id == current:
            break
        current = parent_id
    parts.reverse()
    return " > ".join(parts)


def parse_ncbi(session=None, max_rank: str | None = None) -> list[dict]:
    """Download and parse NCBI taxonomy, filtered to high-level ranks.

    Args:
        session: Optional requests session.
        max_rank: Deepest rank to include (default: from config, typically "order").

    Returns:
        List of unified schema records.
    """
    max_rank = max_rank or SOURCES["ncbi"].get("max_rank", "order")
    max_level = RANK_LEVEL.get(max_rank, len(NCBI_RANK_HIERARCHY) - 1)
    allowed = {r for r in NCBI_RANK_HIERARCHY if RANK_LEVEL[r] <= max_level}

    session = session or get_session(use_cache=False)

    tarball = _download_tarball(session)
    print("  NCBI: parsing nodes.dmp...")
    nodes_text = _extract_file(tarball, "nodes.dmp")
    nodes = _parse_nodes_text(nodes_text)
    print(f"  NCBI: {len(nodes)} total nodes loaded")

    print("  NCBI: parsing names.dmp...")
    names_text = _extract_file(tarball, "names.dmp")
    scientific_names, aliases_map = _parse_names_text(names_text)
    print(f"  NCBI: {len(scientific_names)} scientific names loaded")

    # Filter to only allowed ranks
    filtered_ids = {tid for tid, (pid, rank) in nodes.items() if rank in allowed}
    print(f"  NCBI: {len(filtered_ids)} nodes at ranks {sorted(allowed)}")

    records = []
    for tax_id in sorted(filtered_ids, key=int):
        parent_id_raw, rank = nodes[tax_id]
        name = scientific_names.get(tax_id, f"taxid:{tax_id}")
        level = RANK_LEVEL.get(rank, 0)

        # Find parent within our filtered set
        parent_id = _find_filtered_parent(tax_id, nodes, allowed)

        full_path = _build_full_path(tax_id, nodes, scientific_names, allowed)

        node_aliases = aliases_map.get(tax_id, [])

        records.append({
            "id": tax_id,
            "label": name,
            "definition": None,
            "parent_id": parent_id,
            "type": rank,
            "uri": f"https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id={tax_id}",
            "full_path": full_path,
            "level": level,
            "aliases": node_aliases,
            "cross_refs": [],
            "version": None,
        })

    print(f"  NCBI total: {len(records)} taxonomy nodes (up to {max_rank} rank)")
    return records
