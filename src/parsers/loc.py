"""Library of Congress Subject Headings parser — bulk download approach.

Downloads the full LCSH SKOS N-Triples file (~101MB gz, ~1GB uncompressed),
stream-parses it line by line, then filters to Science and Technology
subtrees via BFS from root IDs.
"""

import gzip
import json
import re
import logging
from collections import defaultdict
from pathlib import Path

from ..config import SOURCES, RAW_DIR
from ..http_client import get_session

logger = logging.getLogger(__name__)

ROOT_IDS = SOURCES["loc"]["root_ids"]
BULK_URL = "https://id.loc.gov/download/authorities/subjects.skosrdf.nt.gz"
BULK_PATH = RAW_DIR / "lcsh-skos.nt.gz"

SKOS = "http://www.w3.org/2004/02/skos/core#"
NT_PATTERN = re.compile(
    r'^<([^>]+)>\s+<([^>]+)>\s+(?:<([^>]+)>|"((?:[^"\\]|\\.)*)"\s*(?:@(\w+))?\s*(?:\^\^<[^>]+>)?)\s*\.\s*$'
)

PREDICATES_OF_INTEREST = {
    f"{SKOS}prefLabel",
    f"{SKOS}altLabel",
    f"{SKOS}broader",
    f"{SKOS}narrower",
    f"{SKOS}related",
    f"{SKOS}note",
    f"{SKOS}scopeNote",
    f"{SKOS}definition",
    f"{SKOS}notation",
}

LCSH_PREFIX = "http://id.loc.gov/authorities/subjects/"


def _extract_id(uri: str) -> str | None:
    """Extract sh-ID from a full LoC URI. Returns None if not an LCSH URI."""
    if uri.startswith(LCSH_PREFIX):
        return uri[len(LCSH_PREFIX):]
    return None


def _download_bulk(session=None) -> Path:
    """Download the LCSH SKOS N-Triples bulk file if not cached."""
    if BULK_PATH.exists():
        size_mb = BULK_PATH.stat().st_size / (1024 * 1024)
        print(f"  LoC bulk file already cached ({size_mb:.0f} MB): {BULK_PATH}")
        return BULK_PATH

    BULK_PATH.parent.mkdir(parents=True, exist_ok=True)
    session = session or get_session(cache_name="loc_bulk", use_cache=False)

    print(f"  Downloading LCSH bulk file (~101 MB)...")
    resp = session.get(BULK_URL, stream=True, timeout=300)
    resp.raise_for_status()

    downloaded = 0
    with open(BULK_PATH, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            mb = downloaded / (1024 * 1024)
            if int(mb) % 10 == 0 and int(mb) > 0:
                print(f"    {mb:.0f} MB downloaded...", end="\r")

    size_mb = BULK_PATH.stat().st_size / (1024 * 1024)
    print(f"  Downloaded {size_mb:.0f} MB to {BULK_PATH}")
    return BULK_PATH


def _parse_nt_line(line: str) -> tuple | None:
    """Parse a single N-Triples line into (subject, predicate, object_uri_or_literal, lang)."""
    m = NT_PATTERN.match(line)
    if not m:
        return None
    subj, pred, obj_uri, obj_lit, lang = m.groups()
    if obj_uri:
        return (subj, pred, obj_uri, None)
    if obj_lit:
        obj_lit = obj_lit.replace('\\"', '"').replace('\\\\', '\\')
        return (subj, pred, obj_lit, lang)
    return None


def _stream_parse(gz_path: Path) -> dict:
    """Stream-parse N-Triples file, extracting only LCSH subjects with SKOS predicates.

    Returns a dict of dicts keyed by sh-ID with collected properties.
    """
    labels = {}          # id -> prefLabel (en preferred)
    alt_labels = defaultdict(list)
    broader = defaultdict(set)   # id -> set of broader ids
    narrower = defaultdict(set)
    related = defaultdict(set)
    notes = {}           # id -> first note/scopeNote
    definitions = {}     # id -> definition

    line_count = 0
    relevant_count = 0

    print(f"  Parsing {gz_path.name}...")
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            line_count += 1
            if line_count % 1_000_000 == 0:
                print(f"    {line_count / 1_000_000:.0f}M lines processed, {relevant_count} relevant triples...")

            parsed = _parse_nt_line(line)
            if not parsed:
                continue

            subj_uri, pred, obj, lang = parsed

            if pred not in PREDICATES_OF_INTEREST:
                continue

            subj_id = _extract_id(subj_uri)
            if not subj_id:
                continue

            relevant_count += 1

            if pred == f"{SKOS}prefLabel":
                if lang == "en" or subj_id not in labels:
                    labels[subj_id] = obj
            elif pred == f"{SKOS}altLabel":
                if lang is None or lang == "en":
                    alt_labels[subj_id].append(obj)
            elif pred == f"{SKOS}broader":
                broader_id = _extract_id(obj)
                if broader_id:
                    broader[subj_id].add(broader_id)
            elif pred == f"{SKOS}narrower":
                narrower_id = _extract_id(obj)
                if narrower_id:
                    narrower[subj_id].add(narrower_id)
            elif pred == f"{SKOS}related":
                related_id = _extract_id(obj)
                if related_id:
                    related[subj_id].add(related_id)
            elif pred in (f"{SKOS}note", f"{SKOS}scopeNote"):
                if subj_id not in notes and (lang is None or lang == "en"):
                    notes[subj_id] = obj
            elif pred == f"{SKOS}definition":
                if subj_id not in definitions and (lang is None or lang == "en"):
                    definitions[subj_id] = obj

    print(f"  Parsed {line_count} lines, {relevant_count} relevant triples")
    print(f"  {len(labels)} subjects with labels, {sum(len(v) for v in broader.values())} broader links")

    return {
        "labels": labels,
        "alt_labels": dict(alt_labels),
        "broader": {k: list(v) for k, v in broader.items()},
        "narrower": {k: list(v) for k, v in narrower.items()},
        "related": {k: list(v) for k, v in related.items()},
        "notes": notes,
        "definitions": definitions,
    }


def _bfs_subtree(root_ids: list[str], narrower: dict[str, list[str]], broader: dict[str, list[str]]) -> set[str]:
    """BFS from root IDs to find all descendants using narrower and reverse-broader links."""
    # Build a children map from both narrower and reverse-broader
    children = defaultdict(set)
    for parent_id, child_ids in narrower.items():
        for cid in child_ids:
            children[parent_id].add(cid)
    for child_id, parent_ids in broader.items():
        for pid in parent_ids:
            children[pid].add(child_id)

    visited = set()
    queue = list(root_ids)
    for rid in queue:
        visited.add(rid)

    while queue:
        current = queue.pop(0)
        for child in children.get(current, []):
            if child not in visited:
                visited.add(child)
                queue.append(child)

    return visited


def _build_paths(subject_ids: set[str], broader: dict[str, list[str]], labels: dict[str, str], root_ids: set[str]) -> dict[str, str]:
    """Build full_path for each subject by walking broader chains to a root."""
    paths = {}

    def _walk(sid, seen=None):
        if sid in paths:
            return paths[sid]
        if seen is None:
            seen = set()
        if sid in seen:
            return labels.get(sid, sid)
        seen.add(sid)

        label = labels.get(sid, sid)
        if sid in root_ids:
            paths[sid] = label
            return label

        parent_ids = broader.get(sid, [])
        valid_parents = [p for p in parent_ids if p in subject_ids]
        if not valid_parents:
            paths[sid] = label
            return label

        parent_path = _walk(valid_parents[0], seen)
        paths[sid] = f"{parent_path} > {label}"
        return paths[sid]

    for sid in subject_ids:
        _walk(sid)

    return paths


def _compute_levels(subject_ids: set[str], broader: dict[str, list[str]], root_ids: set[str]) -> dict[str, int]:
    """Compute hierarchy level (0=root) via BFS from roots."""
    levels = {rid: 0 for rid in root_ids if rid in subject_ids}

    children = defaultdict(set)
    for child_id, parent_ids in broader.items():
        if child_id in subject_ids:
            for pid in parent_ids:
                if pid in subject_ids:
                    children[pid].add(child_id)

    queue = [rid for rid in root_ids if rid in subject_ids]
    while queue:
        current = queue.pop(0)
        for child in children.get(current, []):
            if child not in levels:
                levels[child] = levels[current] + 1
                queue.append(child)

    return levels


def parse_loc(session=None, max_depth: int = 6) -> list[dict]:
    """Download and parse LCSH bulk file, filtering to Science & Technology subtrees.

    Args:
        session: Optional requests session for downloading.
        max_depth: Maximum hierarchy depth to include (default 6).

    Returns:
        List of unified schema records.
    """
    gz_path = _download_bulk(session)
    data = _stream_parse(gz_path)

    labels = data["labels"]
    broader = data["broader"]
    narrower = data["narrower"]
    related = data["related"]
    alt_labels = data["alt_labels"]
    notes = data["notes"]
    definitions = data["definitions"]

    root_set = set(ROOT_IDS)
    missing_roots = root_set - set(labels.keys())
    if missing_roots:
        print(f"  WARNING: root IDs not found in bulk data: {missing_roots}")

    print(f"  Finding Science & Technology subtrees from {len(ROOT_IDS)} roots...")
    subtree_ids = _bfs_subtree(ROOT_IDS, narrower, broader)
    print(f"  Subtree contains {len(subtree_ids)} subjects")

    levels = _compute_levels(subtree_ids, broader, root_set)
    paths = _build_paths(subtree_ids, broader, labels, root_set)

    # Filter by max_depth
    filtered_ids = {sid for sid in subtree_ids if levels.get(sid, 0) <= max_depth}
    print(f"  After depth filter (max={max_depth}): {len(filtered_ids)} subjects")

    records = []
    for sid in sorted(filtered_ids):
        label = labels.get(sid, sid)
        level = levels.get(sid, 0)
        parent_ids = [p for p in broader.get(sid, []) if p in filtered_ids]

        records.append({
            "id": sid,
            "label": label,
            "definition": definitions.get(sid) or notes.get(sid),
            "parent_id": parent_ids[0] if parent_ids else None,
            "type": "subject_heading",
            "uri": f"https://id.loc.gov/authorities/subjects/{sid}",
            "full_path": paths.get(sid, label),
            "level": level,
            "aliases": alt_labels.get(sid, []),
            "cross_refs": [r for r in related.get(sid, []) if r in filtered_ids],
            "version": None,
        })

    depth_dist = defaultdict(int)
    for r in records:
        depth_dist[r["level"]] += 1

    print(f"\n  LoC total: {len(records)} subjects from {len(ROOT_IDS)} roots")
    print(f"  Depth distribution:")
    for d in sorted(depth_dist):
        print(f"    Level {d}: {depth_dist[d]}")

    return records
