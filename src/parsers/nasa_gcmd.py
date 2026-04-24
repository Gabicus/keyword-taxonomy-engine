"""NASA GCMD keyword parser.

Fetches all keyword types from GCMD KMS, handles variable CSV headers,
and normalizes into unified schema records.
"""

import csv
import io
import uuid
from typing import Iterator

from ..config import SOURCES
from ..http_client import get_session

BASE_URL = "https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme"
KEYWORD_TYPES = SOURCES["nasa_gcmd"]["keyword_types"]

HIERARCHY_STOP = {"", "NOT APPLICABLE", "NOT PROVIDED"}


def _fetch_csv(keyword_type: str, session=None) -> str:
    session = session or get_session()
    url = f"{BASE_URL}/{keyword_type}?format=csv"
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text


def _detect_columns(headers: list[str]) -> tuple[list[str], str | None, str | None, str | None]:
    """Separate hierarchy columns from metadata columns.

    Returns (hierarchy_cols, uuid_col, short_name_col, long_name_col).
    UUID is always last if present. Short_Name/Long_Name are metadata, not hierarchy.
    """
    normalized = [h.strip().strip('"') for h in headers]

    uuid_col = None
    short_name_col = None
    long_name_col = None

    if "UUID" in normalized:
        uuid_col = "UUID"
    if "Short_Name" in normalized:
        short_name_col = "Short_Name"
    if "Long_Name" in normalized:
        long_name_col = "Long_Name"

    skip = {uuid_col, short_name_col, long_name_col} - {None}
    hierarchy_cols = [c for c in normalized if c not in skip]

    return hierarchy_cols, uuid_col, short_name_col, long_name_col


def _build_full_path(row: dict, hierarchy_cols: list[str]) -> tuple[str, int]:
    """Build hierarchical path from row values. Returns (path, level)."""
    parts = []
    for col in hierarchy_cols:
        val = row.get(col, "").strip()
        if val.upper() in HIERARCHY_STOP:
            break
        parts.append(val)
    path = " > ".join(parts) if parts else ""
    level = len(parts) - 1  # 0-indexed depth
    return path, max(level, 0)


def _parse_csv_text(csv_text: str, keyword_type: str) -> Iterator[dict]:
    """Parse a single GCMD CSV into normalized records.

    Two-pass approach: first pass collects all records and builds a path→id map,
    second pass resolves parent_ids using actual IDs (not deterministic UUIDs).
    """
    lines = csv_text.strip().splitlines()
    if len(lines) < 3:
        return

    reader = csv.DictReader(lines[1:], quoting=csv.QUOTE_ALL)
    if reader.fieldnames is None:
        return

    hierarchy_cols, uuid_col, short_name_col, long_name_col = _detect_columns(reader.fieldnames)

    # First pass: collect all records, build path→id map
    raw_records = []
    seen_paths = set()
    path_to_id = {}

    for row in reader:
        row = {
            (k.strip().strip('"') if k else ""): (v.strip() if v else "")
            for k, v in row.items() if k is not None
        }
        full_path, level = _build_full_path(row, hierarchy_cols)
        if not full_path or full_path in seen_paths:
            continue
        seen_paths.add(full_path)

        record_id = row.get(uuid_col, "") if uuid_col else ""
        if not record_id:
            record_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"gcmd:{keyword_type}:{full_path}"))

        path_to_id[full_path] = record_id

        label_parts = full_path.split(" > ")
        label = label_parts[-1] if label_parts else ""

        aliases = []
        if short_name_col and row.get(short_name_col):
            sn = row[short_name_col].strip()
            if sn.upper() not in HIERARCHY_STOP and sn != label:
                aliases.append(sn)
        if long_name_col and row.get(long_name_col):
            ln = row[long_name_col].strip()
            if ln.upper() not in HIERARCHY_STOP and ln != label and ln not in aliases:
                aliases.append(ln)

        raw_records.append({
            "id": record_id,
            "label": label,
            "full_path": full_path,
            "level": level,
            "aliases": aliases,
            "type": keyword_type,
            "uri": f"https://gcmd.earthdata.nasa.gov/kms/concept/{record_id}" if record_id else None,
        })

    # Generate synthetic roots for any ancestor paths not in the CSV
    synthetic = []
    for path in list(path_to_id.keys()):
        parts = path.split(" > ")
        for depth in range(len(parts) - 1):
            ancestor_path = " > ".join(parts[: depth + 1])
            if ancestor_path not in path_to_id:
                ancestor_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"gcmd:{keyword_type}:{ancestor_path}"))
                path_to_id[ancestor_path] = ancestor_id
                synthetic.append({
                    "id": ancestor_id,
                    "label": parts[depth],
                    "full_path": ancestor_path,
                    "level": depth,
                    "aliases": [],
                    "type": keyword_type,
                    "uri": None,
                })

    # Second pass: resolve parent_ids using the path→id map
    for record in raw_records + synthetic:
        parent_path = " > ".join(record["full_path"].split(" > ")[:-1])
        parent_id = path_to_id.get(parent_path) if parent_path else None

        yield {
            "id": record["id"],
            "label": record["label"],
            "definition": None,
            "parent_id": parent_id,
            "type": record["type"],
            "uri": record.get("uri"),
            "full_path": record["full_path"],
            "aliases": record["aliases"],
            "level": record["level"],
            "cross_refs": [],
            "version": None,
        }


def parse_nasa_gcmd(keyword_types: list[str] = None, session=None) -> list[dict]:
    """Fetch and parse all NASA GCMD keyword types.

    Returns list of unified schema records.
    """
    keyword_types = keyword_types or KEYWORD_TYPES
    session = session or get_session()
    all_records = []

    for ktype in keyword_types:
        try:
            csv_text = _fetch_csv(ktype, session)
            records = list(_parse_csv_text(csv_text, ktype))
            all_records.extend(records)
            print(f"  GCMD {ktype}: {len(records)} keywords parsed")
        except Exception as e:
            print(f"  GCMD {ktype}: FAILED - {e}")

    # Extract version from first CSV metadata line if available
    if all_records:
        try:
            csv_text = _fetch_csv(keyword_types[0], session)
            first_line = csv_text.strip().splitlines()[0]
            if "Version" in first_line:
                version = first_line.split(",")[0].strip().strip('"')
                for r in all_records:
                    r["version"] = version
        except Exception:
            pass

    print(f"  GCMD total: {len(all_records)} keywords across {len(keyword_types)} types")
    return all_records
