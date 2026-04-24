"""Raw table writers — populate source-specific raw tables from source data.

Each function re-reads source data and returns records matching the raw table schema.
These preserve full source fidelity that the unified `keywords` table drops.
"""

import csv
import io
import json
import gzip
import tarfile
import uuid
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, SKOS, DCTERMS

from .config import SOURCES, RAW_DIR, NCBI_RANK_HIERARCHY
from .parsers.nasa_gcmd import _fetch_csv, _detect_columns, _build_full_path, HIERARCHY_STOP, BASE_URL, KEYWORD_TYPES
from .parsers.ncbi import TARBALL_PATH, _extract_file, _parse_nodes_text, _parse_names_text, RANK_LEVEL, ALIAS_CLASSES
from .parsers.loc import BULK_PATH, _stream_parse, _bfs_subtree, _compute_levels, _build_paths, ROOT_IDS
from .parsers.doe_osti import FALLBACK_CATEGORIES, CATEGORY_GROUPS, _build_code_to_group
from .http_client import get_session


def build_raw_nasa_gcmd(session=None) -> list[dict]:
    """Build raw_nasa_gcmd records from GCMD CSV data."""
    session = session or get_session()
    records = []

    for ktype in KEYWORD_TYPES:
        csv_text = _fetch_csv(ktype, session)
        lines = csv_text.strip().splitlines()
        if len(lines) < 3:
            continue

        version = None
        first_line = lines[0]
        if "Version" in first_line:
            version = first_line.split(",")[0].strip().strip('"')

        reader = csv.DictReader(lines[1:], quoting=csv.QUOTE_ALL)
        if reader.fieldnames is None:
            continue

        hierarchy_cols, uuid_col, short_name_col, long_name_col = _detect_columns(reader.fieldnames)
        seen_paths = set()
        path_to_id = {}
        raw_batch = []

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
                record_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"gcmd:{ktype}:{full_path}"))
            path_to_id[full_path] = record_id

            parts = full_path.split(" > ")
            label = parts[-1]

            short_name = row.get(short_name_col, "").strip() if short_name_col else None
            long_name = row.get(long_name_col, "").strip() if long_name_col else None
            if short_name and short_name.upper() in HIERARCHY_STOP:
                short_name = None
            if long_name and long_name.upper() in HIERARCHY_STOP:
                long_name = None

            # Map hierarchy columns to raw table fields
            cat = parts[0] if len(parts) > 0 else None
            topic = parts[1] if len(parts) > 1 else None
            term = parts[2] if len(parts) > 2 else None
            v1 = parts[3] if len(parts) > 3 else None
            v2 = parts[4] if len(parts) > 4 else None
            v3 = parts[5] if len(parts) > 5 else None
            detailed = parts[6] if len(parts) > 6 else None

            alt_labels = []
            if short_name and short_name != label:
                alt_labels.append(short_name)
            if long_name and long_name != label and long_name not in alt_labels:
                alt_labels.append(long_name)

            raw_batch.append({
                "uuid": record_id,
                "pref_label": label,
                "keyword_type": ktype,
                "category": cat,
                "topic": topic,
                "term": term,
                "var_level_1": v1,
                "var_level_2": v2,
                "var_level_3": v3,
                "detailed_var": detailed,
                "short_name": short_name,
                "long_name": long_name,
                "full_path": full_path,
                "hierarchy_level": level,
                "alt_labels": alt_labels if alt_labels else None,
                "keyword_version": version,
            })

        # Resolve parent UUIDs
        for rec in raw_batch:
            parent_path = " > ".join(rec["full_path"].split(" > ")[:-1])
            rec["parent_uuid"] = path_to_id.get(parent_path)

        records.extend(raw_batch)
        print(f"  raw_nasa_gcmd {ktype}: {len(raw_batch)} records")

    print(f"  raw_nasa_gcmd total: {len(records)}")
    return records


def build_raw_unesco(session=None) -> list[dict]:
    """Build raw_unesco records from UNESCO RDF with all language labels."""
    rdf_path = RAW_DIR / "unesco-thesaurus.rdf"
    if not rdf_path.exists():
        print("  UNESCO RDF not cached. Run ingest first.")
        return []

    print("  Parsing UNESCO RDF for raw table...")
    g = Graph()
    g.parse(str(rdf_path), format="xml")

    concepts = set(g.subjects(RDF.type, SKOS.Concept))

    def _lang_label(concept, pred, lang):
        for obj in g.objects(concept, pred):
            if isinstance(obj, Literal) and obj.language == lang:
                return str(obj)
        return None

    def _all_lang_labels(concept, pred, lang):
        return [str(o) for o in g.objects(concept, pred)
                if isinstance(o, Literal) and o.language == lang]

    def _uri_list(concept, pred):
        return [str(o) for o in g.objects(concept, pred) if isinstance(o, URIRef)]

    broader_map = {}
    for s, _, o in g.triples((None, SKOS.broader, None)):
        broader_map.setdefault(str(s), []).append(str(o))

    label_map = {}
    for uri in concepts:
        lbl = _lang_label(uri, SKOS.prefLabel, "en")
        if lbl:
            label_map[str(uri)] = lbl

    version = None
    for s, p, o in g.triples((None, DCTERMS.modified, None)):
        version = str(o)
        break

    records = []
    for concept_uri in concepts:
        uri_str = str(concept_uri)
        en_label = _lang_label(concept_uri, SKOS.prefLabel, "en")
        if not en_label:
            continue

        broaders = broader_map.get(uri_str, [])

        # Walk path for hierarchy
        def _walk(u, seen=None):
            if seen is None:
                seen = set()
            if u in seen:
                return []
            seen.add(u)
            parents = broader_map.get(u, [])
            if not parents:
                return [label_map.get(u, u)]
            return _walk(parents[0], seen) + [label_map.get(u, u)]

        path_parts = _walk(uri_str)

        records.append({
            "concept_uri": uri_str,
            "pref_label_en": en_label,
            "pref_label_fr": _lang_label(concept_uri, SKOS.prefLabel, "fr"),
            "pref_label_es": _lang_label(concept_uri, SKOS.prefLabel, "es"),
            "pref_label_ru": _lang_label(concept_uri, SKOS.prefLabel, "ru"),
            "pref_label_ar": _lang_label(concept_uri, SKOS.prefLabel, "ar"),
            "pref_label_zh": _lang_label(concept_uri, SKOS.prefLabel, "zh"),
            "definition_en": _lang_label(concept_uri, SKOS.definition, "en"),
            "scope_note_en": _lang_label(concept_uri, SKOS.scopeNote, "en"),
            "notation": _lang_label(concept_uri, SKOS.notation, None),
            "alt_labels_en": _all_lang_labels(concept_uri, SKOS.altLabel, "en") or None,
            "broader_uris": broaders or None,
            "narrower_uris": _uri_list(concept_uri, SKOS.narrower) or None,
            "related_uris": _uri_list(concept_uri, SKOS.related) or None,
            "exact_match": _uri_list(concept_uri, SKOS.exactMatch) or None,
            "close_match": _uri_list(concept_uri, SKOS.closeMatch) or None,
            "broad_match": _uri_list(concept_uri, SKOS.broadMatch) or None,
            "narrow_match": _uri_list(concept_uri, SKOS.narrowMatch) or None,
            "in_scheme": _uri_list(concept_uri, SKOS.inScheme) or None,
            "modified": version,
            "full_path": " > ".join(path_parts) if path_parts else en_label,
            "hierarchy_level": max(len(path_parts) - 1, 0),
        })

    print(f"  raw_unesco total: {len(records)}")
    return records


def build_raw_openalex(session=None) -> list[dict]:
    """Build raw_openalex records from OpenAlex API data."""
    from .parsers.openalex import _fetch_all_pages, _extract_openalex_id, _get_parent_id, _build_full_path, HIERARCHY_LEVELS

    session = session or get_session()
    records = []
    all_raw = {}

    for hier in HIERARCHY_LEVELS:
        entity_type = hier["type"]
        endpoint = hier["endpoint"]
        level = hier["level"]

        print(f"  raw_openalex: fetching {endpoint}...")
        raw_entities = _fetch_all_pages(endpoint, session, entity_type)

        for entity in raw_entities:
            oa_id = _extract_openalex_id(entity)
            all_raw[oa_id] = entity

            domain = entity.get("domain", {})
            field = entity.get("field", {})
            subfield = entity.get("subfield", {})
            siblings = entity.get("siblings", [])
            keywords = entity.get("keywords", [])

            records.append({
                "openalex_id": oa_id,
                "display_name": entity.get("display_name", ""),
                "description": entity.get("description"),
                "entity_type": entity_type,
                "works_count": entity.get("works_count"),
                "cited_by_count": entity.get("cited_by_count"),
                "updated_date": entity.get("updated_date"),
                "parent_id": _get_parent_id(entity, entity_type),
                "parent_name": None,
                "domain_id": _extract_openalex_id(domain) if domain and domain.get("id") else None,
                "domain_name": domain.get("display_name") if domain else None,
                "field_id": _extract_openalex_id(field) if field and field.get("id") else None,
                "field_name": field.get("display_name") if field else None,
                "subfield_id": _extract_openalex_id(subfield) if subfield and subfield.get("id") else None,
                "subfield_name": subfield.get("display_name") if subfield else None,
                "siblings_json": json.dumps(siblings) if siblings else None,
                "keywords_json": json.dumps(keywords) if keywords else None,
                "full_path": _build_full_path(entity, entity_type),
                "hierarchy_level": level,
            })

        print(f"  raw_openalex {entity_type}: {len(raw_entities)} records")

    print(f"  raw_openalex total: {len(records)}")
    return records


def build_raw_ncbi(session=None) -> list[dict]:
    """Build raw_ncbi records from taxdump data."""
    if not TARBALL_PATH.exists():
        print("  NCBI taxdump not cached. Run ingest first.")
        return []

    max_rank = SOURCES["ncbi"].get("max_rank", "order")
    max_level = RANK_LEVEL.get(max_rank, len(NCBI_RANK_HIERARCHY) - 1)
    allowed = {r for r in NCBI_RANK_HIERARCHY if RANK_LEVEL[r] <= max_level}

    print("  Parsing NCBI taxdump for raw table...")
    nodes_text = _extract_file(TARBALL_PATH, "nodes.dmp")
    nodes = _parse_nodes_text(nodes_text)

    names_text = _extract_file(TARBALL_PATH, "names.dmp")
    scientific_names, aliases_map = _parse_names_text(names_text)

    # Also collect common names and other name types
    common_names = defaultdict(list)
    other_names = defaultdict(list)
    for line in names_text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t|\t")
        if len(parts) < 4:
            continue
        tax_id = parts[0].strip().rstrip("|").strip()
        name_txt = parts[1].strip().rstrip("|").strip()
        name_class = parts[3].strip().rstrip("\t|").strip()
        if name_class == "genbank common name" or name_class == "common name":
            common_names[tax_id].append(name_txt)
        elif name_class not in ("scientific name", "synonym", "genbank common name", "common name"):
            other_names[tax_id].append(name_txt)

    filtered_ids = {tid for tid, (pid, rank) in nodes.items() if rank in allowed}

    def _find_filtered_parent(tax_id):
        current = tax_id
        visited = set()
        while current in nodes:
            if current in visited:
                return None
            visited.add(current)
            parent_id, _ = nodes[current]
            if parent_id == current:
                return None
            if parent_id in filtered_ids:
                return int(parent_id)
            current = parent_id
        return None

    def _build_lineage(tax_id):
        parts = []
        current = tax_id
        visited = set()
        while current in nodes:
            if current in visited:
                break
            visited.add(current)
            name = scientific_names.get(current, current)
            parts.append(name)
            parent_id = nodes[current][0]
            if parent_id == current:
                break
            current = parent_id
        parts.reverse()
        return "; ".join(parts)

    def _build_path(tax_id):
        parts = []
        current = tax_id
        visited = set()
        while current in nodes:
            if current in visited:
                break
            visited.add(current)
            _, rank = nodes[current]
            if rank in allowed:
                parts.append(scientific_names.get(current, current))
            parent_id = nodes[current][0]
            if parent_id == current:
                break
            current = parent_id
        parts.reverse()
        return " > ".join(parts)

    records = []
    for tax_id in sorted(filtered_ids, key=int):
        parent_id_raw, rank = nodes[tax_id]
        division_id = None
        genetic_code = None
        mito_genetic_code = None

        # Extract extra fields from nodes.dmp (fields 4, 6, 8 in the pipe-delimited format)
        # These were parsed but not stored — we need to re-extract
        # For now, leave as None since we'd need to re-parse nodes.dmp with more fields

        records.append({
            "tax_id": int(tax_id),
            "scientific_name": scientific_names.get(tax_id, f"taxid:{tax_id}"),
            "rank": rank,
            "parent_tax_id": int(parent_id_raw) if parent_id_raw != tax_id else None,
            "filtered_parent_id": _find_filtered_parent(tax_id),
            "synonyms": aliases_map.get(tax_id) or None,
            "common_names": common_names.get(tax_id) or None,
            "other_names": other_names.get(tax_id) or None,
            "lineage": _build_lineage(tax_id),
            "full_path": _build_path(tax_id),
            "hierarchy_level": RANK_LEVEL.get(rank, 0),
        })

    print(f"  raw_ncbi total: {len(records)}")
    return records


def build_raw_loc() -> list[dict]:
    """Build raw_loc records from parsed bulk LCSH data."""
    if not BULK_PATH.exists():
        print("  LoC bulk file not cached. Run ingest first.")
        return []

    data = _stream_parse(BULK_PATH)
    labels = data["labels"]
    broader = data["broader"]
    narrower = data["narrower"]
    related = data["related"]
    alt_labels = data["alt_labels"]
    notes = data["notes"]
    definitions = data["definitions"]

    root_set = set(ROOT_IDS)
    subtree_ids = _bfs_subtree(ROOT_IDS, narrower, broader)
    levels = _compute_levels(subtree_ids, broader, root_set)
    paths = _build_paths(subtree_ids, broader, labels, root_set)

    # Apply same depth filter as unified parser
    filtered_ids = {sid for sid in subtree_ids if levels.get(sid, 0) <= 6}

    records = []
    for sid in sorted(filtered_ids):
        broader_ids = [b for b in broader.get(sid, []) if b in filtered_ids]
        narrower_ids = [n for n in narrower.get(sid, []) if n in filtered_ids]
        related_ids = [r for r in related.get(sid, []) if r in filtered_ids]

        records.append({
            "loc_id": sid,
            "auth_label": labels.get(sid, sid),
            "pref_label": labels.get(sid, sid),
            "scope_note": notes.get(sid),
            "definition": definitions.get(sid),
            "broader_ids": broader_ids or None,
            "narrower_ids": narrower_ids or None,
            "related_ids": related_ids or None,
            "variants": alt_labels.get(sid) or None,
            "full_path": paths.get(sid, labels.get(sid, sid)),
            "hierarchy_level": levels.get(sid, 0),
            "crawl_depth": levels.get(sid, 0),
        })

    print(f"  raw_loc total: {len(records)}")
    return records


def build_raw_doe_osti() -> list[dict]:
    """Build raw_doe_osti records from hardcoded category data."""
    code_to_group = _build_code_to_group()
    records = []

    # Group parents
    for group_id, group in CATEGORY_GROUPS.items():
        records.append({
            "category_code": f"GRP-{group_id}",
            "category_name": group["label"],
            "description": None,
            "group_code": group_id,
            "group_name": group["label"],
            "active": True,
            "full_path": group["label"],
            "hierarchy_level": 0,
        })

    for code, name, desc in FALLBACK_CATEGORIES:
        group_id = code_to_group.get(code)
        group = CATEGORY_GROUPS.get(group_id, {}) if group_id else {}
        group_label = group.get("label")

        records.append({
            "category_code": code,
            "category_name": name,
            "description": desc or None,
            "group_code": group_id,
            "group_name": group_label,
            "active": True,
            "full_path": f"{group_label} > {name}" if group_label else name,
            "hierarchy_level": 1 if group_id else 0,
        })

    print(f"  raw_doe_osti total: {len(records)}")
    return records
