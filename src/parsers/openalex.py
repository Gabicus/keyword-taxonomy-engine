"""OpenAlex topic/concept taxonomy parser.

Fetches all 4 hierarchy levels (domains, fields, subfields, topics)
plus keywords from the OpenAlex API and normalizes into unified schema records.

API docs: https://docs.openalex.org/api-entities/topics
"""

import time
from typing import Iterator

from ..http_client import get_session

BASE_URL = "https://api.openalex.org"
MAILTO = "gabe.dewitt@gmail.com"
PAGE_SIZE = 200
PAGE_DELAY = 0.1  # 100ms polite delay between pages

# Hierarchy levels and their API endpoints
HIERARCHY_LEVELS = [
    {"type": "domain", "endpoint": "domains", "level": 0},
    {"type": "field", "endpoint": "fields", "level": 1},
    {"type": "subfield", "endpoint": "subfields", "level": 2},
    {"type": "topic", "endpoint": "topics", "level": 3},
]


def _build_params(**extra) -> dict:
    """Build query params with mailto for polite pool."""
    params = {"mailto": MAILTO, "per_page": PAGE_SIZE}
    params.update(extra)
    return params


def _fetch_all_pages(endpoint: str, session, entity_type: str) -> list[dict]:
    """Fetch all pages from a cursor-paginated OpenAlex endpoint."""
    url = f"{BASE_URL}/{endpoint}"
    results = []
    cursor = "*"
    page = 0

    while cursor:
        params = _build_params(cursor=cursor)
        resp = session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("results", [])
        results.extend(batch)
        page += 1

        meta = data.get("meta", {})
        next_cursor = meta.get("next_cursor")

        if next_cursor and len(batch) == PAGE_SIZE:
            cursor = next_cursor
            print(f"    {entity_type}: page {page}, {len(results)} fetched so far...")
            time.sleep(PAGE_DELAY)
        else:
            cursor = None

    return results


def _extract_openalex_id(entity: dict) -> str:
    """Extract short ID from OpenAlex entity (e.g. 'https://openalex.org/T12345' -> 'T12345')."""
    full_id = entity.get("id", "")
    if "/" in full_id:
        return full_id.rsplit("/", 1)[-1]
    return full_id


def _get_parent_id(entity: dict, entity_type: str) -> str | None:
    """Extract parent ID based on entity type."""
    if entity_type == "domain":
        return None
    elif entity_type == "field":
        domain = entity.get("domain", {})
        if domain and domain.get("id"):
            return _extract_openalex_id(domain)
    elif entity_type == "subfield":
        field = entity.get("field", {})
        if field and field.get("id"):
            return _extract_openalex_id(field)
    elif entity_type == "topic":
        subfield = entity.get("subfield", {})
        if subfield and subfield.get("id"):
            return _extract_openalex_id(subfield)
    return None


def _build_full_path(entity: dict, entity_type: str) -> str:
    """Build full hierarchical path for an entity."""
    parts = []
    if entity_type in ("topic", "subfield", "field", "domain"):
        domain = entity.get("domain", {})
        if domain and domain.get("display_name"):
            parts.append(domain["display_name"])
    if entity_type in ("topic", "subfield", "field"):
        field = entity.get("field", {})
        if field and field.get("display_name"):
            parts.append(field["display_name"])
    if entity_type in ("topic", "subfield"):
        subfield = entity.get("subfield", {})
        if subfield and subfield.get("display_name"):
            parts.append(subfield["display_name"])
    if entity_type == "topic":
        parts.append(entity.get("display_name", ""))
    elif entity_type == "domain":
        parts = [entity.get("display_name", "")]
    elif entity_type == "field" and not parts:
        parts = [entity.get("display_name", "")]
    elif entity_type == "subfield" and not parts:
        parts = [entity.get("display_name", "")]

    # For non-topic types, the entity name might not be in parts yet
    name = entity.get("display_name", "")
    if name and (not parts or parts[-1] != name):
        parts.append(name)

    return " > ".join(parts)


def _normalize_entity(entity: dict, entity_type: str, level: int) -> dict:
    """Convert an OpenAlex entity to the unified schema."""
    oa_id = _extract_openalex_id(entity)
    label = entity.get("display_name", "")
    description = entity.get("description", None)

    # Some entities have keywords as a list of strings or objects
    aliases = []
    keywords = entity.get("keywords", [])
    if keywords:
        for kw in keywords:
            if isinstance(kw, str):
                if kw != label:
                    aliases.append(kw)
            elif isinstance(kw, dict):
                kw_name = kw.get("display_name", kw.get("keyword", ""))
                if kw_name and kw_name != label:
                    aliases.append(kw_name)

    # Also check for siblings/alternative names if available
    alt_names = entity.get("display_name_alternatives", [])
    for alt in alt_names:
        if alt and alt != label and alt not in aliases:
            aliases.append(alt)

    return {
        "id": oa_id,
        "label": label,
        "definition": description,
        "parent_id": _get_parent_id(entity, entity_type),
        "type": entity_type,
        "uri": entity.get("id", None),
        "full_path": _build_full_path(entity, entity_type),
        "aliases": aliases,
        "level": level,
        "cross_refs": [],
        "version": None,
    }


def _extract_keywords_from_topics(topic_records: list[dict], raw_topics: list[dict]) -> list[dict]:
    """Extract standalone keyword records from topic keyword lists.

    OpenAlex topics contain keyword lists. We extract unique keywords
    as separate records linked to their parent topic.
    """
    seen_keywords = {}  # keyword_text -> record
    keyword_records = []

    for raw_topic, record in zip(raw_topics, topic_records):
        keywords = raw_topic.get("keywords", [])
        topic_id = record["id"]
        topic_path = record["full_path"]

        for kw in keywords:
            if isinstance(kw, str):
                kw_text = kw
            elif isinstance(kw, dict):
                kw_text = kw.get("display_name", kw.get("keyword", ""))
            else:
                continue

            if not kw_text or kw_text == record["label"]:
                continue

            # Deduplicate keywords, keeping first occurrence
            kw_lower = kw_text.lower()
            if kw_lower in seen_keywords:
                continue

            kw_id = f"KW-{hash(kw_lower) & 0xFFFFFFFF:08x}"
            kw_record = {
                "id": kw_id,
                "label": kw_text,
                "definition": None,
                "parent_id": topic_id,
                "type": "keyword",
                "uri": None,
                "full_path": f"{topic_path} > {kw_text}",
                "aliases": [],
                "level": 4,
                "cross_refs": [],
                "version": None,
            }
            seen_keywords[kw_lower] = kw_record
            keyword_records.append(kw_record)

    return keyword_records


def parse_openalex(session=None) -> list[dict]:
    """Fetch and parse the full OpenAlex topic taxonomy.

    Returns list of unified schema records covering:
    - 4 domains (level 0)
    - 26 fields (level 1)
    - 254 subfields (level 2)
    - ~4,500 topics (level 3)
    - ~26,000 keywords extracted from topics (level 4)
    """
    session = session or get_session()
    all_records = []
    raw_topics = []

    for hier in HIERARCHY_LEVELS:
        entity_type = hier["type"]
        endpoint = hier["endpoint"]
        level = hier["level"]

        print(f"  OpenAlex: fetching {endpoint}...")
        try:
            raw_entities = _fetch_all_pages(endpoint, session, entity_type)
            records = [_normalize_entity(e, entity_type, level) for e in raw_entities]
            all_records.extend(records)
            print(f"  OpenAlex {entity_type}: {len(records)} records parsed")

            if entity_type == "topic":
                raw_topics = raw_entities

        except Exception as e:
            print(f"  OpenAlex {entity_type}: FAILED - {e}")

    # Extract keywords from topics
    if raw_topics:
        print("  OpenAlex: extracting keywords from topics...")
        topic_records = [r for r in all_records if r["type"] == "topic"]
        keyword_records = _extract_keywords_from_topics(topic_records, raw_topics)
        all_records.extend(keyword_records)
        print(f"  OpenAlex keywords: {len(keyword_records)} unique keywords extracted")

    print(f"  OpenAlex total: {len(all_records)} records")
    return all_records
