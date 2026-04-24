"""Enrichment module for NASA GCMD keywords.

Fetches per-concept JSON from the GCMD KMS API to backfill:
- definitions (with citation references)
- altLabels (aliases)
- related concepts
- lastModifiedDate
- isLeaf flag
"""

import time
import json
from pathlib import Path

from ..config import RAW_DIR
from ..http_client import get_session
from ..storage import KeywordStore

CONCEPT_URL = "https://gcmd.earthdata.nasa.gov/kms/concept"
BATCH_SIZE = 50
RATE_LIMIT_DELAY = 0.05  # 50ms between requests


def _fetch_concept(uuid: str, session) -> dict | None:
    try:
        resp = session.get(f"{CONCEPT_URL}/{uuid}?format=json", timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _extract_enrichment(data: dict) -> dict:
    """Extract enrichment fields from concept JSON."""
    result = {}

    definitions = data.get("definitions", [])
    if definitions:
        texts = []
        for d in definitions:
            text = d.get("text", "").strip()
            ref = d.get("reference", "").strip()
            if text:
                if ref:
                    texts.append(f"{text} [Ref: {ref}]")
                else:
                    texts.append(text)
        if texts:
            result["definition"] = " | ".join(texts)

    alt_labels = data.get("altLabels", [])
    if alt_labels:
        aliases = []
        for al in alt_labels:
            if isinstance(al, dict):
                text = al.get("text", "").strip()
            else:
                text = str(al).strip()
            if text:
                aliases.append(text)
        if aliases:
            result["aliases"] = aliases

    related = data.get("related", [])
    if related:
        related_ids = [r["uuid"] for r in related if "uuid" in r]
        if related_ids:
            result["cross_refs"] = related_ids

    last_mod = data.get("lastModifiedDate", "")
    if last_mod:
        result["last_modified"] = last_mod

    return result


def enrich_gcmd(store: KeywordStore = None, max_concepts: int = None, quiet: bool = False):
    """Enrich all NASA GCMD keywords with per-concept API data.

    Fetches JSON for each concept, extracts definitions/aliases/related,
    and updates the DuckDB store in batches.
    """
    own_store = store is None
    if own_store:
        store = KeywordStore()

    session = get_session(cache_name="gcmd_concepts")

    # Get all non-synthetic UUIDs (synthetic ones won't have API entries)
    concepts = store.conn.execute("""
        SELECT id, label, type FROM keywords
        WHERE source = 'NASA GCMD'
        AND uri IS NOT NULL
        ORDER BY type, label
    """).fetchall()

    if max_concepts:
        concepts = concepts[:max_concepts]

    total = len(concepts)
    enriched = 0
    with_def = 0
    with_aliases = 0
    with_related = 0
    failed = 0
    skipped = 0

    if not quiet:
        print(f"Enriching {total} GCMD concepts...")

    updates = []

    for i, (uuid, label, ktype) in enumerate(concepts):
        if i > 0 and i % BATCH_SIZE == 0:
            _flush_updates(store, updates)
            updates = []
            if not quiet:
                print(f"  {i}/{total} processed ({with_def} defs, {with_aliases} aliases, {failed} failed)")

        data = _fetch_concept(uuid, session)
        if data is None:
            failed += 1
            continue

        enrichment = _extract_enrichment(data)
        if not enrichment:
            skipped += 1
            continue

        enrichment["uuid"] = uuid
        updates.append(enrichment)
        enriched += 1

        if "definition" in enrichment:
            with_def += 1
        if "aliases" in enrichment:
            with_aliases += 1
        if "cross_refs" in enrichment:
            with_related += 1

        time.sleep(RATE_LIMIT_DELAY)

    # Flush remaining
    if updates:
        _flush_updates(store, updates)

    if own_store:
        store.close()

    stats = {
        "total": total,
        "enriched": enriched,
        "with_definitions": with_def,
        "with_aliases": with_aliases,
        "with_related": with_related,
        "failed": failed,
        "skipped": skipped,
    }

    if not quiet:
        print(f"\nEnrichment complete:")
        for k, v in stats.items():
            print(f"  {k}: {v}")

    return stats


def _flush_updates(store: KeywordStore, updates: list[dict]):
    """Batch-update enrichment fields in DuckDB."""
    for u in updates:
        uuid = u["uuid"]
        sets = []
        params = []

        if "definition" in u:
            sets.append("definition = ?")
            params.append(u["definition"])

        if "aliases" in u:
            # Merge with existing aliases
            existing = store.conn.execute(
                "SELECT aliases FROM keywords WHERE id = ? AND source = 'NASA GCMD'",
                [uuid],
            ).fetchone()
            existing_aliases = existing[0] if existing and existing[0] else []
            merged = list(dict.fromkeys(existing_aliases + u["aliases"]))
            sets.append("aliases = ?")
            params.append(merged)

        if "cross_refs" in u:
            existing = store.conn.execute(
                "SELECT cross_refs FROM keywords WHERE id = ? AND source = 'NASA GCMD'",
                [uuid],
            ).fetchone()
            existing_refs = existing[0] if existing and existing[0] else []
            merged = list(dict.fromkeys(existing_refs + u["cross_refs"]))
            sets.append("cross_refs = ?")
            params.append(merged)

        if sets:
            params.append(uuid)
            store.conn.execute(
                f"UPDATE keywords SET {', '.join(sets)} WHERE id = ? AND source = 'NASA GCMD'",
                params,
            )
