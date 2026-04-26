#!/usr/bin/env python3
"""Fetch citation networks from OpenAlex for publications in our corpus.

For each publication in raw_openalex_publications, fetches referenced_works
(papers it cites) and stores edges in openalex_citations table.

Uses batched API calls via OpenAlex works filter to minimize requests.
"""
import duckdb
import json
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

DB_PATH = Path("data/lake/keywords.duckdb")
OPENALEX_API = "https://api.openalex.org"
BATCH_SIZE = 50
POLITE_EMAIL = "gabe.dewitt@gmail.com"


def fetch_works_batch(openalex_ids: list[str]) -> list[dict]:
    """Fetch multiple works in one API call using pipe-separated filter."""
    short_ids = [oid.replace("https://openalex.org/", "") for oid in openalex_ids]
    filter_str = "|".join(short_ids)

    params = urllib.parse.urlencode({
        "filter": f"openalex_id:{filter_str}",
        "select": "id,referenced_works",
        "per_page": len(short_ids),
        "mailto": POLITE_EMAIL,
    })
    url = f"{OPENALEX_API}/works?{params}"

    req = urllib.request.Request(url, headers={"User-Agent": f"KeywordTaxonomyEngine/1.0 (mailto:{POLITE_EMAIL})"})

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return data.get("results", [])
    except Exception as e:
        print(f"  API error: {e}", flush=True)
        return []


def main():
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found", file=sys.stderr)
        sys.exit(1)

    conn = duckdb.connect(str(DB_PATH))

    existing = conn.execute("SELECT COUNT(*) FROM openalex_citations").fetchone()[0]
    if existing > 0:
        print(f"Already have {existing:,} citation edges. Fetching remaining...", flush=True)

    already_fetched = set()
    if existing > 0:
        rows = conn.execute("SELECT DISTINCT citing_id FROM openalex_citations").fetchall()
        already_fetched = {r[0] for r in rows}

    all_ids = conn.execute(
        "SELECT openalex_id FROM raw_openalex_publications ORDER BY openalex_id"
    ).fetchall()
    all_ids = [r[0] for r in all_ids]

    to_fetch = [oid for oid in all_ids if oid not in already_fetched]
    print(f"Total pubs: {len(all_ids):,}  Already fetched: {len(already_fetched):,}  "
          f"Remaining: {len(to_fetch):,}", flush=True)

    corpus_set = set(all_ids)
    total_edges = existing
    total_in_corpus = 0

    for batch_start in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE

        works = fetch_works_batch(batch)

        edges = []
        for work in works:
            citing_id = work.get("id", "")
            refs = work.get("referenced_works", []) or []
            for ref_id in refs:
                in_corp = ref_id in corpus_set
                edges.append((citing_id, ref_id, in_corp))
                if in_corp:
                    total_in_corpus += 1

        if edges:
            conn.executemany(
                "INSERT INTO openalex_citations (citing_id, cited_id, in_corpus) "
                "VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                edges
            )

        total_edges += len(edges)
        print(f"  Batch {batch_num}/{total_batches}: {len(works)} works, "
              f"+{len(edges)} edges (total: {total_edges:,})", flush=True)

        if batch_start + BATCH_SIZE < len(to_fetch):
            time.sleep(0.1)

    final_count = conn.execute("SELECT COUNT(*) FROM openalex_citations").fetchone()[0]
    in_corpus_count = conn.execute(
        "SELECT COUNT(*) FROM openalex_citations WHERE in_corpus = true"
    ).fetchone()[0]
    citing_unique = conn.execute(
        "SELECT COUNT(DISTINCT citing_id) FROM openalex_citations"
    ).fetchone()[0]

    print(f"\nDone.", flush=True)
    print(f"  Total citation edges: {final_count:,}", flush=True)
    print(f"  In-corpus edges:      {in_corpus_count:,} (both papers in our DB)", flush=True)
    print(f"  Pubs with references:  {citing_unique:,} / {len(all_ids):,}", flush=True)
    print(f"  Avg refs per paper:    {final_count / max(citing_unique, 1):.1f}", flush=True)

    conn.close()


if __name__ == "__main__":
    main()
