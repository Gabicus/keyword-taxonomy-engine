#!/usr/bin/env python3
"""Look up WoS DOIs in OpenAlex to get full citation data.

For each DOI in raw_wos_natlab_expanded (and raw_wos_publications),
fetches the OpenAlex work record including referenced_works (backward citations)
and stores citation edges in openalex_citations.

Also creates openalex_wos_bridge table linking WoS accession numbers to OpenAlex IDs.

Batched API calls (50 DOIs per request) to stay within rate limits.
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


def fetch_works_by_doi(dois: list[str]) -> list[dict]:
    """Fetch works from OpenAlex by DOI, batched via pipe-separated filter."""
    clean_dois = []
    for d in dois:
        d = d.strip()
        if d.startswith("http"):
            d = d.split("doi.org/")[-1] if "doi.org/" in d else d
        clean_dois.append(d)

    filter_str = "|".join(f"https://doi.org/{d}" for d in clean_dois)

    params = urllib.parse.urlencode({
        "filter": f"doi:{filter_str}",
        "select": "id,doi,referenced_works,cited_by_count,title,publication_year",
        "per_page": BATCH_SIZE,
        "mailto": POLITE_EMAIL,
    })
    url = f"{OPENALEX_API}/works?{params}"

    req = urllib.request.Request(url, headers={
        "User-Agent": f"KeywordTaxonomyEngine/1.0 (mailto:{POLITE_EMAIL})"
    })

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return data.get("results", [])
    except Exception as e:
        print(f"  API error: {e}", flush=True)
        return []


def ensure_bridge_table(conn):
    """Create bridge table linking WoS accession numbers to OpenAlex IDs."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS openalex_wos_bridge (
            accession_number VARCHAR NOT NULL,
            openalex_id VARCHAR NOT NULL,
            doi VARCHAR,
            source_table VARCHAR,
            PRIMARY KEY (accession_number, openalex_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_bridge_oa ON openalex_wos_bridge(openalex_id)
    """)


def main():
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found", file=sys.stderr)
        sys.exit(1)

    conn = duckdb.connect(str(DB_PATH))
    ensure_bridge_table(conn)

    already_bridged = set()
    existing = conn.execute("SELECT DISTINCT doi FROM openalex_wos_bridge WHERE doi IS NOT NULL").fetchall()
    already_bridged = {r[0].lower() for r in existing if r[0]}

    tables_to_process = []

    try:
        count = conn.execute("""
            SELECT COUNT(*) FROM raw_wos_natlab_expanded WHERE doi IS NOT NULL AND doi != ''
        """).fetchone()[0]
        tables_to_process.append(("raw_wos_natlab_expanded", count))
    except Exception:
        pass

    try:
        count = conn.execute("""
            SELECT COUNT(*) FROM raw_wos_natlab_publications WHERE doi IS NOT NULL AND doi != ''
        """).fetchone()[0]
        tables_to_process.append(("raw_wos_natlab_publications", count))
    except Exception:
        pass

    try:
        count = conn.execute("""
            SELECT COUNT(*) FROM raw_wos_publications WHERE doi IS NOT NULL AND doi != ''
        """).fetchone()[0]
        tables_to_process.append(("raw_wos_publications", count))
    except Exception:
        pass

    print(f"Tables with DOIs: {[(t, c) for t, c in tables_to_process]}", flush=True)
    print(f"Already bridged: {len(already_bridged)} DOIs", flush=True)

    total_new_bridges = 0
    total_new_citations = 0
    corpus_ids = set()

    existing_corpus = conn.execute(
        "SELECT openalex_id FROM raw_openalex_publications"
    ).fetchall()
    corpus_ids = {r[0] for r in existing_corpus}

    for table_name, doi_count in tables_to_process:
        print(f"\n{'=' * 60}", flush=True)
        print(f"Processing {table_name} ({doi_count:,} DOIs)...", flush=True)

        rows = conn.execute(f"""
            SELECT accession_number, doi FROM {table_name}
            WHERE doi IS NOT NULL AND doi != ''
        """).fetchall()

        to_lookup = [(acc, doi) for acc, doi in rows if doi.lower() not in already_bridged]
        print(f"  New DOIs to look up: {len(to_lookup):,} (skipping {len(rows) - len(to_lookup):,} already bridged)", flush=True)

        if not to_lookup:
            continue

        total_batches = (len(to_lookup) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_start in range(0, len(to_lookup), BATCH_SIZE):
            batch = to_lookup[batch_start:batch_start + BATCH_SIZE]
            batch_num = batch_start // BATCH_SIZE + 1

            doi_map = {doi.lower(): acc for acc, doi in batch}
            dois = [doi for _, doi in batch]

            works = fetch_works_by_doi(dois)

            bridges = []
            citations = []

            for work in works:
                oa_id = work.get("id", "")
                work_doi = (work.get("doi") or "").replace("https://doi.org/", "")

                acc = doi_map.get(work_doi.lower())
                if not acc:
                    for d, a in doi_map.items():
                        if d in work_doi.lower() or work_doi.lower() in d:
                            acc = a
                            break

                if acc and oa_id:
                    bridges.append((acc, oa_id, work_doi, table_name))
                    already_bridged.add(work_doi.lower())
                    corpus_ids.add(oa_id)

                refs = work.get("referenced_works") or []
                for ref_id in refs:
                    in_corp = ref_id in corpus_ids
                    citations.append((oa_id, ref_id, in_corp))

            if bridges:
                conn.executemany(
                    "INSERT INTO openalex_wos_bridge VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    bridges
                )
                total_new_bridges += len(bridges)

            if citations:
                conn.executemany(
                    "INSERT INTO openalex_citations (citing_id, cited_id, in_corpus) "
                    "VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                    citations
                )
                total_new_citations += len(citations)

            if batch_num % 10 == 0 or batch_num == total_batches:
                print(f"  Batch {batch_num}/{total_batches}: "
                      f"+{len(bridges)} bridges, +{len(citations)} citations "
                      f"(total: {total_new_bridges:,} bridges, {total_new_citations:,} citations)",
                      flush=True)

            time.sleep(0.1)

    bridge_total = conn.execute("SELECT COUNT(*) FROM openalex_wos_bridge").fetchone()[0]
    cite_total = conn.execute("SELECT COUNT(*) FROM openalex_citations").fetchone()[0]
    in_corpus_total = conn.execute(
        "SELECT COUNT(*) FROM openalex_citations WHERE in_corpus = true"
    ).fetchone()[0]

    print(f"\n{'=' * 60}", flush=True)
    print(f"Done.", flush=True)
    print(f"  WoS↔OpenAlex bridges: {bridge_total:,}", flush=True)
    print(f"  Total citation edges:  {cite_total:,}", flush=True)
    print(f"  In-corpus edges:       {in_corpus_total:,}", flush=True)
    print(f"  New bridges this run:  {total_new_bridges:,}", flush=True)
    print(f"  New citations this run: {total_new_citations:,}", flush=True)

    conn.close()


if __name__ == "__main__":
    main()
