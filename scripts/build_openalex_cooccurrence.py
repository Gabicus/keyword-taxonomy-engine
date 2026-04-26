#!/usr/bin/env python3
"""Build co-occurrence edges from OpenAlex publication keyword pairs.

SQL-native approach: no Python dicts or fetchall loops.
Steps:
  1. Create new senses for OpenAlex keywords not yet in keyword_senses
  2. Build co-occurrence pairs via self-join on openalex_pub_keywords
  3. Map keyword labels → best sense_id (highest confidence)
  4. Insert into sense_relationships as 'co_occurrence' type
"""
import duckdb
import sys
from pathlib import Path

DB_PATH = Path("data/lake/keywords.duckdb")


def create_new_senses(conn) -> int:
    """Create keyword_senses entries for OpenAlex keywords not yet in the ontology."""
    conn.execute("""
        CREATE TEMP TABLE IF NOT EXISTS new_openalex_labels AS
        SELECT
            pk.keyword_label,
            pk.keyword_openalex_id,
            COUNT(DISTINCT pk.openalex_id) as pub_count,
            AVG(pk.relevance_score) as avg_relevance
        FROM openalex_pub_keywords pk
        LEFT JOIN keyword_senses ks ON LOWER(pk.keyword_label) = LOWER(ks.keyword_label)
        WHERE ks.sense_id IS NULL
          AND pk.relevance_score >= 0.3
        GROUP BY pk.keyword_label, pk.keyword_openalex_id
    """)

    count = conn.execute("SELECT COUNT(*) FROM new_openalex_labels").fetchone()[0]
    if count == 0:
        print("No new keywords to create senses for.")
        return 0

    print(f"Creating {count} new senses for OpenAlex keywords...", flush=True)

    conn.execute("""
        INSERT INTO keyword_senses (
            sense_id, keyword_id, keyword_source, keyword_label,
            origin_source, origin_path, origin_level,
            discipline_primary, resolution_tier,
            relevance_tags, confidence, provenance
        )
        SELECT
            'openalex_pub:' || REPLACE(LOWER(keyword_label), ' ', '_') || '#0' as sense_id,
            COALESCE(keyword_openalex_id, 'openalex_pub:' || LOWER(keyword_label)) as keyword_id,
            'openalex' as keyword_source,
            keyword_label,
            'openalex_publications' as origin_source,
            NULL as origin_path,
            NULL as origin_level,
            'general_science' as discipline_primary,
            4 as resolution_tier,
            ARRAY['pub_freq:' || pub_count::VARCHAR] as relevance_tags,
            LEAST(avg_relevance * 0.8, 0.9) as confidence,
            'openalex_pub_extraction' as provenance
        FROM new_openalex_labels
        ON CONFLICT (sense_id) DO NOTHING
    """)

    created = conn.execute("""
        SELECT COUNT(*) FROM keyword_senses WHERE provenance = 'openalex_pub_extraction'
    """).fetchone()[0]
    print(f"  Created {created} new senses.", flush=True)
    return created


def build_cooccurrence_edges(conn) -> int:
    """Build co-occurrence relationships from OpenAlex keyword pairs."""

    print("Building co-occurrence pairs (SQL self-join)...", flush=True)
    conn.execute("""
        CREATE TEMP TABLE IF NOT EXISTS cooccur_pairs AS
        SELECT
            a.keyword_label as label_a,
            b.keyword_label as label_b,
            COUNT(*) as cooccur_count
        FROM openalex_pub_keywords a
        JOIN openalex_pub_keywords b
            ON a.openalex_id = b.openalex_id
            AND a.keyword_label < b.keyword_label
        WHERE a.relevance_score >= 0.3
          AND b.relevance_score >= 0.3
        GROUP BY 1, 2
        HAVING COUNT(*) >= 2
    """)

    pair_count = conn.execute("SELECT COUNT(*) FROM cooccur_pairs").fetchone()[0]
    print(f"  {pair_count:,} co-occurrence pairs (cooccur >= 2).", flush=True)

    print("Mapping keyword labels to best sense_ids...", flush=True)
    conn.execute("""
        CREATE TEMP TABLE IF NOT EXISTS label_to_sense AS
        SELECT keyword_label, sense_id, discipline_primary, confidence
        FROM (
            SELECT keyword_label, sense_id, discipline_primary, confidence,
                   ROW_NUMBER() OVER (
                       PARTITION BY LOWER(keyword_label)
                       ORDER BY confidence DESC NULLS LAST, sense_id
                   ) as rn
            FROM keyword_senses
        ) ranked
        WHERE rn = 1
    """)

    mapped = conn.execute("SELECT COUNT(*) FROM label_to_sense").fetchone()[0]
    print(f"  {mapped:,} unique labels mapped to senses.", flush=True)

    print("Joining pairs with senses and inserting edges...", flush=True)
    conn.execute("""
        INSERT INTO sense_relationships (
            source_sense_id, target_sense_id, relationship_type,
            direction, confidence, provenance
        )
        SELECT
            sa.sense_id as source_sense_id,
            sb.sense_id as target_sense_id,
            'co_occurrence' as relationship_type,
            CASE
                WHEN sa.discipline_primary = sb.discipline_primary THEN 'toward'
                ELSE 'across'
            END as direction,
            LEAST(cp.cooccur_count::FLOAT / 20.0, 1.0) as confidence,
            'openalex_pub_cooccurrence' as provenance
        FROM cooccur_pairs cp
        JOIN label_to_sense sa ON LOWER(cp.label_a) = LOWER(sa.keyword_label)
        JOIN label_to_sense sb ON LOWER(cp.label_b) = LOWER(sb.keyword_label)
        WHERE sa.sense_id != sb.sense_id
        ON CONFLICT (source_sense_id, target_sense_id, relationship_type) DO NOTHING
    """)

    inserted = conn.execute("""
        SELECT COUNT(*) FROM sense_relationships
        WHERE provenance = 'openalex_pub_cooccurrence'
    """).fetchone()[0]
    print(f"  {inserted:,} co-occurrence edges inserted.", flush=True)

    cross = conn.execute("""
        SELECT COUNT(*) FROM sense_relationships
        WHERE provenance = 'openalex_pub_cooccurrence' AND direction = 'across'
    """).fetchone()[0]
    print(f"  {cross:,} cross-discipline edges.", flush=True)

    return inserted


def print_stats(conn):
    """Print updated relationship stats."""
    rows = conn.execute("""
        SELECT relationship_type, COUNT(*) as cnt
        FROM sense_relationships
        GROUP BY 1
        ORDER BY 2 DESC
    """).fetchall()
    print("\nRelationship type counts:", flush=True)
    total = 0
    for rtype, cnt in rows:
        print(f"  {rtype:25s} {cnt:>10,}", flush=True)
        total += cnt
    print(f"  {'TOTAL':25s} {total:>10,}", flush=True)

    sense_count = conn.execute("SELECT COUNT(*) FROM keyword_senses").fetchone()[0]
    print(f"\nSenses: {sense_count:,}  Rels: {total:,}  Ratio: {total/sense_count:.2f} rels/sense", flush=True)


def main():
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found", file=sys.stderr)
        sys.exit(1)

    conn = duckdb.connect(str(DB_PATH))
    try:
        new_senses = create_new_senses(conn)
        new_edges = build_cooccurrence_edges(conn)
        print_stats(conn)
        print(f"\nDone. +{new_senses} senses, +{new_edges} co-occurrence edges.", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
