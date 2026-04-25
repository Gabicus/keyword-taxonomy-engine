"""Enrich keyword senses with contextual signals from WoS publication fields.

Processes fields NOT yet extracted into senses:
  1. Abstracts (NLP keyword extraction → publication_frequency update)
  2. Titles (keyword extraction → frequency signals)
  3. Journal→discipline mapping (source_title as discipline context)
  4. Natlab pub co-occurrence (properly split keywords)
  5. Updated polysemy bridges (expanded label set)

Each step includes a spot-check audit before proceeding.
"""

import duckdb
import logging
import re
import sys
from pathlib import Path
from collections import Counter, defaultdict
from itertools import combinations

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def audit_field(conn, field_name: str, table: str, expected_min: int = 0):
    """Spot-check a field after processing."""
    total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    non_null = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {field_name} IS NOT NULL").fetchone()[0]
    logger.info(f"  AUDIT {table}.{field_name}: {non_null:,}/{total:,} non-null ({non_null/total*100:.0f}%)")
    if non_null < expected_min:
        logger.warning(f"  AUDIT FAIL: expected at least {expected_min}, got {non_null}")
        return False
    # Sample 3 values
    samples = conn.execute(f"""
        SELECT {field_name} FROM {table}
        WHERE {field_name} IS NOT NULL
        LIMIT 3
    """).fetchall()
    for s in samples:
        val = str(s[0])[:80]
        logger.info(f"    sample: {val}")
    return True


def step1_abstract_keyword_extraction(conn) -> dict:
    """Extract keywords from abstracts and update publication_frequency on matching senses.

    Does NOT create new senses — enriches existing ones with abstract co-occurrence.
    """
    logger.info("=== STEP 1: Abstract Keyword Extraction ===")

    # Build vocabulary index from existing senses
    vocab = {}
    for (label,) in conn.execute("SELECT DISTINCT LOWER(keyword_label) FROM keyword_senses WHERE LENGTH(keyword_label) > 2").fetchall():
        vocab[label] = True

    logger.info(f"Vocabulary size: {len(vocab):,}")

    # Process abstracts from BOTH WoS tables
    abstract_matches = Counter()

    for table in ["raw_wos_publications", "raw_wos_natlab_publications"]:
        abstracts = conn.execute(f"""
            SELECT abstract FROM {table}
            WHERE abstract IS NOT NULL AND LENGTH(abstract) > 50
        """).fetchall()
        logger.info(f"  {table}: {len(abstracts):,} abstracts")

        for (abstract,) in abstracts:
            abstract_lower = abstract.lower()
            # Check multi-word first (longer matches take priority)
            matched = set()
            for label in vocab:
                if ' ' in label and len(label) > 5 and label in abstract_lower:
                    matched.add(label)
                elif ' ' not in label and len(label) > 3:
                    # Single word: require word boundary
                    if re.search(r'\b' + re.escape(label) + r'\b', abstract_lower):
                        matched.add(label)

            for m in matched:
                abstract_matches[m] += 1

    logger.info(f"Keywords found in abstracts: {len(abstract_matches):,}")

    # Update publication_frequency for senses that have abstract mentions
    # Use top matches (appearing in 5+ abstracts) to avoid noise
    significant = {k: v for k, v in abstract_matches.items() if v >= 5}
    logger.info(f"Significant (≥5 abstracts): {len(significant):,}")

    # Spot-check top matches
    logger.info("  Top 15 abstract keyword matches:")
    for kw, count in sorted(significant.items(), key=lambda x: -x[1])[:15]:
        logger.info(f"    {kw}: {count:,} abstracts")

    # Create abstract_frequency tag on matching senses
    updated = 0
    for kw, count in significant.items():
        # Find matching senses and add abstract frequency as a relevance tag
        senses = conn.execute("""
            SELECT sense_id, relevance_tags FROM keyword_senses
            WHERE LOWER(keyword_label) = ?
        """, [kw]).fetchall()
        for sid, tags in senses:
            tag = f"abstract_freq:{count}"
            if tags and tag not in tags:
                conn.execute("""
                    UPDATE keyword_senses
                    SET relevance_tags = list_append(relevance_tags, ?)
                    WHERE sense_id = ?
                """, [tag, sid])
                updated += 1

    logger.info(f"Updated {updated:,} senses with abstract frequency tags")

    # AUDIT
    tagged = conn.execute("""
        SELECT COUNT(*) FROM keyword_senses
        WHERE relevance_tags IS NOT NULL
          AND array_to_string(relevance_tags, ',') LIKE '%abstract_freq%'
    """).fetchone()[0]
    logger.info(f"  AUDIT: {tagged:,} senses have abstract_freq tags")

    return {"abstract_matches": len(abstract_matches), "significant": len(significant), "updated": updated}


def step2_title_keyword_extraction(conn) -> dict:
    """Extract keywords from publication titles and tag matching senses."""
    logger.info("=== STEP 2: Title Keyword Extraction ===")

    vocab = set()
    for (label,) in conn.execute("SELECT DISTINCT LOWER(keyword_label) FROM keyword_senses WHERE LENGTH(keyword_label) > 3").fetchall():
        vocab.add(label)
    logger.info(f"Vocabulary: {len(vocab):,}")

    title_matches = Counter()

    for table in ["raw_wos_publications", "raw_wos_natlab_publications"]:
        titles = conn.execute(f"""
            SELECT title FROM {table}
            WHERE title IS NOT NULL AND LENGTH(title) > 10
        """).fetchall()
        logger.info(f"  {table}: {len(titles):,} titles")

        for (title,) in titles:
            title_lower = title.lower()
            matched = set()
            for label in vocab:
                if ' ' in label and len(label) > 5 and label in title_lower:
                    matched.add(label)
            for m in matched:
                title_matches[m] += 1

    significant = {k: v for k, v in title_matches.items() if v >= 3}
    logger.info(f"Keywords in titles: {len(title_matches):,}, significant (≥3): {len(significant):,}")

    logger.info("  Top 15 title keyword matches:")
    for kw, count in sorted(significant.items(), key=lambda x: -x[1])[:15]:
        logger.info(f"    {kw}: {count:,} titles")

    updated = 0
    for kw, count in significant.items():
        senses = conn.execute("""
            SELECT sense_id, relevance_tags FROM keyword_senses
            WHERE LOWER(keyword_label) = ?
        """, [kw]).fetchall()
        for sid, tags in senses:
            tag = f"title_freq:{count}"
            if tags and tag not in tags:
                conn.execute("""
                    UPDATE keyword_senses SET relevance_tags = list_append(relevance_tags, ?)
                    WHERE sense_id = ?
                """, [tag, sid])
                updated += 1

    logger.info(f"Updated {updated:,} senses with title frequency tags")
    return {"title_matches": len(title_matches), "significant": len(significant), "updated": updated}


def step3_journal_discipline_mapping(conn) -> dict:
    """Map journals to disciplines based on keyword content of their publications."""
    logger.info("=== STEP 3: Journal→Discipline Mapping ===")

    from src.ontology import _classify_wos_keyword

    # Collect journal → category mappings
    journal_cats = defaultdict(Counter)

    for table, cat_col in [("raw_wos_publications", "subject_cat_traditional_1"),
                            ("raw_wos_natlab_publications", "subject_cat_traditional_1")]:
        rows = conn.execute(f"""
            SELECT source_title, {cat_col} FROM {table}
            WHERE source_title IS NOT NULL AND {cat_col} IS NOT NULL
        """).fetchall()
        for journal, cat in rows:
            journal_cats[journal.strip()][cat.strip()] += 1

    logger.info(f"Unique journals with categories: {len(journal_cats):,}")

    # Map each journal to its dominant discipline
    journal_disc = {}
    for journal, cats in journal_cats.items():
        top_cat = cats.most_common(1)[0][0]
        disc = _classify_wos_keyword(journal.lower(), top_cat)
        journal_disc[journal.lower()] = disc

    # Tag journal senses with discipline context
    updated = 0
    for (sid, label) in conn.execute("""
        SELECT sense_id, keyword_label FROM keyword_senses
        WHERE origin_source IN ('WoS_journal', 'WoS_natlab_publisher')
    """).fetchall():
        disc = journal_disc.get(label.lower())
        if disc:
            conn.execute("""
                UPDATE keyword_senses SET discipline_primary = ?
                WHERE sense_id = ?
            """, [disc, sid])
            updated += 1

    logger.info(f"Updated {updated:,} journal/publisher senses with discipline mapping")

    # AUDIT: check discipline distribution
    logger.info("  AUDIT journal discipline distribution:")
    for r in conn.execute("""
        SELECT discipline_primary, COUNT(*) FROM keyword_senses
        WHERE origin_source IN ('WoS_journal', 'WoS_natlab_publisher')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchall():
        logger.info(f"    {r[0]}: {r[1]:,}")

    return {"journals_mapped": len(journal_disc), "senses_updated": updated}


def step4_natlab_cooccurrence(conn) -> dict:
    """Build co-occurrence from natlab pubs (properly split keywords)."""
    logger.info("=== STEP 4: Natlab Pub Co-occurrence ===")

    # Load existing for dedup
    existing = set()
    for r in conn.execute("SELECT source_sense_id, target_sense_id, relationship_type FROM sense_relationships").fetchall():
        existing.add((r[0], r[1], r[2]))

    label_to_senses = defaultdict(list)
    for sid, label in conn.execute("SELECT sense_id, LOWER(keyword_label) FROM keyword_senses").fetchall():
        label_to_senses[label].append(sid)

    cooccur = Counter()
    total_pubs = 0

    pubs = conn.execute("""
        SELECT keywords_author, keywords_plus
        FROM raw_wos_natlab_publications
        WHERE (keywords_author IS NOT NULL AND array_length(keywords_author) > 1)
           OR (keywords_plus IS NOT NULL AND array_length(keywords_plus) > 1)
    """).fetchall()

    for kw_auth, kw_plus in pubs:
        all_kw = set()
        for kw_list in [kw_auth, kw_plus]:
            if kw_list:
                for kw in kw_list:
                    cleaned = kw.strip().lower()
                    if len(cleaned) > 2 and len(cleaned) < 100:
                        all_kw.add(cleaned)

        if len(all_kw) >= 2:
            for a, b in combinations(sorted(all_kw), 2):
                cooccur[(a, b)] += 1
            total_pubs += 1

    logger.info(f"Pubs with 2+ keywords: {total_pubs:,}")
    logger.info(f"Total pairs: {len(cooccur):,}")

    significant = {pair: count for pair, count in cooccur.items() if count >= 3}
    logger.info(f"Significant pairs (≥3): {len(significant):,}")

    # Create relationships
    rels = []
    seen = set()

    for (la, lb), count in significant.items():
        sa_list = label_to_senses.get(la, [])
        sb_list = label_to_senses.get(lb, [])
        if not sa_list or not sb_list:
            continue

        conf = min(0.9, 0.5 + count * 0.01)
        for sa in sa_list[:1]:
            for sb in sb_list[:1]:
                key = (sa, sb, "related_to")
                if key in existing or key in seen:
                    continue
                seen.add(key)
                rels.append((sa, sb, "related_to", "across", conf, "natlab_pub_cooccurrence", []))

    logger.info(f"New co-occurrence edges: {len(rels):,}")

    if rels:
        conn.execute("""CREATE TEMP TABLE IF NOT EXISTS _tmp_cooc (
            source VARCHAR, target VARCHAR, rel_type VARCHAR,
            direction VARCHAR, confidence DOUBLE, provenance VARCHAR, lens VARCHAR[]
        )""")
        conn.execute("DELETE FROM _tmp_cooc")
        for i in range(0, len(rels), 5000):
            conn.executemany("INSERT INTO _tmp_cooc VALUES (?,?,?,?,?,?,?)", rels[i:i+5000])
        conn.execute("""INSERT INTO sense_relationships
            (source_sense_id, target_sense_id, relationship_type, direction, confidence, provenance, lens_contexts)
            SELECT * FROM _tmp_cooc""")
        conn.execute("DROP TABLE _tmp_cooc")

    # AUDIT
    natlab_cooc = conn.execute("SELECT COUNT(*) FROM sense_relationships WHERE provenance = 'natlab_pub_cooccurrence'").fetchone()[0]
    logger.info(f"  AUDIT: {natlab_cooc:,} natlab co-occurrence edges total")

    return {"pairs_checked": len(cooccur), "significant": len(significant), "edges": len(rels)}


def step5_polysemy_bridges(conn) -> dict:
    """Re-run polysemy bridges with expanded label set."""
    logger.info("=== STEP 5: Updated Polysemy Bridges ===")

    existing = set()
    for r in conn.execute("SELECT source_sense_id, target_sense_id, relationship_type FROM sense_relationships").fetchall():
        existing.add((r[0], r[1], r[2]))

    polysemous = conn.execute("""
        SELECT keyword_label, LIST(sense_id), LIST(discipline_primary)
        FROM keyword_senses
        WHERE disambiguation IS NOT NULL OR origin_source != keyword_source
        GROUP BY keyword_label
        HAVING COUNT(DISTINCT origin_source) >= 2
    """).fetchall()

    logger.info(f"Polysemous labels (2+ sources): {len(polysemous):,}")

    rels = []
    seen = set()
    for label, sense_ids, disciplines in polysemous:
        for i, sid_a in enumerate(sense_ids):
            for j in range(i + 1, len(sense_ids)):
                sid_b = sense_ids[j]
                disc_a = disciplines[i]
                disc_b = disciplines[j]
                if disc_a == disc_b:
                    rel_type = "equivalent_sense"
                    direction = "toward"
                else:
                    rel_type = "cross_domain_bridge"
                    direction = "across"
                key = (sid_a, sid_b, rel_type)
                if key not in existing and key not in seen:
                    rels.append((sid_a, sid_b, rel_type, direction, 0.75, "polysemy_bridge_v2", []))
                    seen.add(key)

    logger.info(f"New polysemy edges: {len(rels):,}")

    if rels:
        conn.execute("""CREATE TEMP TABLE IF NOT EXISTS _tmp_poly (
            source VARCHAR, target VARCHAR, rel_type VARCHAR,
            direction VARCHAR, confidence DOUBLE, provenance VARCHAR, lens VARCHAR[]
        )""")
        conn.execute("DELETE FROM _tmp_poly")
        for i in range(0, len(rels), 5000):
            conn.executemany("INSERT INTO _tmp_poly VALUES (?,?,?,?,?,?,?)", rels[i:i+5000])
        conn.execute("""INSERT INTO sense_relationships
            (source_sense_id, target_sense_id, relationship_type, direction, confidence, provenance, lens_contexts)
            SELECT * FROM _tmp_poly""")
        conn.execute("DROP TABLE _tmp_poly")

    # AUDIT
    total_bridges = conn.execute("SELECT COUNT(*) FROM sense_relationships WHERE relationship_type = 'cross_domain_bridge'").fetchone()[0]
    total_equiv = conn.execute("SELECT COUNT(*) FROM sense_relationships WHERE relationship_type = 'equivalent_sense'").fetchone()[0]
    logger.info(f"  AUDIT: {total_bridges:,} cross_domain_bridges, {total_equiv:,} equivalent_senses total")

    return {"polysemous_labels": len(polysemous), "new_edges": len(rels)}


def main():
    conn = duckdb.connect("data/lake/keywords.duckdb")

    results = {}

    results["abstracts"] = step1_abstract_keyword_extraction(conn)
    results["titles"] = step2_title_keyword_extraction(conn)
    results["journals"] = step3_journal_discipline_mapping(conn)
    results["cooccurrence"] = step4_natlab_cooccurrence(conn)
    results["polysemy"] = step5_polysemy_bridges(conn)

    # Final state
    total_senses = conn.execute("SELECT COUNT(*) FROM keyword_senses").fetchone()[0]
    total_rels = conn.execute("SELECT COUNT(*) FROM sense_relationships").fetchone()[0]

    print("\n" + "=" * 60)
    print("ENRICHMENT COMPLETE")
    print("=" * 60)
    for step, data in results.items():
        print(f"\n{step}:")
        for k, v in data.items():
            print(f"  {k}: {v:,}")

    print(f"\nFinal state:")
    print(f"  Senses: {total_senses:,}")
    print(f"  Relationships: {total_rels:,}")
    print(f"  Rels/sense: {total_rels/total_senses:.3f}")

    conn.close()


if __name__ == "__main__":
    main()
