"""Build relationship edges from multiple strategies.

Run after hierarchy edges (Strategy 1) are already inserted.
Strategies 2-5 add label containment, pub co-occurrence,
polysemy bridges, and TF-IDF similarity edges.
"""

import duckdb
import logging
import re
from collections import defaultdict, Counter
from itertools import combinations

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _insert_new_rels(conn, rels: list[tuple], existing_rels: set) -> int:
    """Insert relationships that don't already exist. Returns count inserted."""
    seen = set()
    truly_new = []
    for r in rels:
        key = (r[0], r[1], r[2])
        if key not in existing_rels and key not in seen:
            truly_new.append(r)
            seen.add(key)
    if not truly_new:
        return 0
    conn.execute("""CREATE TEMP TABLE IF NOT EXISTS _tmp_newrels (
        source VARCHAR, target VARCHAR, rel_type VARCHAR,
        direction VARCHAR, confidence DOUBLE, provenance VARCHAR, lens VARCHAR[]
    )""")
    conn.execute("DELETE FROM _tmp_newrels")
    batch = 5000
    for i in range(0, len(truly_new), batch):
        conn.executemany(
            "INSERT INTO _tmp_newrels VALUES (?,?,?,?,?,?,?)",
            [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in truly_new[i:i + batch]],
        )
    conn.execute("""INSERT INTO sense_relationships
        (source_sense_id, target_sense_id, relationship_type, direction, confidence, provenance, lens_contexts)
        SELECT source, target, rel_type, direction, confidence, provenance, lens FROM _tmp_newrels""")
    conn.execute("DROP TABLE _tmp_newrels")
    for key in seen:
        existing_rels.add(key)
    return len(truly_new)


def strategy2_label_containment(conn, existing_rels: set) -> int:
    """Multi-word senses that contain single-word senses → subtopic_of.

    "carbon capture" subtopic_of "carbon"
    "coal gasification" subtopic_of "coal"
    """
    logger.info("=== Strategy 2: Label Containment ===")

    senses = conn.execute("""
        SELECT sense_id, LOWER(keyword_label) as label, discipline_primary
        FROM keyword_senses
        WHERE LENGTH(keyword_label) > 2
    """).fetchall()

    single_word = {}
    multi_word = []
    for sid, label, disc in senses:
        if ' ' not in label:
            if label not in single_word:
                single_word[label] = []
            single_word[label].append((sid, disc))
        else:
            multi_word.append((sid, label, disc))

    logger.info(f"Single-word senses: {len(single_word)} unique labels")
    logger.info(f"Multi-word senses: {len(multi_word)}")

    rels = []
    for child_sid, child_label, child_disc in multi_word:
        words = child_label.split()
        for word in words:
            if word in single_word and len(word) > 3:
                for parent_sid, parent_disc in single_word[word]:
                    if parent_sid == child_sid:
                        continue
                    if child_disc == parent_disc:
                        direction = "toward"
                    else:
                        direction = "across"
                    rels.append((
                        child_sid, parent_sid, "subtopic_of",
                        direction, 0.6, "label_containment", [],
                    ))

    inserted = _insert_new_rels(conn, rels, existing_rels)
    logger.info(f"Label containment: {len(rels)} candidates, {inserted} new")
    return inserted


def strategy3_pub_cooccurrence(conn, existing_rels: set, min_cooccur: int = 3) -> int:
    """Keywords appearing together in WoS publications → related_to.

    Only creates edges for keyword pairs co-occurring in min_cooccur+ papers.
    """
    logger.info("=== Strategy 3: Publication Co-occurrence ===")

    pubs = conn.execute("""
        SELECT accession_number, keywords_author, keywords_plus
        FROM raw_wos_publications
        WHERE keywords_author IS NOT NULL OR keywords_plus IS NOT NULL
    """).fetchall()

    # Build co-occurrence matrix
    cooccur = Counter()
    for acc, kw_auth, kw_plus in pubs:
        all_kw = set()
        for kw_list in [kw_auth, kw_plus]:
            if kw_list:
                for kw in kw_list:
                    all_kw.add(kw.lower().strip())

        for a, b in combinations(sorted(all_kw), 2):
            if len(a) > 2 and len(b) > 2:
                cooccur[(a, b)] += 1

    significant = {pair: count for pair, count in cooccur.items() if count >= min_cooccur}
    logger.info(f"Co-occurrence pairs ≥{min_cooccur}: {len(significant)}")

    # Map labels to sense IDs
    label_to_senses = defaultdict(list)
    for sid, label in conn.execute(
        "SELECT sense_id, LOWER(keyword_label) FROM keyword_senses"
    ).fetchall():
        label_to_senses[label].append(sid)

    rels = []
    for (label_a, label_b), count in significant.items():
        senses_a = label_to_senses.get(label_a, [])
        senses_b = label_to_senses.get(label_b, [])
        if not senses_a or not senses_b:
            continue

        conf = min(0.9, 0.5 + count * 0.02)
        for sa in senses_a[:2]:
            for sb in senses_b[:2]:
                disc_a = conn.execute(
                    "SELECT discipline_primary FROM keyword_senses WHERE sense_id = ?", [sa]
                ).fetchone()
                disc_b = conn.execute(
                    "SELECT discipline_primary FROM keyword_senses WHERE sense_id = ?", [sb]
                ).fetchone()
                if disc_a and disc_b:
                    direction = "toward" if disc_a[0] == disc_b[0] else "across"
                    rels.append((sa, sb, "related_to", direction, conf, "pub_cooccurrence", []))

    inserted = _insert_new_rels(conn, rels, existing_rels)
    logger.info(f"Pub co-occurrence: {len(rels)} candidates, {inserted} new")
    return inserted


def strategy4_polysemy_bridges(conn, existing_rels: set) -> int:
    """Polysemous labels (same term, different sources) → equivalent_sense or cross_domain_bridge."""
    logger.info("=== Strategy 4: Polysemy Bridges ===")

    polysemous = conn.execute("""
        SELECT keyword_label, LIST(sense_id), LIST(discipline_primary)
        FROM keyword_senses
        WHERE disambiguation IS NOT NULL
        GROUP BY keyword_label
        HAVING COUNT(*) >= 2
    """).fetchall()

    rels = []
    for label, sense_ids, disciplines in polysemous:
        for i, sid_a in enumerate(sense_ids):
            for sid_b in sense_ids[i + 1:]:
                disc_a = disciplines[i]
                disc_b = disciplines[sense_ids.index(sid_b)]
                if disc_a == disc_b:
                    rel_type = "equivalent_sense"
                    direction = "toward"
                else:
                    rel_type = "cross_domain_bridge"
                    direction = "across"
                rels.append((sid_a, sid_b, rel_type, direction, 0.75, "polysemy_bridge", []))

    inserted = _insert_new_rels(conn, rels, existing_rels)
    logger.info(f"Polysemy bridges: {len(rels)} candidates, {inserted} new")
    return inserted


def strategy5_tfidf_similarity(conn, existing_rels: set, top_n: int = 30000, threshold: float = 0.4) -> int:
    """TF-IDF cosine similarity on definitions → related_to edges.

    Only for senses that HAVE definitions (avoids noise from bare labels).
    """
    logger.info("=== Strategy 5: TF-IDF Similarity ===")

    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np

    senses_with_defs = conn.execute("""
        SELECT sense_id, keyword_label, definition_in_context, discipline_primary
        FROM keyword_senses
        WHERE definition_in_context IS NOT NULL
          AND LENGTH(definition_in_context) > 20
    """).fetchall()

    logger.info(f"Senses with definitions: {len(senses_with_defs)}")

    if len(senses_with_defs) < 100:
        logger.info("Too few definitions for TF-IDF")
        return 0

    texts = [f"{label}. {defn}" for _, label, defn, _ in senses_with_defs]

    tfidf = TfidfVectorizer(max_features=10000, ngram_range=(1, 2), stop_words='english')
    matrix = tfidf.fit_transform(texts)
    logger.info(f"TF-IDF matrix: {matrix.shape}")

    # Process in chunks to avoid memory explosion
    rels = []
    chunk_size = 2000
    for start in range(0, len(senses_with_defs), chunk_size):
        end = min(start + chunk_size, len(senses_with_defs))
        chunk_sim = cosine_similarity(matrix[start:end], matrix)

        for i in range(end - start):
            global_i = start + i
            sim_row = chunk_sim[i]
            top_indices = np.argsort(sim_row)[-20:][::-1]

            for j in top_indices:
                if j == global_i:
                    continue
                score = float(sim_row[j])
                if score < threshold:
                    continue

                sid_a = senses_with_defs[global_i][0]
                sid_b = senses_with_defs[j][0]
                disc_a = senses_with_defs[global_i][3]
                disc_b = senses_with_defs[j][3]

                if disc_a == disc_b:
                    direction = "toward"
                else:
                    direction = "across"

                rels.append((sid_a, sid_b, "related_to", direction, round(score, 3), "tfidf_similarity", []))

                if len(rels) >= top_n:
                    break
            if len(rels) >= top_n:
                break
        if len(rels) >= top_n:
            break

    inserted = _insert_new_rels(conn, rels, existing_rels)
    logger.info(f"TF-IDF similarity: {len(rels)} candidates, {inserted} new")
    return inserted


def main():
    conn = duckdb.connect('data/lake/keywords.duckdb')

    # Load existing relationships
    existing_rels = set()
    for r in conn.execute("SELECT source_sense_id, target_sense_id, relationship_type FROM sense_relationships").fetchall():
        existing_rels.add((r[0], r[1], r[2]))
    logger.info(f"Existing relationships: {len(existing_rels)}")

    results = {}
    results["label_containment"] = strategy2_label_containment(conn, existing_rels)
    results["pub_cooccurrence"] = strategy3_pub_cooccurrence(conn, existing_rels)
    results["polysemy_bridges"] = strategy4_polysemy_bridges(conn, existing_rels)
    results["tfidf_similarity"] = strategy5_tfidf_similarity(conn, existing_rels)

    total = conn.execute("SELECT COUNT(*) FROM sense_relationships").fetchone()[0]
    total_senses = conn.execute("SELECT COUNT(*) FROM keyword_senses").fetchone()[0]

    print(f"\n=== RELATIONSHIP BUILD COMPLETE ===")
    print(f"Results per strategy:")
    for k, v in results.items():
        print(f"  {k}: {v:,} new edges")
    print(f"\nTotal relationships: {total:,}")
    print(f"Total senses: {total_senses:,}")
    print(f"Ratio: {total/total_senses:.3f} rels/sense")

    print(f"\nBy type:")
    for row in conn.execute("SELECT relationship_type, COUNT(*) FROM sense_relationships GROUP BY relationship_type ORDER BY COUNT(*) DESC").fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    print(f"\nBy provenance:")
    for row in conn.execute("SELECT provenance, COUNT(*) FROM sense_relationships GROUP BY provenance ORDER BY COUNT(*) DESC").fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    conn.close()


if __name__ == "__main__":
    main()
