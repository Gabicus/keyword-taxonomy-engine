#!/usr/bin/env python3
"""Validation suite for the keyword taxonomy engine.

Produces artifacts for peer-review-grade validation:
  1. Stratified gold-standard sample from AI-curated orphans
  2. Cohen's kappa computation framework
  3. Confusion matrix generation
  4. Sensitivity analysis (AI-only vs corrupted ontology)
  5. Topic coherence for vector bundling (NPMI)
  6. Lens divergence heatmap
  7. Citation-coherence (if citation data available)

Outputs: data/validation/ (JSON + CSV) and figures/validation/ (PNG).
"""
import duckdb
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

DB_PATH = Path("data/lake/keywords.duckdb")
OUT_DIR = Path("data/validation")
FIG_DIR = Path("figures/validation")


def ensure_dirs():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 1. Stratified Gold-Standard Sample
# ============================================================

def build_gold_standard_sample(conn, n: int = 400) -> list[dict]:
    """Pull stratified sample from AI-curated orphan connections for human annotation.

    AI curation created sense_relationships (not senses), so we sample from
    relationships with ai_reasoning provenance and present the source sense
    + connection for human judgment.

    Stratifies across:
      - AI strategy (from relationship provenance: discipline_anchor, contains_term, etc.)
      - Confidence buckets (high/med/low)
    """
    rows = conn.execute("""
        SELECT
            ks.sense_id, ks.keyword_label, ks.discipline_primary,
            ks.confidence, ks.provenance as sense_prov,
            ks.relevance_tags, ks.definition_in_context, ks.disambiguation,
            sr.relationship_type, sr.confidence as rel_confidence,
            sr.provenance as rel_provenance,
            t.keyword_label as target_label, t.discipline_primary as target_disc
        FROM sense_relationships sr
        JOIN keyword_senses ks ON ks.sense_id = sr.source_sense_id
        JOIN keyword_senses t ON t.sense_id = sr.target_sense_id
        WHERE sr.provenance LIKE 'ai_reasoning%'
    """).fetchall()

    sense_map = {}
    for row in rows:
        sid = row[0]
        rel_prov = row[10]
        key = f"{sid}|{rel_prov}"
        if key not in sense_map:
            sense_map[key] = {
                "sense_id": sid,
                "keyword_label": row[1],
                "discipline": row[2],
                "confidence": row[3],
                "sense_provenance": row[4],
                "tags": row[5],
                "definition": row[6],
                "disambiguation": row[7],
                "rel_type": row[8],
                "rel_confidence": row[9],
                "ai_provenance": rel_prov,
                "target_label": row[11],
                "target_discipline": row[12],
            }

    senses = list(sense_map.values())
    if not senses:
        print("WARNING: No AI-curated relationships found.", flush=True)
        return []

    print(f"  Total AI-curated connections: {len(senses)}", flush=True)

    strategy_map = defaultdict(list)
    for s in senses:
        prov = s["ai_provenance"]
        parts = prov.split(":")
        strategy = parts[1] if len(parts) > 1 else parts[0]
        conf = s["rel_confidence"] or 0
        if conf >= 0.7:
            bucket = "high"
        elif conf >= 0.4:
            bucket = "medium"
        else:
            bucket = "low"
        key = f"{strategy}|{bucket}"
        strategy_map[key].append(s)

    sample = []
    strata_counts = {}
    per_stratum = max(1, n // len(strategy_map))
    remainder = n - per_stratum * len(strategy_map)

    for key, items in sorted(strategy_map.items()):
        take = min(per_stratum, len(items))
        chosen = random.sample(items, take)
        sample.extend(chosen)
        strata_counts[key] = take

    if len(sample) < n and remainder > 0:
        remaining = [s for s in senses if s not in sample]
        extra = min(remainder, len(remaining))
        sample.extend(random.sample(remaining, extra))

    for s in sample:
        s["human_label"] = None
        s["human_discipline"] = None
        s["human_notes"] = ""
        s["annotator"] = ""

    print(f"Gold-standard sample: {len(sample)} items from {len(strategy_map)} strata", flush=True)
    for key, count in sorted(strata_counts.items()):
        total = len(strategy_map[key])
        print(f"  {key:40s} {count:>4d} / {total:>5d}", flush=True)

    return sample


# ============================================================
# 2. Cohen's Kappa Computation
# ============================================================

def cohens_kappa(labels_a: list, labels_b: list) -> dict:
    """Compute Cohen's kappa between two annotators."""
    assert len(labels_a) == len(labels_b)
    n = len(labels_a)
    if n == 0:
        return {"kappa": 0.0, "agreement": 0.0, "n": 0}

    categories = sorted(set(labels_a) | set(labels_b))
    cat_idx = {c: i for i, c in enumerate(categories)}
    k = len(categories)

    matrix = np.zeros((k, k), dtype=int)
    for a, b in zip(labels_a, labels_b):
        matrix[cat_idx[a], cat_idx[b]] += 1

    observed_agreement = np.trace(matrix) / n

    row_marginals = matrix.sum(axis=1)
    col_marginals = matrix.sum(axis=0)
    expected_agreement = (row_marginals * col_marginals).sum() / (n * n)

    if expected_agreement == 1.0:
        kappa = 1.0
    else:
        kappa = (observed_agreement - expected_agreement) / (1 - expected_agreement)

    return {
        "kappa": round(kappa, 4),
        "observed_agreement": round(observed_agreement, 4),
        "expected_agreement": round(expected_agreement, 4),
        "n": n,
        "categories": categories,
        "confusion_matrix": matrix.tolist(),
    }


def per_category_metrics(labels_true: list, labels_pred: list) -> dict:
    """Compute per-category precision, recall, F1."""
    categories = sorted(set(labels_true) | set(labels_pred))
    metrics = {}

    for cat in categories:
        tp = sum(1 for t, p in zip(labels_true, labels_pred) if t == cat and p == cat)
        fp = sum(1 for t, p in zip(labels_true, labels_pred) if t != cat and p == cat)
        fn = sum(1 for t, p in zip(labels_true, labels_pred) if t == cat and p != cat)

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

        metrics[cat] = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "support": tp + fn,
        }

    return metrics


# ============================================================
# 3. Sensitivity Analysis
# ============================================================

def build_corrupted_ontology(conn, corruption_rate: float = 0.10) -> int:
    """Create corrupted version by randomly removing AI-curated relationships.

    Simulates what happens if the AI curation was wrong for N% of connections.
    Operates on a temp table that shadows sense_relationships.
    """
    conn.execute("DROP TABLE IF EXISTS corrupted_relationships")

    total = conn.execute("""
        SELECT COUNT(*) FROM sense_relationships WHERE provenance LIKE 'ai_reasoning%'
    """).fetchone()[0]
    n_corrupt = int(total * corruption_rate)

    conn.execute(f"""
        CREATE TEMP TABLE corrupted_relationships AS
        SELECT source_sense_id, target_sense_id, relationship_type
        FROM sense_relationships
        WHERE provenance LIKE 'ai_reasoning%'
        ORDER BY RANDOM()
        LIMIT {n_corrupt}
    """)

    actual = conn.execute("SELECT COUNT(*) FROM corrupted_relationships").fetchone()[0]
    print(f"Marked {actual}/{total} AI-curated relationships for removal ({corruption_rate*100:.0f}%)", flush=True)
    return actual


def sensitivity_analysis(conn) -> dict:
    """Compare lens query results across AI-only vs corrupted ontology."""
    test_queries = [
        ("fossil_energy", "carbon"),
        ("earth_environmental", "climate"),
        ("materials", "polymer"),
        ("chemical_sciences", "catalyst"),
        ("computation_data", "simulation"),
    ]

    results = {}
    for disc, search in test_queries:
        original = conn.execute(f"""
            SELECT ks.sense_id, ks.keyword_label
            FROM keyword_senses ks
            WHERE ks.discipline_primary = '{disc}'
              AND LOWER(ks.keyword_label) LIKE '%{search}%'
            ORDER BY ks.confidence DESC
            LIMIT 50
        """).fetchall()
        original_ids = set(r[0] for r in original)

        corrupted = conn.execute(f"""
            WITH connected AS (
                SELECT DISTINCT source_sense_id as sid FROM sense_relationships
                WHERE provenance LIKE 'ai_reasoning%'
                  AND (source_sense_id, target_sense_id, relationship_type)
                      NOT IN (SELECT * FROM corrupted_relationships)
                UNION
                SELECT DISTINCT target_sense_id FROM sense_relationships
                WHERE provenance LIKE 'ai_reasoning%'
                  AND (source_sense_id, target_sense_id, relationship_type)
                      NOT IN (SELECT * FROM corrupted_relationships)
            )
            SELECT ks.sense_id, ks.keyword_label
            FROM keyword_senses ks
            WHERE ks.discipline_primary = '{disc}'
              AND LOWER(ks.keyword_label) LIKE '%{search}%'
              AND (ks.sense_id NOT IN (
                  SELECT DISTINCT source_sense_id FROM sense_relationships
                  WHERE provenance LIKE 'ai_reasoning%'
              ) OR ks.sense_id IN (SELECT sid FROM connected))
            ORDER BY ks.confidence DESC
            LIMIT 50
        """).fetchall()
        corrupted_ids = set(r[0] for r in corrupted)

        overlap = len(original_ids & corrupted_ids)
        total = len(original_ids | corrupted_ids)
        jaccard = overlap / total if total > 0 else 1.0

        results[f"{disc}:{search}"] = {
            "original_count": len(original_ids),
            "corrupted_count": len(corrupted_ids),
            "overlap": overlap,
            "jaccard_similarity": round(jaccard, 4),
        }

    return results


# ============================================================
# 4. Topic Coherence (NPMI) for Vector Bundling
# ============================================================

def compute_neighborhood_coherence(conn, sample_size: int = 50) -> list[dict]:
    """Compute NPMI-based coherence for keyword neighborhoods.

    For each sampled keyword, get its sense neighbors and compute
    how often those neighbors co-occur in publications.
    """
    centers = conn.execute(f"""
        SELECT ks.sense_id, ks.keyword_label, ks.discipline_primary
        FROM keyword_senses ks
        JOIN sense_relationships sr ON sr.source_sense_id = ks.sense_id
        WHERE ks.confidence >= 0.5
        GROUP BY ks.sense_id, ks.keyword_label, ks.discipline_primary
        HAVING COUNT(*) >= 5
        ORDER BY RANDOM()
        LIMIT {sample_size}
    """).fetchall()

    total_pubs = conn.execute(
        "SELECT COUNT(DISTINCT openalex_id) FROM openalex_pub_keywords"
    ).fetchone()[0]

    results = []
    for sense_id, label, disc in centers:
        neighbors = conn.execute("""
            SELECT DISTINCT ks2.keyword_label
            FROM sense_relationships sr
            JOIN keyword_senses ks2 ON ks2.sense_id = sr.target_sense_id
            WHERE sr.source_sense_id = ?
            LIMIT 20
        """, [sense_id]).fetchall()
        neighbor_labels = [n[0] for n in neighbors]

        if len(neighbor_labels) < 3:
            continue

        pair_npmis = []
        for i in range(len(neighbor_labels)):
            for j in range(i + 1, min(i + 5, len(neighbor_labels))):
                w1, w2 = neighbor_labels[i], neighbor_labels[j]
                counts = conn.execute("""
                    SELECT
                        (SELECT COUNT(DISTINCT openalex_id) FROM openalex_pub_keywords
                         WHERE LOWER(keyword_label) = LOWER(?)) as c1,
                        (SELECT COUNT(DISTINCT openalex_id) FROM openalex_pub_keywords
                         WHERE LOWER(keyword_label) = LOWER(?)) as c2,
                        (SELECT COUNT(DISTINCT a.openalex_id)
                         FROM openalex_pub_keywords a
                         JOIN openalex_pub_keywords b ON a.openalex_id = b.openalex_id
                         WHERE LOWER(a.keyword_label) = LOWER(?)
                           AND LOWER(b.keyword_label) = LOWER(?)) as c12
                """, [w1, w2, w1, w2]).fetchone()

                c1, c2, c12 = counts
                if c1 > 0 and c2 > 0 and c12 > 0:
                    p1 = c1 / total_pubs
                    p2 = c2 / total_pubs
                    p12 = c12 / total_pubs
                    pmi = np.log(p12 / (p1 * p2))
                    npmi = pmi / (-np.log(p12))
                    pair_npmis.append(npmi)

        coherence = float(np.mean(pair_npmis)) if pair_npmis else 0.0
        results.append({
            "sense_id": sense_id,
            "keyword": label,
            "discipline": disc,
            "n_neighbors": len(neighbor_labels),
            "n_pairs_scored": len(pair_npmis),
            "coherence_npmi": round(coherence, 4),
        })

    return results


# ============================================================
# 5. Lens Divergence Analysis
# ============================================================

def compute_lens_divergence(conn, sample_keywords: int = 100) -> dict:
    """Compute pairwise rank correlation between lenses.

    For sample keywords, get their neighborhood rankings across all
    template lenses and compute Spearman correlation.
    """
    from scipy import stats as scipy_stats

    lenses = conn.execute("""
        SELECT lens_id, discipline_primary, discipline_weights
        FROM ontology_lenses
        WHERE is_template = true AND role_type = 'researcher'
        ORDER BY discipline_primary
    """).fetchall()

    keywords = conn.execute(f"""
        SELECT keyword_label FROM keyword_senses
        WHERE confidence >= 0.6
        GROUP BY keyword_label
        HAVING COUNT(DISTINCT discipline_primary) >= 2
        ORDER BY RANDOM()
        LIMIT {sample_keywords}
    """).fetchall()
    keyword_list = [k[0] for k in keywords]

    lens_rankings = {}
    for lens_id, disc, weights_json in lenses:
        weights = json.loads(weights_json) if isinstance(weights_json, str) else weights_json
        rankings = {}
        for kw in keyword_list:
            senses = conn.execute("""
                SELECT sense_id, discipline_primary, confidence
                FROM keyword_senses
                WHERE LOWER(keyword_label) = LOWER(?)
                ORDER BY confidence DESC
            """, [kw]).fetchall()

            score = 0.0
            for sid, d, conf in senses:
                w = weights.get(d, 0.05)
                score += w * (conf or 0.5)
            rankings[kw] = score

        lens_rankings[lens_id] = rankings

    lens_ids = list(lens_rankings.keys())
    n = len(lens_ids)
    corr_matrix = np.zeros((n, n))

    for i in range(n):
        for j in range(n):
            if i == j:
                corr_matrix[i, j] = 1.0
                continue
            ranks_i = [lens_rankings[lens_ids[i]].get(kw, 0) for kw in keyword_list]
            ranks_j = [lens_rankings[lens_ids[j]].get(kw, 0) for kw in keyword_list]
            if len(set(ranks_i)) <= 1 or len(set(ranks_j)) <= 1:
                corr_matrix[i, j] = 0.0
            else:
                corr, _ = scipy_stats.spearmanr(ranks_i, ranks_j)
                corr_matrix[i, j] = corr

    return {
        "lens_ids": lens_ids,
        "disciplines": [l[1] for l in lenses],
        "correlation_matrix": corr_matrix.tolist(),
        "n_keywords": len(keyword_list),
    }


# ============================================================
# 6. Generate Figures
# ============================================================

def plot_confusion_matrix(kappa_result: dict, output_path: Path):
    """Figure 1: Confusion matrix, AI vs human consensus."""
    import matplotlib.pyplot as plt

    matrix = np.array(kappa_result["confusion_matrix"])
    categories = kappa_result["categories"]

    fig, ax = plt.subplots(figsize=(max(8, len(categories)), max(6, len(categories) * 0.8)))
    im = ax.imshow(matrix, cmap="Blues")

    ax.set_xticks(range(len(categories)))
    ax.set_yticks(range(len(categories)))
    ax.set_xticklabels(categories, rotation=45, ha="right", fontsize=8)
    ax.set_yticklabels(categories, fontsize=8)
    ax.set_xlabel("Annotator B")
    ax.set_ylabel("Annotator A")
    ax.set_title(f"Inter-Annotator Confusion Matrix (κ = {kappa_result['kappa']:.3f})")

    for i in range(len(categories)):
        for j in range(len(categories)):
            ax.text(j, i, str(matrix[i, j]), ha="center", va="center",
                    color="white" if matrix[i, j] > matrix.max() / 2 else "black")

    fig.colorbar(im)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved {output_path}", flush=True)


def plot_sensitivity(sensitivity_results: dict, output_path: Path):
    """Figure 2: Sensitivity bar chart — Jaccard similarity under corruption."""
    import matplotlib.pyplot as plt

    queries = list(sensitivity_results.keys())
    jaccards = [sensitivity_results[q]["jaccard_similarity"] for q in queries]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(range(len(queries)), jaccards, color="#4A90D9")
    ax.axhline(y=0.95, color="green", linestyle="--", alpha=0.7, label="Target (≥0.95)")
    ax.axhline(y=0.90, color="orange", linestyle="--", alpha=0.7, label="Warning (<0.90)")
    ax.set_xticks(range(len(queries)))
    ax.set_xticklabels([q.replace(":", "\n") for q in queries], fontsize=9)
    ax.set_ylabel("Jaccard Similarity (AI-only vs 10% corrupted)")
    ax.set_title("Sensitivity Analysis: Lens Query Stability Under Ontology Corruption")
    ax.set_ylim(0, 1.05)
    ax.legend()

    for bar, val in zip(bars, jaccards):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                f"{val:.3f}", ha="center", fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved {output_path}", flush=True)


def plot_coherence_distribution(coherence_results: list[dict], output_path: Path):
    """Figure 3: Coherence score distribution across neighborhoods."""
    import matplotlib.pyplot as plt

    scores = [r["coherence_npmi"] for r in coherence_results if r["n_pairs_scored"] > 0]
    disciplines = [r["discipline"] for r in coherence_results if r["n_pairs_scored"] > 0]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.hist(scores, bins=30, color="#4A90D9", edgecolor="white")
    ax1.axvline(np.mean(scores), color="red", linestyle="--",
                label=f"Mean: {np.mean(scores):.3f}")
    ax1.set_xlabel("NPMI Coherence Score")
    ax1.set_ylabel("Count")
    ax1.set_title("Neighborhood Coherence Distribution")
    ax1.legend()

    disc_scores = defaultdict(list)
    for s, d in zip(scores, disciplines):
        disc_scores[d].append(s)

    disc_labels = sorted(disc_scores.keys(), key=lambda d: -np.mean(disc_scores[d]))
    disc_means = [np.mean(disc_scores[d]) for d in disc_labels]
    disc_stds = [np.std(disc_scores[d]) for d in disc_labels]

    ax2.barh(range(len(disc_labels)), disc_means, xerr=disc_stds,
             color="#4A90D9", alpha=0.8)
    ax2.set_yticks(range(len(disc_labels)))
    ax2.set_yticklabels(disc_labels, fontsize=8)
    ax2.set_xlabel("Mean NPMI Coherence")
    ax2.set_title("Coherence by Discipline")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved {output_path}", flush=True)


def plot_lens_divergence(divergence_data: dict, output_path: Path):
    """Figure 4: Lens divergence heatmap — pairwise rank correlations."""
    import matplotlib.pyplot as plt

    matrix = np.array(divergence_data["correlation_matrix"])
    disciplines = divergence_data["disciplines"]

    fig, ax = plt.subplots(figsize=(12, 10))
    im = ax.imshow(matrix, cmap="RdYlBu", vmin=-0.5, vmax=1.0)

    ax.set_xticks(range(len(disciplines)))
    ax.set_yticks(range(len(disciplines)))
    ax.set_xticklabels(disciplines, rotation=45, ha="right", fontsize=8)
    ax.set_yticklabels(disciplines, fontsize=8)
    ax.set_title(f"Lens Divergence: Pairwise Spearman Correlation (n={divergence_data['n_keywords']} keywords)")

    for i in range(len(disciplines)):
        for j in range(len(disciplines)):
            val = matrix[i, j]
            ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                    color="white" if abs(val) > 0.5 else "black", fontsize=7)

    fig.colorbar(im, label="Spearman ρ")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved {output_path}", flush=True)


# ============================================================
# Main
# ============================================================

def main():
    random.seed(42)
    np.random.seed(42)

    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found", file=sys.stderr)
        sys.exit(1)

    ensure_dirs()
    conn = duckdb.connect(str(DB_PATH), read_only=False)

    try:
        # 1. Gold-standard sample
        print("\n=== 1. Building Gold-Standard Sample ===", flush=True)
        sample = build_gold_standard_sample(conn, n=400)
        sample_path = OUT_DIR / "gold_standard_sample.json"
        with open(sample_path, "w") as f:
            json.dump(sample, f, indent=2, default=str)
        print(f"  Saved {sample_path} ({len(sample)} items)", flush=True)

        # 2. Sensitivity analysis
        print("\n=== 2. Sensitivity Analysis ===", flush=True)
        build_corrupted_ontology(conn, corruption_rate=0.10)
        sensitivity = sensitivity_analysis(conn)
        sens_path = OUT_DIR / "sensitivity_analysis.json"
        with open(sens_path, "w") as f:
            json.dump(sensitivity, f, indent=2)
        print(f"  Saved {sens_path}", flush=True)

        avg_jaccard = np.mean([v["jaccard_similarity"] for v in sensitivity.values()])
        print(f"  Average Jaccard similarity: {avg_jaccard:.4f}", flush=True)
        if avg_jaccard >= 0.95:
            print(f"  PASS: Ontology is robust to 10% corruption.", flush=True)
        elif avg_jaccard >= 0.90:
            print(f"  WARN: Some sensitivity detected.", flush=True)
        else:
            print(f"  FAIL: Significant sensitivity to corruption.", flush=True)

        plot_sensitivity(sensitivity, FIG_DIR / "fig2_sensitivity.png")

        # 3. Topic coherence
        print("\n=== 3. Neighborhood Coherence (NPMI) ===", flush=True)
        coherence = compute_neighborhood_coherence(conn, sample_size=50)
        coh_path = OUT_DIR / "coherence_scores.json"
        with open(coh_path, "w") as f:
            json.dump(coherence, f, indent=2)

        scored = [c for c in coherence if c["n_pairs_scored"] > 0]
        if scored:
            mean_coh = np.mean([c["coherence_npmi"] for c in scored])
            print(f"  {len(scored)} neighborhoods scored, mean NPMI: {mean_coh:.4f}", flush=True)
            plot_coherence_distribution(coherence, FIG_DIR / "fig3_coherence.png")
        else:
            print(f"  No neighborhoods had scorable pairs.", flush=True)

        # 4. Lens divergence
        print("\n=== 4. Lens Divergence Analysis ===", flush=True)
        try:
            divergence = compute_lens_divergence(conn, sample_keywords=100)
            div_path = OUT_DIR / "lens_divergence.json"
            with open(div_path, "w") as f:
                json.dump(divergence, f, indent=2)

            corr_matrix = np.array(divergence["correlation_matrix"])
            mask = np.triu_indices_from(corr_matrix, k=1)
            upper = corr_matrix[mask]
            print(f"  Mean pairwise correlation: {np.mean(upper):.4f}", flush=True)
            print(f"  Min: {np.min(upper):.4f}  Max: {np.max(upper):.4f}", flush=True)

            plot_lens_divergence(divergence, FIG_DIR / "fig4_lens_divergence.png")
        except ImportError:
            print("  SKIP: scipy not installed (needed for Spearman correlation)", flush=True)

        # Summary
        print("\n=== Validation Summary ===", flush=True)
        print(f"  Gold-standard sample:   {len(sample)} items ready for annotation", flush=True)
        print(f"  Sensitivity (Jaccard):  {avg_jaccard:.4f} ({'PASS' if avg_jaccard >= 0.95 else 'NEEDS REVIEW'})", flush=True)
        if scored:
            print(f"  Coherence (NPMI):       {mean_coh:.4f}", flush=True)
        print(f"  Outputs in:             {OUT_DIR}/  and  {FIG_DIR}/", flush=True)
        print(f"\n  NEXT: Have 2 domain experts annotate {sample_path}", flush=True)
        print(f"  Then run: python scripts/validation_suite.py --compute-kappa", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
