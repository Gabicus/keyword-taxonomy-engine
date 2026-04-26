#!/usr/bin/env python3
"""Citation-coherence validation: do citing papers share keyword neighborhoods?

Tests the hypothesis that papers connected by citations should have more
keyword overlap (through the ontology) than random paper pairs. This validates
that the keyword bundling captures meaningful semantic structure.

Produces:
  - data/validation/citation_coherence.json
  - figures/validation/fig5_citation_coherence.png
"""
import duckdb
import json
import sys
import numpy as np
from collections import defaultdict
from pathlib import Path

DB_PATH = Path("data/lake/keywords.duckdb")
OUT_DIR = Path("data/validation")
FIG_DIR = Path("figures/validation")


def get_paper_keyword_sets(conn) -> dict[str, set[str]]:
    """Get keyword sets for each paper (lowercased labels)."""
    rows = conn.execute("""
        SELECT openalex_id, keyword_label
        FROM openalex_pub_keywords
        WHERE relevance_score >= 0.3
    """).fetchall()

    paper_kw = defaultdict(set)
    for oa_id, label in rows:
        paper_kw[oa_id].add(label.lower())

    return dict(paper_kw)


def get_paper_sense_sets(conn) -> dict[str, set[str]]:
    """Get sense neighborhoods for each paper (discipline-tagged)."""
    rows = conn.execute("""
        SELECT pk.openalex_id, ks.discipline_primary
        FROM openalex_pub_keywords pk
        JOIN keyword_senses ks ON LOWER(pk.keyword_label) = LOWER(ks.keyword_label)
        WHERE pk.relevance_score >= 0.3
    """).fetchall()

    paper_discs = defaultdict(set)
    for oa_id, disc in rows:
        paper_discs[oa_id].add(disc)

    return dict(paper_discs)


def jaccard(set_a: set, set_b: set) -> float:
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def compute_citation_coherence(conn, paper_kw: dict, paper_discs: dict,
                                n_random: int = 5000) -> dict:
    """Compare keyword overlap: citing pairs vs random pairs."""

    in_corpus_edges = conn.execute("""
        SELECT citing_id, cited_id
        FROM openalex_citations
        WHERE in_corpus = true
    """).fetchall()

    if not in_corpus_edges:
        print("No in-corpus citation edges found yet.", flush=True)
        return {"status": "no_data"}

    print(f"  In-corpus citation edges: {len(in_corpus_edges)}", flush=True)

    cite_kw_jaccards = []
    cite_disc_jaccards = []
    for citing, cited in in_corpus_edges:
        kw_a = paper_kw.get(citing, set())
        kw_b = paper_kw.get(cited, set())
        if kw_a and kw_b:
            cite_kw_jaccards.append(jaccard(kw_a, kw_b))

        disc_a = paper_discs.get(citing, set())
        disc_b = paper_discs.get(cited, set())
        if disc_a and disc_b:
            cite_disc_jaccards.append(jaccard(disc_a, disc_b))

    all_papers = list(paper_kw.keys())
    rng = np.random.RandomState(42)
    random_kw_jaccards = []
    random_disc_jaccards = []

    for _ in range(n_random):
        i, j = rng.choice(len(all_papers), 2, replace=False)
        p1, p2 = all_papers[i], all_papers[j]

        kw_a = paper_kw.get(p1, set())
        kw_b = paper_kw.get(p2, set())
        if kw_a and kw_b:
            random_kw_jaccards.append(jaccard(kw_a, kw_b))

        disc_a = paper_discs.get(p1, set())
        disc_b = paper_discs.get(p2, set())
        if disc_a and disc_b:
            random_disc_jaccards.append(jaccard(disc_a, disc_b))

    from scipy import stats as scipy_stats
    kw_stat, kw_pval = scipy_stats.mannwhitneyu(
        cite_kw_jaccards, random_kw_jaccards, alternative="greater"
    ) if cite_kw_jaccards and random_kw_jaccards else (0, 1)

    disc_stat, disc_pval = scipy_stats.mannwhitneyu(
        cite_disc_jaccards, random_disc_jaccards, alternative="greater"
    ) if cite_disc_jaccards and random_disc_jaccards else (0, 1)

    result = {
        "citation_pairs": len(in_corpus_edges),
        "random_pairs": n_random,
        "keyword_overlap": {
            "citing_mean": float(np.mean(cite_kw_jaccards)) if cite_kw_jaccards else 0,
            "citing_median": float(np.median(cite_kw_jaccards)) if cite_kw_jaccards else 0,
            "random_mean": float(np.mean(random_kw_jaccards)) if random_kw_jaccards else 0,
            "random_median": float(np.median(random_kw_jaccards)) if random_kw_jaccards else 0,
            "mann_whitney_U": float(kw_stat),
            "p_value": float(kw_pval),
            "effect_significant": bool(kw_pval < 0.05),
            "n_citing": len(cite_kw_jaccards),
            "n_random": len(random_kw_jaccards),
        },
        "discipline_overlap": {
            "citing_mean": float(np.mean(cite_disc_jaccards)) if cite_disc_jaccards else 0,
            "citing_median": float(np.median(cite_disc_jaccards)) if cite_disc_jaccards else 0,
            "random_mean": float(np.mean(random_disc_jaccards)) if random_disc_jaccards else 0,
            "random_median": float(np.median(random_disc_jaccards)) if random_disc_jaccards else 0,
            "mann_whitney_U": float(disc_stat),
            "p_value": float(disc_pval),
            "effect_significant": bool(disc_pval < 0.05),
            "n_citing": len(cite_disc_jaccards),
            "n_random": len(random_disc_jaccards),
        },
    }

    print(f"  Keyword overlap — citing: {result['keyword_overlap']['citing_mean']:.4f} vs "
          f"random: {result['keyword_overlap']['random_mean']:.4f}  "
          f"(p={kw_pval:.2e})", flush=True)
    print(f"  Discipline overlap — citing: {result['discipline_overlap']['citing_mean']:.4f} vs "
          f"random: {result['discipline_overlap']['random_mean']:.4f}  "
          f"(p={disc_pval:.2e})", flush=True)

    return result, cite_kw_jaccards, random_kw_jaccards, cite_disc_jaccards, random_disc_jaccards


def plot_citation_coherence(result, cite_kw, random_kw, cite_disc, random_disc, output_path):
    """Figure 5: Citation coherence — citing pairs vs random pairs."""
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Keyword overlap
    ax = axes[0]
    bins = np.linspace(0, max(0.5, max(max(cite_kw, default=0), max(random_kw, default=0))), 40)
    ax.hist(random_kw, bins=bins, alpha=0.5, label=f"Random (n={len(random_kw)})", color="gray", density=True)
    ax.hist(cite_kw, bins=bins, alpha=0.7, label=f"Citing (n={len(cite_kw)})", color="#E74C3C", density=True)
    ax.axvline(np.mean(cite_kw), color="#E74C3C", linestyle="--", linewidth=2)
    ax.axvline(np.mean(random_kw), color="gray", linestyle="--", linewidth=2)
    kw_data = result["keyword_overlap"]
    ax.set_title(f"Keyword Overlap (p={kw_data['p_value']:.2e})")
    ax.set_xlabel("Jaccard Similarity")
    ax.set_ylabel("Density")
    ax.legend()

    # Discipline overlap
    ax = axes[1]
    bins = np.linspace(0, 1.0, 30)
    ax.hist(random_disc, bins=bins, alpha=0.5, label=f"Random (n={len(random_disc)})", color="gray", density=True)
    ax.hist(cite_disc, bins=bins, alpha=0.7, label=f"Citing (n={len(cite_disc)})", color="#3498DB", density=True)
    ax.axvline(np.mean(cite_disc), color="#3498DB", linestyle="--", linewidth=2)
    ax.axvline(np.mean(random_disc), color="gray", linestyle="--", linewidth=2)
    disc_data = result["discipline_overlap"]
    ax.set_title(f"Discipline Overlap (p={disc_data['p_value']:.2e})")
    ax.set_xlabel("Jaccard Similarity")
    ax.set_ylabel("Density")
    ax.legend()

    plt.suptitle("Citation Coherence: Do Citing Papers Share Keyword Neighborhoods?", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved {output_path}", flush=True)


def main():
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found", file=sys.stderr)
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH), read_only=True)

    try:
        citation_count = conn.execute("SELECT COUNT(*) FROM openalex_citations").fetchone()[0]
        if citation_count == 0:
            print("No citations fetched yet. Run fetch_openalex_citations.py first.", flush=True)
            sys.exit(0)

        print(f"Citation edges in DB: {citation_count:,}", flush=True)

        print("\nBuilding paper keyword/discipline sets...", flush=True)
        paper_kw = get_paper_keyword_sets(conn)
        paper_discs = get_paper_sense_sets(conn)
        print(f"  {len(paper_kw)} papers with keywords, {len(paper_discs)} with discipline mappings", flush=True)

        print("\nComputing citation coherence...", flush=True)
        output = compute_citation_coherence(conn, paper_kw, paper_discs)

        if isinstance(output, dict) and output.get("status") == "no_data":
            print("No in-corpus citation pairs. Need more data.", flush=True)
            return

        result, cite_kw, random_kw, cite_disc, random_disc = output

        out_path = OUT_DIR / "citation_coherence.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\n  Saved {out_path}", flush=True)

        plot_citation_coherence(result, cite_kw, random_kw, cite_disc, random_disc,
                                FIG_DIR / "fig5_citation_coherence.png")

        print("\n=== Citation Coherence Summary ===", flush=True)
        kw = result["keyword_overlap"]
        disc = result["discipline_overlap"]
        print(f"  Keyword overlap:    citing {kw['citing_mean']:.4f} vs random {kw['random_mean']:.4f}  "
              f"({'SIGNIFICANT' if kw['effect_significant'] else 'NOT significant'})", flush=True)
        print(f"  Discipline overlap: citing {disc['citing_mean']:.4f} vs random {disc['random_mean']:.4f}  "
              f"({'SIGNIFICANT' if disc['effect_significant'] else 'NOT significant'})", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
