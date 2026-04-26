#!/usr/bin/env python3
"""Estimate keyword coverage completeness per discipline.

Queries our DuckDB for keyword_senses and source counts, then compares
against OpenAlex concept universe (~65K concepts, 19 level-0 domains,
284 level-1 fields) to estimate coverage percentage per discipline.

Outputs: data/viz/coverage_estimates.json
"""
import duckdb
import json
import shutil
import sys
import tempfile
import urllib.request
from pathlib import Path

DB_PATH = Path("data/lake/keywords.duckdb")
OUT_DIR = Path("data/viz")

MAILTO = "gabe.dewitt@gmail.com"
OA_BASE = "https://api.openalex.org"

# ============================================================
# OpenAlex level-0 domain → our discipline mapping
# Our 14 disciplines (from primer):
#   fossil_energy, earth_environmental, materials, chemistry,
#   biology_life, medicine_health, computation_data, engineering,
#   physics, mathematics, social_behavioral, economics_policy,
#   humanities_arts, general_science
# ============================================================

OA_DOMAIN_TO_DISCIPLINE = {
    # STEM core
    "Computer science": "computation_data",
    "Physics": "physics",
    "Mathematics": "mathematics",
    "Engineering": "engineering",
    "Materials science": "materials",
    "Chemistry": "chemistry",
    # Life / health
    "Medicine": "medicine_health",
    "Biology": "biology_life",
    "Psychology": "social_behavioral",
    # Earth / environment
    "Environmental science": "earth_environmental",
    "Geology": "earth_environmental",
    "Geography": "earth_environmental",
    # Social / humanities
    "Political science": "social_behavioral",
    "Sociology": "social_behavioral",
    "Economics": "economics_policy",
    "Business": "economics_policy",
    "Philosophy": "humanities_arts",
    "Art": "humanities_arts",
    "History": "humanities_arts",
}

# Level-1 concepts → discipline (best-effort for energy-relevant ones)
# We map the top ~100 level-1 fields that overlap with our scope
OA_LEVEL1_TO_DISCIPLINE = {
    # Energy-adjacent
    "Thermodynamics": "fossil_energy",
    "Chemical engineering": "fossil_energy",
    "Nuclear engineering": "engineering",
    "Mechanical engineering": "engineering",
    "Electrical engineering": "engineering",
    "Structural engineering": "engineering",
    "Metallurgy": "materials",
    "Composite material": "materials",
    "Nanotechnology": "materials",
    "Crystallography": "materials",
    "Optoelectronics": "materials",
    # Computation
    "Artificial intelligence": "computation_data",
    "Machine learning": "computation_data",
    "Data mining": "computation_data",
    "Computer vision": "computation_data",
    "Programming language": "computation_data",
    "Database": "computation_data",
    "Computer network": "computation_data",
    "Computer security": "computation_data",
    "Operating system": "computation_data",
    "Information retrieval": "computation_data",
    "World Wide Web": "computation_data",
    "Algorithm": "computation_data",
    "Statistics": "mathematics",
    "Geometry": "mathematics",
    "Mathematical analysis": "mathematics",
    "Combinatorics": "mathematics",
    "Pure mathematics": "mathematics",
    # Physics
    "Optics": "physics",
    "Quantum mechanics": "physics",
    "Atomic physics": "physics",
    "Nuclear physics": "physics",
    "Mechanics": "physics",
    "Acoustics": "physics",
    "Astronomy": "physics",
    # Chemistry
    "Organic chemistry": "chemistry",
    "Physical chemistry": "chemistry",
    "Biochemistry": "chemistry",
    "Chromatography": "chemistry",
    "Nuclear magnetic resonance": "chemistry",
    # Biology / life
    "Genetics": "biology_life",
    "Ecology": "biology_life",
    "Botany": "biology_life",
    "Zoology": "biology_life",
    "Molecular biology": "biology_life",
    "Cell biology": "biology_life",
    "Microbiology": "biology_life",
    "Computational biology": "biology_life",
    "Virology": "biology_life",
    "Agronomy": "biology_life",
    "Horticulture": "biology_life",
    "Food science": "biology_life",
    # Medicine / health
    "Internal medicine": "medicine_health",
    "Surgery": "medicine_health",
    "Pathology": "medicine_health",
    "Immunology": "medicine_health",
    "Endocrinology": "medicine_health",
    "Cardiology": "medicine_health",
    "Pharmacology": "medicine_health",
    "Radiology": "medicine_health",
    "Psychiatry": "medicine_health",
    "Neuroscience": "medicine_health",
    "Nursing": "medicine_health",
    "Gynecology": "medicine_health",
    "Cancer research": "medicine_health",
    "Intensive care medicine": "medicine_health",
    "Family medicine": "medicine_health",
    "Anatomy": "medicine_health",
    "Environmental health": "medicine_health",
    "Medical education": "medicine_health",
    # Earth / environmental
    "Remote sensing": "earth_environmental",
    "Physical geography": "earth_environmental",
    "Paleontology": "earth_environmental",
    "Oceanography": "earth_environmental",
    "Meteorology": "earth_environmental",
    "Cartography": "earth_environmental",
    "Earth science": "earth_environmental",
    "Environmental resource management": "earth_environmental",
    # Social / behavioral
    "Social psychology": "social_behavioral",
    "Linguistics": "social_behavioral",
    "Demography": "social_behavioral",
    "Social science": "social_behavioral",
    "Law": "social_behavioral",
    "Pedagogy": "social_behavioral",
    "Mathematics education": "social_behavioral",
    "Public administration": "social_behavioral",
    "Library science": "social_behavioral",
    # Economics / policy
    "Finance": "economics_policy",
    "Marketing": "economics_policy",
    "Management": "economics_policy",
    "Economic growth": "economics_policy",
    "Public relations": "economics_policy",
    # Humanities / arts
    "Humanities": "humanities_arts",
    "Archaeology": "humanities_arts",
    "Art history": "humanities_arts",
    "Visual arts": "humanities_arts",
    "Literature": "humanities_arts",
    "Ancient history": "humanities_arts",
    "Theology": "humanities_arts",
    "Epistemology": "humanities_arts",
    "Aesthetics": "humanities_arts",
    "Media studies": "humanities_arts",
}


def fetch_openalex_concepts(level: int, per_page: int = 200) -> list[dict]:
    """Fetch OpenAlex concepts at a given hierarchy level."""
    url = f"{OA_BASE}/concepts?filter=level:{level}&per_page={per_page}&mailto={MAILTO}"
    print(f"  Fetching OpenAlex level-{level} concepts from API...")
    req = urllib.request.Request(url, headers={"User-Agent": f"KeywordTaxonomy/1.0 (mailto:{MAILTO})"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    results = data.get("results", [])
    total = data.get("meta", {}).get("count", len(results))
    print(f"    Got {len(results)} of {total} level-{level} concepts")
    return results, total


def query_our_data(conn) -> dict:
    """Query our DuckDB for discipline counts and source counts."""
    print("  Querying keyword_senses by discipline...")
    disc_rows = conn.execute("""
        SELECT
            discipline_primary,
            COUNT(*) as sense_count,
            COUNT(DISTINCT keyword_label) as unique_labels
        FROM keyword_senses
        GROUP BY discipline_primary
        ORDER BY sense_count DESC
    """).fetchall()

    by_discipline = {}
    total_senses = 0
    total_labels = 0
    for row in disc_rows:
        disc, senses, labels = row[0], row[1], row[2]
        by_discipline[disc] = {"senses": senses, "unique_labels": labels}
        total_senses += senses
        total_labels += labels

    print(f"    {len(by_discipline)} disciplines, {total_senses:,} senses, {total_labels:,} labels")

    # Keywords per source pillar
    print("  Querying keywords by source...")
    src_rows = conn.execute("""
        SELECT source, COUNT(*) as cnt
        FROM keywords
        GROUP BY source
        ORDER BY cnt DESC
    """).fetchall()
    by_source = {row[0]: row[1] for row in src_rows}
    print(f"    {len(by_source)} sources")

    # Check for WoS category mapping table
    wos_cats = []
    try:
        cat_rows = conn.execute("""
            SELECT DISTINCT wos_category
            FROM wos_category_mapping
            ORDER BY wos_category
        """).fetchall()
        wos_cats = [r[0] for r in cat_rows]
        print(f"    {len(wos_cats)} WoS subject categories mapped")
    except Exception:
        # Table may not exist yet (queued in pipeline)
        print("    wos_category_mapping table not found (queued)")

    return {
        "by_discipline": by_discipline,
        "by_source": by_source,
        "wos_categories": wos_cats,
        "total_senses": total_senses,
        "total_labels": total_labels,
    }


def build_coverage_estimate(our_data: dict, level0: list[dict], level1: list[dict],
                            total_concepts: int) -> dict:
    """Build coverage estimate by mapping OpenAlex domains to our disciplines."""

    # Aggregate OpenAlex level-0 by our discipline
    oa_by_disc = {}
    level0_mapped = []
    for c in level0:
        name = c["display_name"]
        our_disc = OA_DOMAIN_TO_DISCIPLINE.get(name, "unmapped")
        level0_mapped.append({
            "name": name,
            "works_count": c["works_count"],
            "cited_by_count": c.get("cited_by_count", 0),
            "our_discipline": our_disc,
        })
        if our_disc != "unmapped":
            if our_disc not in oa_by_disc:
                oa_by_disc[our_disc] = {"works_count": 0, "oa_domains": []}
            oa_by_disc[our_disc]["works_count"] += c["works_count"]
            oa_by_disc[our_disc]["oa_domains"].append(name)

    # Aggregate level-1 by discipline
    level1_mapped = []
    oa_level1_by_disc = {}
    for c in level1:
        name = c["display_name"]
        our_disc = OA_LEVEL1_TO_DISCIPLINE.get(name)
        level1_mapped.append({
            "name": name,
            "works_count": c["works_count"],
            "cited_by_count": c.get("cited_by_count", 0),
            "our_discipline": our_disc or "unmapped",
        })
        if our_disc:
            if our_disc not in oa_level1_by_disc:
                oa_level1_by_disc[our_disc] = {"concept_count": 0, "works_count": 0}
            oa_level1_by_disc[our_disc]["concept_count"] += 1
            oa_level1_by_disc[our_disc]["works_count"] += c["works_count"]

    # Compute coverage percentages
    # Coverage = our_unique_labels / estimated_concepts_in_discipline
    # We estimate concepts per discipline proportionally from the 65K total
    coverage_pct = {}
    our_disc = our_data["by_discipline"]

    for disc in our_disc:
        our_labels = our_disc[disc]["unique_labels"]
        our_senses = our_disc[disc]["senses"]

        # Use level-1 concept count as proxy for discipline breadth
        oa_l1 = oa_level1_by_disc.get(disc, {})
        oa_l0 = oa_by_disc.get(disc, {})

        # Rough estimate: OpenAlex has ~65K concepts total across 19 L0 domains.
        # Each L0 domain has ~3,400 concepts on average.
        # Scale by works_count proportion for more accuracy.
        total_oa_works = sum(c["works_count"] for c in level0)
        disc_oa_works = oa_l0.get("works_count", 0)
        works_share = disc_oa_works / total_oa_works if total_oa_works > 0 else 0
        estimated_oa_concepts = max(1, int(total_concepts * works_share))

        # Coverage ratio (capped at 1.0 since we may have more labels from
        # multiple sources than OpenAlex has concepts)
        pct = min(1.0, our_labels / estimated_oa_concepts) if estimated_oa_concepts > 0 else 0.0

        coverage_pct[disc] = {
            "coverage_ratio": round(pct, 4),
            "our_labels": our_labels,
            "our_senses": our_senses,
            "estimated_oa_concepts": estimated_oa_concepts,
            "oa_works_share": round(works_share, 4),
            "oa_l1_concept_count": oa_l1.get("concept_count", 0),
        }

    return {
        "our_data": {
            "by_discipline": our_disc,
            "by_source": our_data["by_source"],
            "wos_categories": our_data["wos_categories"],
            "total_senses": our_data["total_senses"],
            "total_labels": our_data["total_labels"],
        },
        "openalex_universe": {
            "level0_domains": level0_mapped,
            "level1_concepts": level1_mapped,
            "total_concepts": total_concepts,
            "total_level0": len(level0),
            "total_level1": len(level1),
        },
        "coverage_pct": {
            "by_discipline": coverage_pct,
        },
    }


def print_summary(result: dict):
    """Print human-readable coverage summary."""
    print("\n" + "=" * 70)
    print("COVERAGE ESTIMATE SUMMARY")
    print("=" * 70)

    our = result["our_data"]
    oa = result["openalex_universe"]
    cov = result["coverage_pct"]["by_discipline"]

    print(f"\nOur data: {our['total_senses']:,} senses, {our['total_labels']:,} unique labels")
    print(f"OpenAlex universe: {oa['total_concepts']:,} concepts ({oa['total_level0']} L0, {oa['total_level1']} L1)")

    print(f"\n{'Discipline':<28} {'Our Labels':>10} {'Est OA':>10} {'Coverage':>10}")
    print("-" * 62)

    for disc in sorted(cov.keys(), key=lambda d: cov[d]["coverage_ratio"], reverse=True):
        c = cov[disc]
        bar = "#" * int(c["coverage_ratio"] * 20)
        print(f"{disc:<28} {c['our_labels']:>10,} {c['estimated_oa_concepts']:>10,} "
              f"{c['coverage_ratio']:>9.1%} {bar}")

    print(f"\n{'Source':<28} {'Keywords':>10}")
    print("-" * 40)
    for src, cnt in sorted(our["by_source"].items(), key=lambda x: -x[1]):
        print(f"{src:<28} {cnt:>10,}")

    if our["wos_categories"]:
        print(f"\nWoS subject categories mapped: {len(our['wos_categories'])}")


def main():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Open directly in read-only mode (no temp copy needed)
    print("Opening database in read-only mode...")
    tmp = None
    conn = duckdb.connect(str(DB_PATH), read_only=True)

    try:
        # 1. Query our data
        print("\n[1/3] Querying our database...")
        our_data = query_our_data(conn)

        # 2. Fetch OpenAlex concept counts
        print("\n[2/3] Fetching OpenAlex concept universe...")
        level0, _ = fetch_openalex_concepts(0, per_page=50)
        level1, _ = fetch_openalex_concepts(1, per_page=200)
        total_concepts = 65026  # From meta.count query

        # 3. Build coverage estimate
        print("\n[3/3] Computing coverage estimates...")
        result = build_coverage_estimate(our_data, level0, level1, total_concepts)

        # Save
        out_path = OUT_DIR / "coverage_estimates.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\nSaved to {out_path}")

        print_summary(result)

    finally:
        conn.close()
        # Clean up temp files if any
        if tmp:
            for p in [tmp, tmp + ".wal"]:
                try:
                    Path(p).unlink(missing_ok=True)
                except Exception:
                    pass


if __name__ == "__main__":
    main()
