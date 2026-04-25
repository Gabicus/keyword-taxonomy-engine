"""CLI driver for keyword data lake operations."""

import argparse
import sys

from .storage import KeywordStore
from .parsers.nasa_gcmd import parse_nasa_gcmd
from .parsers.unesco import parse_unesco
from .parsers.openalex import parse_openalex
from .parsers.ncbi import parse_ncbi
from .parsers.loc import parse_loc
from .parsers.doe_osti import parse_doe_osti
from .enrichment.gcmd_enricher import enrich_gcmd
from .raw_writers import (
    build_raw_nasa_gcmd, build_raw_unesco, build_raw_openalex,
    build_raw_ncbi, build_raw_loc, build_raw_doe_osti,
)

PARSERS = {
    "nasa_gcmd": ("NASA GCMD", parse_nasa_gcmd),
    "unesco": ("UNESCO Thesaurus", parse_unesco),
    "openalex": ("OpenAlex", parse_openalex),
    "ncbi": ("NCBI Taxonomy", parse_ncbi),
    "loc": ("Library of Congress", parse_loc),
    "doe_osti": ("DOE OSTI", parse_doe_osti),
}

ALL_SOURCES = list(PARSERS.keys())

RAW_BUILDERS = {
    "nasa_gcmd": ("raw_nasa_gcmd", "uuid", build_raw_nasa_gcmd),
    "unesco": ("raw_unesco", "concept_uri", build_raw_unesco),
    "openalex": ("raw_openalex", "openalex_id", build_raw_openalex),
    "ncbi": ("raw_ncbi", "tax_id", build_raw_ncbi),
    "loc": ("raw_loc", "loc_id", build_raw_loc),
    "doe_osti": ("raw_doe_osti", "category_code", build_raw_doe_osti),
}


def _ingest_source(store, key):
    source_name, parser_fn = PARSERS[key]
    print(f"\n=== Ingesting: {source_name} ===")
    try:
        records = parser_fn()
        result = store.upsert(records, source_name)
        print(f"  Inserted: {result['inserted']} (was {result['previous']}, delta {result['delta']})")
        validation = store.validate(source_name)
        print(f"  Validation: {'PASS' if validation['valid'] else 'FAIL'}")
        if validation["issues"]:
            for issue in validation["issues"]:
                print(f"    - {issue}")
        return result
    except Exception as e:
        print(f"  FAILED: {e}")
        return None


def _ingest_raw(store, key):
    if key not in RAW_BUILDERS:
        return None
    table, key_col, builder_fn = RAW_BUILDERS[key]
    print(f"\n--- Populating {table} ---")
    try:
        records = builder_fn()
        result = store.upsert_raw(table, records, key_col)
        print(f"  Inserted: {result['inserted']} (was {result['previous']})")
        return result
    except Exception as e:
        print(f"  FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def cmd_ingest(args):
    sources = args.sources or ALL_SOURCES
    with KeywordStore() as store:
        for source in sources:
            if source not in PARSERS:
                print(f"\n  Unknown source: {source}")
                print(f"  Available: {', '.join(ALL_SOURCES)}")
                continue
            _ingest_source(store, source)

        if args.raw:
            print("\n=== Populating Raw Tables ===")
            for source in sources:
                if source in RAW_BUILDERS:
                    _ingest_raw(store, source)

        print(f"\n=== Data Lake Stats ===")
        stats = store.stats()
        print(f"  Total keywords: {stats['total']}")
        for src, cnt in stats["by_source"].items():
            print(f"  {src}: {cnt}")


def cmd_populate_raw(args):
    sources = args.sources or ALL_SOURCES
    with KeywordStore() as store:
        for source in sources:
            if source in RAW_BUILDERS:
                _ingest_raw(store, source)


def cmd_export(args):
    with KeywordStore() as store:
        source = args.source
        path = store.export_parquet(source)
        print(f"Exported to: {path}")


def cmd_stats(args):
    with KeywordStore() as store:
        stats = store.stats()
        print(f"Total keywords: {stats['total']}")
        for src, cnt in stats["by_source"].items():
            print(f"  {src}: {cnt}")


def cmd_enrich(args):
    with KeywordStore() as store:
        if args.source == "nasa_gcmd":
            enrich_gcmd(store=store, max_concepts=args.max)
        else:
            print(f"Enrichment not implemented for: {args.source}")


def cmd_search(args):
    with KeywordStore() as store:
        results = store.search(args.query, args.source)
        print(f"Found {len(results)} results for '{args.query}':")
        for r in results[:20]:
            print(f"  [{r['source']}] {r['full_path'] or r['label']}")
            if r.get("definition"):
                print(f"    def: {r['definition'][:100]}...")
            if r.get("aliases"):
                print(f"    aliases: {r['aliases']}")


def cmd_lens(args):
    """Query through a lens — the core product feature."""
    import duckdb
    from .ontology import compose_lens, query_through_lens

    conn = duckdb.connect("data/lake/keywords.duckdb", read_only=True)

    lens = compose_lens(conn, args.discipline, args.role,
                        interests=args.interest, org_node=args.org)

    results = query_through_lens(conn, lens, search=args.search,
                                 limit=args.limit, min_score=args.min_score,
                                 source_filter=args.source)

    print(f"\n{'=' * 70}")
    print(f"  LENS: {lens.get('name', lens['lens_id'])}")
    print(f"  Role: {lens.get('role_type', 'researcher')}  |  "
          f"Discipline: {lens.get('discipline_primary')}  |  "
          f"Altitude: {lens.get('altitude', 1000):,}")
    if args.search:
        print(f"  Search: \"{args.search}\"")
    print(f"{'=' * 70}")
    print(f"  {'Score':>6}  {'Conf':>5}  {'Rels':>4}  {'Source':<20}  Label")
    print(f"  {'─' * 6}  {'─' * 5}  {'─' * 4}  {'─' * 20}  {'─' * 30}")

    for r in results:
        bridge = " ⟷" if r["is_bridge"] else ""
        label = r["label"]
        if r["disambiguation"]:
            label += f" ({r['disambiguation']})"
        print(f"  {r['score']:6.3f}  {r['confidence'] or 0:5.2f}  "
              f"{r['rel_count']:4d}  {r['source']:<20}  {label}{bridge}")

    print(f"\n  {len(results)} results shown")
    if any(r["is_bridge"] for r in results):
        print(f"  ⟷ = cross-domain bridge")
    conn.close()


def cmd_lens_explore(args):
    """Explore relationships from a keyword through a lens."""
    import duckdb
    from .ontology import compose_lens, explore_from_keyword

    conn = duckdb.connect("data/lake/keywords.duckdb", read_only=True)

    lens = compose_lens(conn, args.discipline, args.role)
    result = explore_from_keyword(conn, args.keyword, lens, limit=args.limit)

    print(f"\n{'=' * 70}")
    print(f"  EXPLORE: \"{result['keyword']}\"")
    print(f"  Through: {lens.get('name', lens['lens_id'])}")
    print(f"{'=' * 70}")

    if not result["senses"]:
        print(f"  No senses found for \"{args.keyword}\"")
        conn.close()
        return

    print(f"\n  Senses ({len(result['senses'])}):")
    for s in result["senses"]:
        disambig = f" ({s['disambiguation']})" if s.get("disambiguation") else ""
        print(f"    [{s['source']}] {s['discipline']} "
              f"(conf={s['confidence'] or 0:.2f}, lens_wt={s['lens_weight']:.2f}){disambig}")

    print(f"\n  Connected keywords ({len(result['neighbors'])} of {result['total_neighbors']}):")
    print(f"  {'Score':>6}  {'Conf':>5}  {'Rel':<18}  {'Discipline':<22}  Label")
    print(f"  {'─' * 6}  {'─' * 5}  {'─' * 18}  {'─' * 22}  {'─' * 25}")

    for n in result["neighbors"]:
        print(f"  {n['lens_score']:6.3f}  {n['confidence'] or 0:5.2f}  "
              f"{n['relationship']:<18}  {n['discipline']:<22}  {n['label']}")

    conn.close()


def cmd_lens_compare(args):
    """Compare how a keyword appears through different lenses."""
    import duckdb
    from .ontology import compare_lenses

    conn = duckdb.connect("data/lake/keywords.duckdb", read_only=True)
    result = compare_lenses(conn, args.keyword, args.lenses)

    print(f"\n{'=' * 70}")
    print(f"  COMPARE: \"{result['keyword']}\" across {len(args.lenses)} lenses")
    print(f"{'=' * 70}")

    for lid, data in result["perspectives"].items():
        print(f"\n  ┌─ {data['lens_name']}")
        print(f"  │  Role: {data['role']}  |  Primary: {data['primary_discipline']}")
        print(f"  │  Senses: {data['senses_found']}  |  "
              f"Connections: {data['total_connections']}")

        if data["discipline_spread"]:
            spread = ", ".join(f"{d}: {c}" for d, c in data["discipline_spread"].items())
            print(f"  │  Neighbor disciplines: {spread}")

        if data["top_neighbors"]:
            print(f"  │  Top neighbors:")
            for n in data["top_neighbors"][:5]:
                print(f"  │    {n['lens_score']:.3f}  {n['label']} "
                      f"({n['relationship']}, {n['discipline']})")
        print(f"  └─")

    conn.close()


def cmd_lens_list(args):
    """List available template lenses."""
    import duckdb
    from .ontology import list_lenses

    conn = duckdb.connect("data/lake/keywords.duckdb", read_only=True)
    lenses = list_lenses(conn, role=args.role, discipline=args.discipline)

    print(f"\n  Available lenses ({len(lenses)}):")
    print(f"  {'Lens ID':<35}  {'Role':<12}  {'Discipline':<22}  {'Alt':>7}")
    print(f"  {'─' * 35}  {'─' * 12}  {'─' * 22}  {'─' * 7}")

    for l in lenses:
        print(f"  {l['lens_id']:<35}  {l['role']:<12}  "
              f"{l['discipline']:<22}  {l['altitude']:>7,}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Keyword Taxonomy Engine")
    sub = parser.add_subparsers(dest="command")

    p_ingest = sub.add_parser("ingest", help="Ingest keyword sources into data lake")
    p_ingest.add_argument("--sources", nargs="+", default=None,
                          help=f"Sources to ingest (default: all). Options: {', '.join(ALL_SOURCES)}")
    p_ingest.add_argument("--raw", action="store_true", help="Also populate raw source tables")
    p_ingest.set_defaults(func=cmd_ingest)

    p_raw = sub.add_parser("populate-raw", help="Populate raw source tables from cached data")
    p_raw.add_argument("--sources", nargs="+", default=None,
                       help=f"Sources to populate (default: all). Options: {', '.join(ALL_SOURCES)}")
    p_raw.set_defaults(func=cmd_populate_raw)

    p_export = sub.add_parser("export", help="Export data lake to Parquet")
    p_export.add_argument("--source", default=None, help="Export specific source")
    p_export.set_defaults(func=cmd_export)

    p_stats = sub.add_parser("stats", help="Show data lake statistics")
    p_stats.set_defaults(func=cmd_stats)

    p_enrich = sub.add_parser("enrich", help="Enrich keywords with API metadata")
    p_enrich.add_argument("--source", default="nasa_gcmd", help="Source to enrich")
    p_enrich.add_argument("--max", type=int, default=None, help="Max concepts to fetch")
    p_enrich.set_defaults(func=cmd_enrich)

    p_search = sub.add_parser("search", help="Search keywords")
    p_search.add_argument("query", help="Search pattern")
    p_search.add_argument("--source", default=None, help="Filter by source")
    p_search.set_defaults(func=cmd_search)

    # === Lens Query Commands ===
    p_lens = sub.add_parser("lens", help="Query through a composed lens")
    p_lens.add_argument("discipline", help="Primary discipline (e.g., fossil_energy, materials)")
    p_lens.add_argument("--role", default="researcher",
                        choices=["director", "program_mgr", "researcher"],
                        help="Role perspective (default: researcher)")
    p_lens.add_argument("--search", default=None, help="Filter by keyword pattern")
    p_lens.add_argument("--interest", nargs="+", default=None, help="Interest weights")
    p_lens.add_argument("--org", default=None, help="Org envelope node ID")
    p_lens.add_argument("--source", default=None, help="Filter by origin source")
    p_lens.add_argument("--limit", type=int, default=50, help="Max results")
    p_lens.add_argument("--min-score", type=float, default=0.0, help="Minimum lens score")
    p_lens.set_defaults(func=cmd_lens)

    p_explore = sub.add_parser("lens-explore", help="Explore keyword relationships through a lens")
    p_explore.add_argument("keyword", help="Keyword to explore")
    p_explore.add_argument("--discipline", default="fossil_energy", help="Lens discipline")
    p_explore.add_argument("--role", default="researcher", help="Role perspective")
    p_explore.add_argument("--limit", type=int, default=30, help="Max neighbors")
    p_explore.set_defaults(func=cmd_lens_explore)

    p_compare = sub.add_parser("lens-compare", help="Compare keyword across lenses")
    p_compare.add_argument("keyword", help="Keyword to compare")
    p_compare.add_argument("--lenses", nargs="+", required=True,
                           help="Lens IDs to compare (e.g., hat:fossil_energy:researcher hat:materials:director)")
    p_compare.set_defaults(func=cmd_lens_compare)

    p_lenslist = sub.add_parser("lens-list", help="List available template lenses")
    p_lenslist.add_argument("--role", default=None, help="Filter by role")
    p_lenslist.add_argument("--discipline", default=None, help="Filter by discipline")
    p_lenslist.set_defaults(func=cmd_lens_list)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
