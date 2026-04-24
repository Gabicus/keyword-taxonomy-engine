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

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
