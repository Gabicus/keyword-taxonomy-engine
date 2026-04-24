"""Fetch publication-level keyword data from OpenAlex API for DOE/NETL/fossil energy works."""

import json
import csv
import time
import requests
from pathlib import Path

BASE_URL = "https://api.openalex.org/works"
MAILTO = "gabe.dewitt@gmail.com"
PER_PAGE = 200
MAX_WORKS = 2000  # 10 pages max per query
OUTPUT_DIR = Path("data/raw/openalex_pubs")

QUERIES = {
    "fossil_fuels_T10291": "topics.id:T10291",
    "carbon_capture_T10383": "topics.id:T10383",
    "coal_search": "default.search:coal combustion energy",
    "netl_ror": "institutions.ror:https://ror.org/02aqsxs83",
}


def fetch_works(name: str, filter_str: str) -> list[dict]:
    """Fetch up to MAX_WORKS using cursor pagination."""
    works = []
    cursor = "*"
    page = 0

    while cursor and len(works) < MAX_WORKS:
        params = {
            "filter": filter_str,
            "per_page": PER_PAGE,
            "cursor": cursor,
            "mailto": MAILTO,
        }
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            break

        works.extend(results)
        cursor = data.get("meta", {}).get("next_cursor")
        page += 1
        total = data.get("meta", {}).get("count", "?")
        print(f"  [{name}] page {page}: got {len(results)} works (total available: {total}, fetched so far: {len(works)})")

        # Polite rate limiting
        time.sleep(0.1)

    return works[:MAX_WORKS]


def extract_summary(work: dict) -> dict:
    """Extract key fields from a work record."""
    topics = work.get("topics") or []
    keywords = work.get("keywords") or []

    topic_ids = ";".join(t.get("id", "").split("/")[-1] for t in topics if t.get("id"))
    topic_labels = ";".join(t.get("display_name", "") for t in topics)
    keyword_labels = ";".join(k.get("keyword", k.get("display_name", "")) for k in keywords)

    return {
        "openalex_id": work.get("id", ""),
        "doi": work.get("doi", ""),
        "title": work.get("title", ""),
        "publication_year": work.get("publication_year", ""),
        "cited_by_count": work.get("cited_by_count", 0),
        "topic_ids": topic_ids,
        "topic_labels": topic_labels,
        "keyword_labels": keyword_labels,
        "type": work.get("type", ""),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_summaries = []
    seen_ids = set()

    for name, filter_str in QUERIES.items():
        print(f"\nFetching: {name} (filter={filter_str})")
        works = fetch_works(name, filter_str)
        print(f"  Total fetched: {len(works)}")

        # Save raw JSON
        json_path = OUTPUT_DIR / f"{name}.json"
        with open(json_path, "w") as f:
            json.dump(works, f)
        print(f"  Saved: {json_path} ({json_path.stat().st_size / 1024 / 1024:.1f} MB)")

        # Extract summaries, dedup by openalex_id
        for w in works:
            oa_id = w.get("id", "")
            if oa_id not in seen_ids:
                seen_ids.add(oa_id)
                all_summaries.append(extract_summary(w))

    # Write combined summary CSV
    csv_path = OUTPUT_DIR / "summary.csv"
    fieldnames = ["openalex_id", "doi", "title", "publication_year", "cited_by_count",
                  "topic_ids", "topic_labels", "keyword_labels", "type"]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_summaries)

    print(f"\n=== DONE ===")
    print(f"Total unique works: {len(all_summaries)}")
    print(f"Summary CSV: {csv_path}")


if __name__ == "__main__":
    main()
