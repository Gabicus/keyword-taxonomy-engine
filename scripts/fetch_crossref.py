#!/usr/bin/env python3
"""Fetch CrossRef subject/category data for energy and fossil fuel related works.

Note: CrossRef deprecated the `subject` field on works in late 2024.
We extract category signals from: container-title (journal/book), group-title,
type, and journal-level subject mappings where available.
"""

import json
import time
from collections import Counter
from pathlib import Path

import requests

BASE = "https://api.crossref.org"
MAILTO = "mailto=gabe.dewitt@gmail.com"
OUT = Path("data/raw/crossref")
OUT.mkdir(parents=True, exist_ok=True)

WORK_QUERIES = [
    "fossil+energy+carbon+capture",
    "coal+gasification",
    "natural+gas+methane",
    "petroleum+refining+hydrocarbon",
    "renewable+energy+wind+solar",
    "combustion+emissions+power+plant",
    "hydrogen+fuel+cell+energy",
    "geothermal+energy+subsurface",
]

JOURNAL_QUERIES = [
    "energy+fuels",
    "chemical+engineering",
    "petroleum",
    "combustion",
    "renewable+energy",
    "environmental+science",
    "geoscience+geology",
    "nuclear+energy",
]

# Don't use select= so we get all available fields including subject, group-title, container-title
FIELDS_TO_KEEP = [
    "DOI", "title", "subject", "type", "is-referenced-by-count",
    "container-title", "group-title", "published", "publisher",
    "short-container-title",
]


def fetch_json(url: str) -> dict:
    print(f"  GET {url[:120]}...")
    r = requests.get(url, timeout=60, headers={"User-Agent": f"KeywordTaxonomyEngine/1.0 ({MAILTO})"})
    r.raise_for_status()
    return r.json()


def slim_work(w: dict) -> dict:
    """Keep only the fields we care about to reduce file size."""
    out = {}
    for f in FIELDS_TO_KEEP:
        if f in w and w[f]:
            out[f] = w[f]
    # Flatten published to year
    pub = w.get("published") or w.get("published-print") or w.get("published-online")
    if pub and "date-parts" in pub and pub["date-parts"] and pub["date-parts"][0]:
        out["year"] = pub["date-parts"][0][0]
    return out


def main():
    # 1. Fetch works
    all_works = []
    seen_dois = set()
    for q in WORK_QUERIES:
        url = f"{BASE}/works?query={q}&rows=200&{MAILTO}"
        data = fetch_json(url)
        items = data.get("message", {}).get("items", [])
        new = 0
        for w in items:
            doi = w.get("DOI", "")
            if doi not in seen_dois:
                seen_dois.add(doi)
                all_works.append(slim_work(w))
                new += 1
        print(f"  -> {len(items)} results, {new} new for '{q}'")
        time.sleep(1)

    # 2. Fetch types
    url = f"{BASE}/types"
    types_data = fetch_json(url)
    time.sleep(1)

    # 3. Fetch journals
    all_journals = []
    seen_issns = set()
    for q in JOURNAL_QUERIES:
        url = f"{BASE}/journals?query={q}&rows=50&{MAILTO}"
        data = fetch_json(url)
        items = data.get("message", {}).get("items", [])
        new = 0
        for j in items:
            issns = tuple(j.get("ISSN", []))
            key = issns or j.get("title", "")
            if key not in seen_issns:
                seen_issns.add(key)
                all_journals.append(j)
                new += 1
        print(f"  -> {len(items)} results, {new} new for '{q}'")
        time.sleep(1)

    # 4. Save works_raw.json
    works_out = {
        "total_works": len(all_works),
        "queries": WORK_QUERIES,
        "types": types_data.get("message", {}).get("items", []),
        "items": all_works,
    }
    (OUT / "works_raw.json").write_text(json.dumps(works_out, indent=2))
    print(f"\nSaved works_raw.json ({len(all_works)} works)")

    # 5. Extract subjects from works (subject, group-title, container-title)
    subject_counter = Counter()
    container_counter = Counter()
    group_counter = Counter()
    type_counter = Counter()

    for w in all_works:
        for s in w.get("subject", []):
            subject_counter[s] += 1
        for c in w.get("container-title", []):
            container_counter[c] += 1
        gt = w.get("group-title")
        if gt:
            group_counter[gt if isinstance(gt, str) else str(gt)] += 1
        t = w.get("type", "")
        if t:
            type_counter[t] += 1

    subjects_out = {
        "note": "CrossRef deprecated work-level 'subject' field in late 2024. Container-title and group-title serve as category proxies.",
        "work_subjects": {
            "total_unique": len(subject_counter),
            "items": [{"subject": s, "count": c} for s, c in subject_counter.most_common()],
        },
        "container_titles": {
            "total_unique": len(container_counter),
            "items": [{"container": s, "count": c} for s, c in container_counter.most_common()],
        },
        "group_titles": {
            "total_unique": len(group_counter),
            "items": [{"group": s, "count": c} for s, c in group_counter.most_common()],
        },
        "work_types": {
            "total_unique": len(type_counter),
            "items": [{"type": s, "count": c} for s, c in type_counter.most_common()],
        },
    }
    (OUT / "subjects.json").write_text(json.dumps(subjects_out, indent=2))
    print(f"Saved subjects.json ({len(subject_counter)} work subjects, {len(container_counter)} containers, {len(group_counter)} groups)")

    # 6. Extract journal-level subjects
    journal_subjects_list = []
    journal_subject_counter = Counter()
    for j in all_journals:
        title = j.get("title", "")
        subjects = j.get("subjects", [])
        issn = j.get("ISSN", [])
        publisher = j.get("publisher", "")
        counts = j.get("counts", {})
        subj_names = [s.get("name", s) if isinstance(s, dict) else s for s in subjects]
        for s in subj_names:
            journal_subject_counter[s] += 1
        journal_subjects_list.append({
            "title": title,
            "issn": issn,
            "publisher": publisher,
            "total_dois": counts.get("total-dois", 0),
            "subjects": subj_names,
        })

    journal_out = {
        "total_journals": len(journal_subjects_list),
        "queries": JOURNAL_QUERIES,
        "unique_subjects": [
            {"subject": s, "count": c}
            for s, c in journal_subject_counter.most_common()
        ],
        "journals": journal_subjects_list,
    }
    (OUT / "journal_subjects.json").write_text(json.dumps(journal_out, indent=2))
    print(f"Saved journal_subjects.json ({len(journal_subjects_list)} journals, {len(journal_subject_counter)} unique subjects)")

    # 7. Summary
    print("\n" + "=" * 60)
    print("CROSSREF DATA PULL SUMMARY")
    print("=" * 60)
    print(f"Works fetched (deduplicated): {len(all_works)}")
    print(f"Unique work subjects:         {len(subject_counter)}")
    print(f"Unique container-titles:      {len(container_counter)}")
    print(f"Unique group-titles:          {len(group_counter)}")
    print(f"Work types:                   {len(type_counter)}")
    print(f"Journals fetched:             {len(journal_subjects_list)}")
    print(f"Unique journal subjects:      {len(journal_subject_counter)}")
    print(f"CrossRef types:               {len(works_out['types'])}")

    if subject_counter:
        print(f"\nTop 20 work subjects:")
        for s, c in subject_counter.most_common(20):
            print(f"  {c:4d}  {s}")

    print(f"\nTop 20 container-titles (journals/books):")
    for s, c in container_counter.most_common(20):
        print(f"  {c:4d}  {s}")

    if group_counter:
        print(f"\nGroup-titles:")
        for s, c in group_counter.most_common(20):
            print(f"  {c:4d}  {s}")

    print(f"\nWork types:")
    for s, c in type_counter.most_common():
        print(f"  {c:4d}  {s}")

    if journal_subject_counter:
        print(f"\nTop 20 journal subjects:")
        for s, c in journal_subject_counter.most_common(20):
            print(f"  {c:4d}  {s}")

    print(f"\nFiles saved:")
    for f in sorted(OUT.glob("*.json")):
        size = f.stat().st_size
        print(f"  {f}  ({size:,} bytes)")


if __name__ == "__main__":
    main()
