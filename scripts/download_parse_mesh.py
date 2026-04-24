#!/usr/bin/env python3
"""Download and parse MeSH (Medical Subject Headings) XML descriptor file."""

import gzip
import json
import sys
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "mesh"
OUTPUT_JSON = DATA_DIR / "mesh_descriptors.json"

URL = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/desc2026.gz"

CATEGORY_MAP = {
    "A": "Anatomy",
    "B": "Organisms",
    "C": "Diseases",
    "D": "Chemicals and Drugs",
    "E": "Analytical, Diagnostic and Therapeutic Techniques, and Equipment",
    "F": "Psychiatry and Psychology",
    "G": "Phenomena and Processes",
    "H": "Disciplines and Occupations",
    "I": "Anthropology, Education, Sociology, and Social Phenomena",
    "J": "Technology, Industry, and Agriculture",
    "K": "Humanities",
    "L": "Information Science",
    "M": "Named Groups",
    "N": "Health Care",
    "V": "Publication Characteristics",
    "Z": "Geographicals",
}


def download_mesh() -> Path:
    """Download MeSH descriptor file (gzipped XML)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATA_DIR / "desc2026.gz"
    if dest.exists() and dest.stat().st_size > 1_000_000:
        print(f"Already downloaded: {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
        return dest

    # Remove any previous bad download
    dest.unlink(missing_ok=True)

    print(f"Downloading {URL} ...")
    urllib.request.urlretrieve(URL, dest)
    size = dest.stat().st_size
    print(f"Saved: {dest} ({size / 1e6:.1f} MB)")

    if size < 1_000_000:
        print("ERROR: File too small, likely not the real data.")
        sys.exit(1)

    return dest


def parse_mesh(gz_path: Path) -> list[dict]:
    """Parse MeSH XML descriptor file into list of records."""
    print("Decompressing and parsing XML (this may take a minute)...")

    with gzip.open(gz_path, "rb") as f:
        tree = ET.parse(f)

    root = tree.getroot()
    records = []

    for desc in root.iter("DescriptorRecord"):
        ui = ""
        heading = ""
        tree_numbers = []
        scope_note = ""
        entries = []

        ui_el = desc.find("DescriptorUI")
        if ui_el is not None and ui_el.text:
            ui = ui_el.text.strip()

        name_el = desc.find("DescriptorName/String")
        if name_el is not None and name_el.text:
            heading = name_el.text.strip()

        for tn_el in desc.iter("TreeNumber"):
            if tn_el.text:
                tree_numbers.append(tn_el.text.strip())

        # Scope note is in ConceptList > Concept > ScopeNote
        for concept in desc.iter("Concept"):
            pref = concept.get("PreferredConceptYN", "N")
            sn_el = concept.find("ScopeNote")
            if pref == "Y" and sn_el is not None and sn_el.text:
                scope_note = sn_el.text.strip()
                break

        # Synonyms from TermList entries (excluding the preferred heading itself)
        for term_el in desc.iter("Term"):
            string_el = term_el.find("String")
            if string_el is not None and string_el.text:
                t = string_el.text.strip()
                if t != heading:
                    entries.append(t)

        # Deduplicate entries preserving order
        seen = set()
        unique_entries = []
        for e in entries:
            if e not in seen:
                seen.add(e)
                unique_entries.append(e)

        # Category from tree numbers
        cats = set()
        for tn in tree_numbers:
            letter = tn[0] if tn else ""
            if letter in CATEGORY_MAP:
                cats.add(letter)

        cat_labels = [f"{c} ({CATEGORY_MAP[c]})" for c in sorted(cats)]
        if len(cat_labels) == 1:
            category = cat_labels[0]
        elif cat_labels:
            category = cat_labels
        else:
            category = "Unknown"

        records.append({
            "ui": ui,
            "heading": heading,
            "tree_numbers": tree_numbers,
            "scope_note": scope_note,
            "entries": unique_entries,
            "category": category,
        })

    return records


def print_summary(records: list[dict]) -> None:
    """Print breakdown by top-level category."""
    cat_counter: Counter[str] = Counter()
    for r in records:
        for tn in r["tree_numbers"]:
            letter = tn[0] if tn else "?"
            cat_counter[letter] += 1

    multi_cat = sum(1 for r in records if isinstance(r["category"], list))
    no_tree = sum(1 for r in records if not r["tree_numbers"])
    with_scope = sum(1 for r in records if r["scope_note"])
    total_synonyms = sum(len(r["entries"]) for r in records)

    print(f"\n{'='*72}")
    print(f"MeSH Descriptors Summary")
    print(f"{'='*72}")
    print(f"  Total descriptors:        {len(records):>6}")
    print(f"  With scope notes:         {with_scope:>6}")
    print(f"  Multi-category:           {multi_cat:>6}")
    print(f"  No tree number:           {no_tree:>6}")
    print(f"  Total synonyms/entries:   {total_synonyms:>6}")
    print(f"\nBreakdown by top-level MeSH category:")
    print(f"  {'Cat':<4} {'Name':<55} {'Count':>6}")
    print(f"  {'-'*67}")
    for letter in sorted(cat_counter):
        name = CATEGORY_MAP.get(letter, "Unknown")
        print(f"  {letter}    {name:<55} {cat_counter[letter]:>6}")
    print(f"\n  Total tree number assignments: {sum(cat_counter.values())}")


def main():
    mesh_file = download_mesh()
    records = parse_mesh(mesh_file)
    print(f"Parsed {len(records)} descriptors.")

    with open(OUTPUT_JSON, "w") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"Saved JSON: {OUTPUT_JSON} ({OUTPUT_JSON.stat().st_size / 1e6:.1f} MB)")

    print_summary(records)


if __name__ == "__main__":
    main()
