#!/usr/bin/env python3
"""Download three open thesauri for keyword taxonomy engine.

Usage: python3 scripts/download_thesauri.py

Downloads:
  1. EuroVoc (EU multilingual thesaurus, ~7K concepts) - SKOS/RDF
  2. STW Thesaurus for Economics (~6K concepts) - N-Triples
  3. FAST (Faceted Application of Subject Terms, ~1.8M terms) - N-Triples
"""
import os
import sys
import urllib.request
import ssl
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent / "data" / "raw"

DOWNLOADS = [
    {
        "name": "STW Thesaurus for Economics",
        "dir": "stw",
        "filename": "stw.nt.zip",
        "url": "https://zbw.eu/stw/version/latest/download/stw.nt.zip",
        "description": "~6,000 concepts, economics domain, SKOS N-Triples (zipped)",
    },
    {
        "name": "FAST (Faceted Application of Subject Terms)",
        "dir": "fast",
        "filename": "FASTAll.nt.zip",
        "url": "https://researchworks.oclc.org/researchdata/fast/FASTAll.nt.zip",
        "description": "~1.8M terms derived from LCSH, N-Triples (zipped, ~266MB)",
    },
]

# EuroVoc requires manual download due to EU portal redirect complexity.
# We'll try multiple URLs.
EUROVOC_URLS = [
    "https://op.europa.eu/o/opportal-service/euvoc-download-handler?cellarURI=http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fcellar%2Fb3a3e26b-cb77-11ef-a562-01aa75ed71a1&fileName=eurovoc_skos_ap_eu.rdf",
    "https://op.europa.eu/o/opportal-service/euvoc-download-handler?cellarURI=http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fcellar%2F8971194a-0b10-11ee-b12e-01aa75ed71a1&fileName=eurovoc_skos_ap_eu.rdf",
]


def download_file(url: str, dest: Path, description: str) -> bool:
    """Download a file with progress reporting."""
    print(f"  URL: {url}")
    print(f"  Dest: {dest}")

    # Create SSL context that doesn't verify (some academic servers have cert issues)
    ctx = ssl.create_default_context()

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (keyword-taxonomy-engine)"})

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
            total = resp.headers.get("Content-Length")
            total = int(total) if total else None

            with open(dest, "wb") as f:
                downloaded = 0
                while True:
                    chunk = resp.read(1024 * 256)  # 256KB chunks
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded * 100 / total
                        print(f"\r  Progress: {downloaded // 1024}KB / {total // 1024}KB ({pct:.0f}%)", end="", flush=True)
                    else:
                        print(f"\r  Downloaded: {downloaded // 1024}KB", end="", flush=True)

            print()
            size = dest.stat().st_size
            print(f"  OK: {size:,} bytes ({size / 1024 / 1024:.1f} MB)")
            return True

    except Exception as e:
        print(f"\n  FAILED: {e}")
        if dest.exists():
            dest.unlink()
        return False


def main():
    print("=" * 60)
    print("Downloading open thesauri for keyword taxonomy engine")
    print("=" * 60)

    results = {}

    # --- EuroVoc ---
    print(f"\n--- 1. EuroVoc (EU multilingual thesaurus, ~7K concepts) ---")
    eurovoc_dir = BASE / "eurovoc"
    eurovoc_dir.mkdir(parents=True, exist_ok=True)
    dest = eurovoc_dir / "eurovoc_skos_ap_eu.rdf"

    success = False
    for i, url in enumerate(EUROVOC_URLS):
        print(f"  Trying URL {i + 1}/{len(EUROVOC_URLS)}...")
        if download_file(url, dest, "EuroVoc SKOS/RDF"):
            # Verify it's actually RDF/XML (not an HTML error page)
            with open(dest, "rb") as f:
                header = f.read(500)
            if b"<rdf:" in header or b"<RDF" in header or b"<?xml" in header:
                success = True
                break
            else:
                print("  WARNING: Downloaded file doesn't look like RDF. Trying next URL...")
                dest.unlink()

    if not success:
        print("  ERROR: All EuroVoc URLs failed.")
        print("  MANUAL: Visit https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/eurovoc")
        print("  Click 'Downloads' tab, download 'eurovoc_skos_ap_eu.rdf'")
        print(f"  Save to: {dest}")
    results["EuroVoc"] = success

    # --- STW and FAST ---
    for i, dl in enumerate(DOWNLOADS, 2):
        print(f"\n--- {i}. {dl['name']} ---")
        print(f"  {dl['description']}")
        dl_dir = BASE / dl["dir"]
        dl_dir.mkdir(parents=True, exist_ok=True)
        dest = dl_dir / dl["filename"]
        results[dl["name"]] = download_file(dl["url"], dest, dl["description"])

    # --- Summary ---
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, ok in results.items():
        status = "OK" if ok else "FAILED"
        print(f"  {name}: {status}")

    # List all downloaded files
    print("\nFiles:")
    for subdir in ["eurovoc", "stw", "fast"]:
        d = BASE / subdir
        if d.exists():
            for f in sorted(d.iterdir()):
                print(f"  {f}  ({f.stat().st_size:,} bytes)")

    if not all(results.values()):
        print("\nSome downloads failed. See errors above.")
        sys.exit(1)
    else:
        print("\nAll downloads complete. Raw files cached in data/raw/{eurovoc,stw,fast}/")


if __name__ == "__main__":
    main()
