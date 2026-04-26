#!/bin/bash
# Download three open thesauri for keyword taxonomy engine
# Run: bash scripts/download_thesauri.sh
set -euo pipefail

BASE="data/raw"
cd "$(dirname "$0")/.."

# --- 1. EuroVoc (EU multilingual thesaurus, ~7K concepts) ---
echo "=== Downloading EuroVoc ==="
mkdir -p "$BASE/eurovoc"
# Try the EU Vocabularies CELLAR download handler (latest version 4.23)
# If this URL 404s, go to https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/eurovoc
# Click "Downloads" tab, find "eurovoc_skos_ap_eu.rdf" and update the cellarURI below.
wget -O "$BASE/eurovoc/eurovoc_skos_ap_eu.rdf" \
  "https://op.europa.eu/o/opportal-service/euvoc-download-handler?cellarURI=http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fcellar%2Fb3a3e26b-cb77-11ef-a562-01aa75ed71a1&fileName=eurovoc_skos_ap_eu.rdf" \
  2>&1 || {
    echo "EuroVoc primary URL failed, trying alternative..."
    # Alternative: try the CELLAR direct
    wget -O "$BASE/eurovoc/eurovoc_skos_ap_eu.rdf" \
      "https://op.europa.eu/o/opportal-service/euvoc-download-handler?cellarURI=http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fcellar%2F8971194a-0b10-11ee-b12e-01aa75ed71a1&fileName=eurovoc_skos_ap_eu.rdf" \
      2>&1 || echo "ERROR: EuroVoc download failed. Visit the EU Vocabularies site manually."
  }
ls -lh "$BASE/eurovoc/" 2>/dev/null
echo ""

# --- 2. STW Thesaurus for Economics (~6K concepts) ---
echo "=== Downloading STW ==="
mkdir -p "$BASE/stw"
wget -O "$BASE/stw/stw.nt.zip" \
  "https://zbw.eu/stw/version/latest/download/stw.nt.zip" 2>&1
ls -lh "$BASE/stw/"
echo ""

# --- 3. FAST (Faceted Application of Subject Terms, ~1.8M terms) ---
echo "=== Downloading FAST ==="
mkdir -p "$BASE/fast"
echo "WARNING: FAST is ~266MB compressed, ~1.8M terms. This may take a while."
wget -O "$BASE/fast/FASTAll.nt.zip" \
  "https://researchworks.oclc.org/researchdata/fast/FASTAll.nt.zip" 2>&1
ls -lh "$BASE/fast/"
echo ""

# --- Summary ---
echo "=== Download Summary ==="
echo "EuroVoc:"
ls -lh "$BASE/eurovoc/" 2>/dev/null || echo "  (no files)"
echo "STW:"
ls -lh "$BASE/stw/" 2>/dev/null || echo "  (no files)"
echo "FAST:"
ls -lh "$BASE/fast/" 2>/dev/null || echo "  (no files)"
echo ""
echo "Done. Raw files cached in $BASE/{eurovoc,stw,fast}/"
echo "Next: write parsers for each source."
