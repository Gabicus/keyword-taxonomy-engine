#!/bin/bash
# Run all pending scripts sequentially after citation fetch completes.
# Usage: bash scripts/run_all_pending.sh

set -e
cd /home/victor/Desktop/Projects/keywords

echo "=== Waiting for citation fetch to finish ==="
while pgrep -f "fetch_openalex_citations" > /dev/null 2>&1; do
    count=$(duckdb data/lake/keywords.duckdb -readonly -c "SELECT COUNT(*) FROM openalex_citations;" 2>/dev/null | grep -oP '\d+' | head -1)
    echo "  Citations so far: ${count:-?}... waiting 30s"
    sleep 30
done
echo "  Citation fetch done."

echo ""
echo "=== 1/4: DOI Backfill ==="
python3 -u scripts/backfill_dois.py

echo ""
echo "=== 2/4: Expanded Natlab Ingestion ==="
python3 -u scripts/ingest_expanded_natlab.py

echo ""
echo "=== 3/4: WoS Category Mappings ==="
python3 -u scripts/ingest_wos_categories.py

echo ""
echo "=== 4/4: Citation Coherence Analysis ==="
python3 -u scripts/citation_coherence.py

echo ""
echo "=== ALL DONE ==="
duckdb data/lake/keywords.duckdb -readonly -c "
SELECT 'keyword_senses' as tbl, COUNT(*) as cnt FROM keyword_senses
UNION ALL SELECT 'sense_relationships', COUNT(*) FROM sense_relationships
UNION ALL SELECT 'openalex_citations', COUNT(*) FROM openalex_citations
UNION ALL SELECT 'raw_wos_natlab_expanded', COUNT(*) FROM raw_wos_natlab_expanded
UNION ALL SELECT 'wos_category_mapping', COUNT(*) FROM wos_category_mapping;
"
