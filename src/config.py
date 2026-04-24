from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
LAKE_DIR = DATA_DIR / "lake"
DB_PATH = LAKE_DIR / "keywords.duckdb"

SOURCES = {
    "nasa_gcmd": {
        "name": "NASA GCMD",
        "base_url": "https://gcmd.earthdata.nasa.gov/static/kms/",
        "keyword_types": [
            "sciencekeywords",
            "instruments",
            "platforms",
            "projects",
            "providers",
            "locations",
        ],
    },
    "unesco": {
        "name": "UNESCO Thesaurus",
        "url": "https://skos.um.es/unescothes/unescothes.rdf",
    },
    "ncbi": {
        "name": "NCBI Taxonomy",
        "ftp_url": "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz",
        "max_rank": "order",
    },
    "loc": {
        "name": "Library of Congress",
        "base_url": "https://id.loc.gov/authorities/subjects/",
        "root_ids": ["sh85118553", "sh85133067"],
    },
    "doe_osti": {
        "name": "DOE OSTI",
        "url": "https://www.osti.gov/api/v1/",
    },
    "openalex": {
        "name": "OpenAlex",
    },
}

NCBI_RANK_HIERARCHY = [
    "superkingdom", "kingdom", "phylum", "class", "order",
]

USER_AGENT = (
    "KeywordTaxonomyEngine/1.0 "
    "(Scientific Keyword Research; contact: gabe.dewitt@gmail.com)"
)
