"""Harvest cross-taxonomy alignments from UNESCO Thesaurus SKOS data.

UNESCO provides skos:exactMatch, skos:closeMatch, skos:broadMatch, and
skos:narrowMatch links to external vocabularies. These are free,
authoritative cross-taxonomy alignments that we can load directly into
the cross_taxonomy_alignment table.

Known external vocabularies linked from UNESCO:
  - AGROVOC (FAO agricultural thesaurus)
  - EuroVoc (EU multilingual thesaurus)
  - Library of Congress Subject Headings
  - INIST (French CNRS thesaurus)
  - STW (German economics thesaurus)
  - TheSoz (German social science thesaurus)
"""

from pathlib import Path
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import SKOS

from ..config import RAW_DIR


CACHE_PATH = RAW_DIR / "unesco-thesaurus.rdf"

MATCH_PROPERTIES = [
    (SKOS.exactMatch, "exact"),
    (SKOS.closeMatch, "close"),
    (SKOS.broadMatch, "broad"),
    (SKOS.narrowMatch, "narrow"),
]

VOCAB_PATTERNS = {
    "agrovoc.fao.org": "AGROVOC",
    "aims.fao.org": "AGROVOC",
    "eurovoc.europa.eu": "EuroVoc",
    "id.loc.gov": "Library of Congress",
    "lod.nal.usda.gov": "NAL Thesaurus",
    "stw.zbw.eu": "STW Thesaurus",
    "lod.gesis.org": "TheSoz",
    "data.bnf.fr": "BnF (French National Library)",
    "www.yso.fi": "YSO (Finnish Thesaurus)",
}


def _identify_vocab(uri: str) -> str:
    """Identify which vocabulary a URI belongs to."""
    for pattern, name in VOCAB_PATTERNS.items():
        if pattern in uri:
            return name
    return f"Unknown ({uri.split('/')[2] if '/' in uri else 'unknown'})"


def _get_en_label(g: Graph, concept_uri: URIRef) -> str:
    """Get English prefLabel for a UNESCO concept."""
    for label in g.objects(concept_uri, SKOS.prefLabel):
        if hasattr(label, "language") and label.language == "en":
            return str(label)
    for label in g.objects(concept_uri, SKOS.prefLabel):
        return str(label)
    return str(concept_uri).rsplit("/", 1)[-1]


def harvest_unesco_matches(rdf_path: Path = None) -> list[dict]:
    """Extract all cross-vocabulary match links from UNESCO RDF.

    Returns list of alignment records ready for cross_taxonomy_alignment table.
    """
    rdf_path = rdf_path or CACHE_PATH
    if not rdf_path.exists():
        print(f"  UNESCO RDF not found at {rdf_path}")
        print("  Run UNESCO ingestion first to download the file.")
        return []

    print(f"  Loading UNESCO RDF from {rdf_path}...")
    g = Graph()
    g.parse(str(rdf_path), format="xml")
    print(f"  Loaded {len(g)} triples")

    alignments = []
    seen = set()

    for match_prop, match_type in MATCH_PROPERTIES:
        for subj, _, obj in g.triples((None, match_prop, None)):
            source_uri = str(subj)
            target_uri = str(obj)

            key = (source_uri, target_uri, match_type)
            if key in seen:
                continue
            seen.add(key)

            target_vocab = _identify_vocab(target_uri)
            source_label = _get_en_label(g, subj)

            alignments.append({
                "source_id": source_uri,
                "source_name": "UNESCO Thesaurus",
                "target_id": target_uri,
                "target_name": target_vocab,
                "match_type": match_type,
                "confidence": 1.0 if match_type == "exact" else 0.9,
                "method": "skos_match",
                "source_label": source_label,
                "target_label": target_uri.rsplit("/", 1)[-1],
                "reviewed": False,
                "review_note": None,
            })

    by_type = {}
    by_vocab = {}
    for a in alignments:
        by_type[a["match_type"]] = by_type.get(a["match_type"], 0) + 1
        by_vocab[a["target_name"]] = by_vocab.get(a["target_name"], 0) + 1

    print(f"\n  UNESCO cross-vocabulary matches: {len(alignments)} total")
    print(f"  By match type:")
    for t, c in sorted(by_type.items()):
        print(f"    {t}: {c}")
    print(f"  By target vocabulary:")
    for v, c in sorted(by_vocab.items(), key=lambda x: -x[1]):
        print(f"    {v}: {c}")

    return alignments
