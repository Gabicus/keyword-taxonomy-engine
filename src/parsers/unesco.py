"""UNESCO Thesaurus SKOS/RDF parser.

Fetches the UNESCO Thesaurus RDF export, parses SKOS concepts with rdflib,
and normalizes into unified schema records.
"""

from pathlib import Path

from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, SKOS, DCTERMS

from ..config import SOURCES, RAW_DIR
from ..http_client import get_session

UNESCO_CFG = SOURCES["unesco"]
RDF_URL = UNESCO_CFG["url"]
CACHE_PATH = RAW_DIR / "unesco-thesaurus.rdf"


def _download_rdf(session=None) -> Path:
    """Download the UNESCO Thesaurus RDF file, caching locally."""
    if CACHE_PATH.exists():
        size_mb = CACHE_PATH.stat().st_size / (1024 * 1024)
        print(f"  UNESCO: Using cached RDF ({size_mb:.1f} MB)")
        return CACHE_PATH

    session = session or get_session(use_cache=False)
    print(f"  UNESCO: Downloading {RDF_URL} ...")
    resp = session.get(RDF_URL, timeout=120, stream=True)
    resp.raise_for_status()

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 64):
            f.write(chunk)

    size_mb = CACHE_PATH.stat().st_size / (1024 * 1024)
    print(f"  UNESCO: Downloaded ({size_mb:.1f} MB)")
    return CACHE_PATH


def _en_value(graph: Graph, subject: URIRef, predicate: URIRef) -> str | None:
    """Get the English literal value for a predicate, or first available."""
    for obj in graph.objects(subject, predicate):
        if isinstance(obj, Literal) and obj.language == "en":
            return str(obj)
    return None


def _en_values(graph: Graph, subject: URIRef, predicate: URIRef) -> list[str]:
    """Get all English literal values for a predicate."""
    results = []
    for obj in graph.objects(subject, predicate):
        if isinstance(obj, Literal) and obj.language == "en":
            results.append(str(obj))
    return results


def _build_broader_map(graph: Graph) -> dict[str, list[str]]:
    """Build a mapping from concept URI to list of broader concept URIs."""
    broader = {}
    for s, _, o in graph.triples((None, SKOS.broader, None)):
        s_uri = str(s)
        o_uri = str(o)
        broader.setdefault(s_uri, []).append(o_uri)
    return broader


def _walk_path(uri: str, broader_map: dict[str, list[str]],
               label_map: dict[str, str], seen: set | None = None) -> list[str]:
    """Walk the broader chain to build a full path (root first).

    Uses the first broader concept when multiple exist.
    Guards against cycles.
    """
    if seen is None:
        seen = set()
    if uri in seen:
        return []
    seen.add(uri)

    parents = broader_map.get(uri, [])
    if not parents:
        label = label_map.get(uri, uri)
        return [label]

    parent_uri = parents[0]
    ancestor_path = _walk_path(parent_uri, broader_map, label_map, seen)
    label = label_map.get(uri, uri)
    return ancestor_path + [label]


def parse_unesco(session=None) -> list[dict]:
    """Fetch and parse the UNESCO Thesaurus into unified schema records.

    Returns list of dicts matching the unified keyword schema.
    """
    rdf_path = _download_rdf(session)

    print("  UNESCO: Parsing RDF (this may take a minute) ...")
    g = Graph()
    g.parse(str(rdf_path), format="xml")
    print(f"  UNESCO: Loaded {len(g)} triples")

    # Collect all SKOS Concepts
    concepts = set()
    for s in g.subjects(RDF.type, SKOS.Concept):
        concepts.add(s)
    print(f"  UNESCO: Found {len(concepts)} concepts")

    # Build lookup maps
    broader_map = _build_broader_map(g)

    label_map: dict[str, str] = {}
    for uri in concepts:
        label = _en_value(g, uri, SKOS.prefLabel)
        if label:
            label_map[str(uri)] = label

    # Try to extract version from RDF metadata
    version = None
    for s, p, o in g.triples((None, DCTERMS.modified, None)):
        version = str(o)
        break
    if not version:
        for s, p, o in g.triples((None, DCTERMS.issued, None)):
            version = str(o)
            break

    # Build records
    records = []
    skipped = 0

    for concept_uri in concepts:
        uri_str = str(concept_uri)

        label = _en_value(g, concept_uri, SKOS.prefLabel)
        if not label:
            skipped += 1
            continue

        # Definition: try skos:definition first, then skos:scopeNote
        definition = _en_value(g, concept_uri, SKOS.definition)
        if not definition:
            definition = _en_value(g, concept_uri, SKOS.scopeNote)

        # Parent: first broader concept
        broaders = broader_map.get(uri_str, [])
        parent_id = broaders[0] if broaders else None

        # Aliases: English altLabels
        aliases = _en_values(g, concept_uri, SKOS.altLabel)

        # Cross-references: skos:related
        cross_refs = []
        for obj in g.objects(concept_uri, SKOS.related):
            cross_refs.append(str(obj))

        # Full path and level
        path_parts = _walk_path(uri_str, broader_map, label_map)
        full_path = " > ".join(path_parts) if path_parts else label
        level = len(path_parts) - 1 if path_parts else 0

        records.append({
            "id": uri_str,
            "label": label,
            "definition": definition,
            "parent_id": parent_id,
            "type": "thesaurus",
            "uri": uri_str,
            "full_path": full_path,
            "aliases": aliases,
            "level": max(level, 0),
            "cross_refs": cross_refs,
            "version": version,
        })

    if skipped:
        print(f"  UNESCO: Skipped {skipped} concepts without English labels")
    print(f"  UNESCO: {len(records)} keywords parsed")
    return records
