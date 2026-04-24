"""Tests for UNESCO Thesaurus parser."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, SKOS, DCTERMS

from src.parsers.unesco import (
    parse_unesco,
    _en_value,
    _en_values,
    _build_broader_map,
    _walk_path,
)


def _make_sample_graph() -> Graph:
    """Build a small SKOS graph mimicking UNESCO Thesaurus structure."""
    g = Graph()
    NS = Namespace("http://vocabularies.unesco.org/thesaurus/")

    # Three-level hierarchy: Education > Higher education > Universities
    education = NS["concept1"]
    higher_ed = NS["concept2"]
    universities = NS["concept3"]
    # Unrelated concept with no English label
    unlabeled = NS["concept4"]
    # Concept with multiple broaders
    multi_parent = NS["concept5"]

    for concept in [education, higher_ed, universities, unlabeled, multi_parent]:
        g.add((concept, RDF.type, SKOS.Concept))

    # Labels
    g.add((education, SKOS.prefLabel, Literal("Education", lang="en")))
    g.add((education, SKOS.prefLabel, Literal("Educacion", lang="es")))
    g.add((higher_ed, SKOS.prefLabel, Literal("Higher education", lang="en")))
    g.add((universities, SKOS.prefLabel, Literal("Universities", lang="en")))
    g.add((unlabeled, SKOS.prefLabel, Literal("Sans etiquette", lang="fr")))
    g.add((multi_parent, SKOS.prefLabel, Literal("Research universities", lang="en")))

    # Hierarchy
    g.add((higher_ed, SKOS.broader, education))
    g.add((universities, SKOS.broader, higher_ed))
    g.add((multi_parent, SKOS.broader, universities))
    g.add((multi_parent, SKOS.broader, higher_ed))  # second broader

    # Definitions & scope notes
    g.add((education, SKOS.definition, Literal("The process of learning", lang="en")))
    g.add((higher_ed, SKOS.scopeNote, Literal("Post-secondary education", lang="en")))

    # Alt labels
    g.add((universities, SKOS.altLabel, Literal("Uni", lang="en")))
    g.add((universities, SKOS.altLabel, Literal("Universidades", lang="es")))

    # Related
    g.add((education, SKOS.related, higher_ed))

    # Version metadata
    g.add((URIRef("http://vocabularies.unesco.org/thesaurus"),
           DCTERMS.modified, Literal("2024-06-15")))

    return g


SAMPLE_GRAPH = _make_sample_graph()


class TestEnValue:
    def test_returns_english(self):
        uri = URIRef("http://vocabularies.unesco.org/thesaurus/concept1")
        assert _en_value(SAMPLE_GRAPH, uri, SKOS.prefLabel) == "Education"

    def test_returns_none_for_no_english(self):
        uri = URIRef("http://vocabularies.unesco.org/thesaurus/concept4")
        assert _en_value(SAMPLE_GRAPH, uri, SKOS.definition) is None

    def test_skips_non_english(self):
        uri = URIRef("http://vocabularies.unesco.org/thesaurus/concept4")
        # concept4 only has French label
        assert _en_value(SAMPLE_GRAPH, uri, SKOS.prefLabel) is None


class TestEnValues:
    def test_returns_all_english(self):
        uri = URIRef("http://vocabularies.unesco.org/thesaurus/concept3")
        vals = _en_values(SAMPLE_GRAPH, uri, SKOS.altLabel)
        assert "Uni" in vals
        assert "Universidades" not in vals

    def test_empty_when_none(self):
        uri = URIRef("http://vocabularies.unesco.org/thesaurus/concept1")
        assert _en_values(SAMPLE_GRAPH, uri, SKOS.altLabel) == []


class TestBroaderMap:
    def test_builds_map(self):
        bmap = _build_broader_map(SAMPLE_GRAPH)
        c2 = "http://vocabularies.unesco.org/thesaurus/concept2"
        c1 = "http://vocabularies.unesco.org/thesaurus/concept1"
        assert c1 in bmap[c2]

    def test_multiple_broaders(self):
        bmap = _build_broader_map(SAMPLE_GRAPH)
        c5 = "http://vocabularies.unesco.org/thesaurus/concept5"
        assert len(bmap[c5]) == 2


class TestWalkPath:
    def test_root_concept(self):
        bmap = _build_broader_map(SAMPLE_GRAPH)
        lmap = {
            "http://vocabularies.unesco.org/thesaurus/concept1": "Education",
            "http://vocabularies.unesco.org/thesaurus/concept2": "Higher education",
            "http://vocabularies.unesco.org/thesaurus/concept3": "Universities",
        }
        path = _walk_path("http://vocabularies.unesco.org/thesaurus/concept1", bmap, lmap)
        assert path == ["Education"]

    def test_nested_path(self):
        bmap = _build_broader_map(SAMPLE_GRAPH)
        lmap = {
            "http://vocabularies.unesco.org/thesaurus/concept1": "Education",
            "http://vocabularies.unesco.org/thesaurus/concept2": "Higher education",
            "http://vocabularies.unesco.org/thesaurus/concept3": "Universities",
        }
        path = _walk_path("http://vocabularies.unesco.org/thesaurus/concept3", bmap, lmap)
        assert path == ["Education", "Higher education", "Universities"]

    def test_handles_cycle(self):
        """Cycle guard: if A->B->A, should not infinite loop."""
        bmap = {"a": ["b"], "b": ["a"]}
        lmap = {"a": "A", "b": "B"}
        path = _walk_path("a", bmap, lmap)
        # Should return something finite, not hang
        assert len(path) <= 3


class TestParseUnesco:
    @patch("src.parsers.unesco._download_rdf")
    def test_full_parse(self, mock_download, tmp_path):
        """Test parse_unesco with sample RDF data written to a temp file."""
        rdf_file = tmp_path / "test.rdf"
        SAMPLE_GRAPH.serialize(str(rdf_file), format="xml")
        mock_download.return_value = rdf_file

        records = parse_unesco()

        # Should have 4 records (concept4 has no English label, skipped)
        labels = {r["label"] for r in records}
        assert "Education" in labels
        assert "Higher education" in labels
        assert "Universities" in labels
        assert "Research universities" in labels
        assert len(records) == 4

    @patch("src.parsers.unesco._download_rdf")
    def test_schema_fields(self, mock_download, tmp_path):
        rdf_file = tmp_path / "test.rdf"
        SAMPLE_GRAPH.serialize(str(rdf_file), format="xml")
        mock_download.return_value = rdf_file

        records = parse_unesco()
        by_label = {r["label"]: r for r in records}

        edu = by_label["Education"]
        assert edu["type"] == "thesaurus"
        assert edu["definition"] == "The process of learning"
        assert edu["parent_id"] is None
        assert edu["level"] == 0
        assert edu["uri"] == "http://vocabularies.unesco.org/thesaurus/concept1"
        assert edu["version"] == "2024-06-15"

    @patch("src.parsers.unesco._download_rdf")
    def test_hierarchy(self, mock_download, tmp_path):
        rdf_file = tmp_path / "test.rdf"
        SAMPLE_GRAPH.serialize(str(rdf_file), format="xml")
        mock_download.return_value = rdf_file

        records = parse_unesco()
        by_label = {r["label"]: r for r in records}

        higher = by_label["Higher education"]
        assert higher["parent_id"] == "http://vocabularies.unesco.org/thesaurus/concept1"
        assert higher["level"] == 1
        assert "Education > Higher education" == higher["full_path"]

    @patch("src.parsers.unesco._download_rdf")
    def test_scope_note_as_definition(self, mock_download, tmp_path):
        rdf_file = tmp_path / "test.rdf"
        SAMPLE_GRAPH.serialize(str(rdf_file), format="xml")
        mock_download.return_value = rdf_file

        records = parse_unesco()
        by_label = {r["label"]: r for r in records}
        assert by_label["Higher education"]["definition"] == "Post-secondary education"

    @patch("src.parsers.unesco._download_rdf")
    def test_aliases(self, mock_download, tmp_path):
        rdf_file = tmp_path / "test.rdf"
        SAMPLE_GRAPH.serialize(str(rdf_file), format="xml")
        mock_download.return_value = rdf_file

        records = parse_unesco()
        by_label = {r["label"]: r for r in records}
        assert "Uni" in by_label["Universities"]["aliases"]

    @patch("src.parsers.unesco._download_rdf")
    def test_cross_refs(self, mock_download, tmp_path):
        rdf_file = tmp_path / "test.rdf"
        SAMPLE_GRAPH.serialize(str(rdf_file), format="xml")
        mock_download.return_value = rdf_file

        records = parse_unesco()
        by_label = {r["label"]: r for r in records}
        assert len(by_label["Education"]["cross_refs"]) == 1

    @patch("src.parsers.unesco._download_rdf")
    def test_no_empty_labels(self, mock_download, tmp_path):
        rdf_file = tmp_path / "test.rdf"
        SAMPLE_GRAPH.serialize(str(rdf_file), format="xml")
        mock_download.return_value = rdf_file

        records = parse_unesco()
        assert all(r["label"] for r in records)

    @patch("src.parsers.unesco._download_rdf")
    def test_multi_broader_uses_first(self, mock_download, tmp_path):
        rdf_file = tmp_path / "test.rdf"
        SAMPLE_GRAPH.serialize(str(rdf_file), format="xml")
        mock_download.return_value = rdf_file

        records = parse_unesco()
        by_label = {r["label"]: r for r in records}
        ru = by_label["Research universities"]
        # Should have a parent_id (one of the two broaders)
        assert ru["parent_id"] is not None
        assert ru["level"] >= 1
