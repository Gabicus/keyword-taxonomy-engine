"""Tests for Library of Congress Subject Headings bulk parser."""

import gzip
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.parsers.loc import (
    _extract_id,
    _parse_nt_line,
    _stream_parse,
    _bfs_subtree,
    _build_paths,
    _compute_levels,
    parse_loc,
)


class TestExtractId:
    def test_full_uri(self):
        assert _extract_id("http://id.loc.gov/authorities/subjects/sh85118553") == "sh85118553"

    def test_non_lcsh_uri(self):
        assert _extract_id("http://example.com/other/sh85118553") is None

    def test_empty(self):
        assert _extract_id("") is None


class TestParseNtLine:
    def test_uri_object(self):
        line = '<http://id.loc.gov/authorities/subjects/sh85009003> <http://www.w3.org/2004/02/skos/core#broader> <http://id.loc.gov/authorities/subjects/sh85118553> .\n'
        result = _parse_nt_line(line)
        assert result is not None
        subj, pred, obj, lang = result
        assert "sh85009003" in subj
        assert pred.endswith("broader")
        assert "sh85118553" in obj
        assert lang is None

    def test_literal_with_lang(self):
        line = '<http://id.loc.gov/authorities/subjects/sh85118553> <http://www.w3.org/2004/02/skos/core#prefLabel> "Science"@en .\n'
        result = _parse_nt_line(line)
        assert result is not None
        _, _, obj, lang = result
        assert obj == "Science"
        assert lang == "en"

    def test_literal_without_lang(self):
        line = '<http://id.loc.gov/authorities/subjects/sh85118553> <http://www.w3.org/2004/02/skos/core#notation> "QC" .\n'
        result = _parse_nt_line(line)
        assert result is not None
        _, _, obj, lang = result
        assert obj == "QC"

    def test_escaped_quotes(self):
        line = '<http://id.loc.gov/authorities/subjects/sh1234> <http://www.w3.org/2004/02/skos/core#note> "He said \\"hello\\""@en .\n'
        result = _parse_nt_line(line)
        assert result is not None
        _, _, obj, _ = result
        assert obj == 'He said "hello"'

    def test_comment_line(self):
        assert _parse_nt_line("# comment\n") is None

    def test_empty_line(self):
        assert _parse_nt_line("\n") is None


SAMPLE_NT = """\
<http://id.loc.gov/authorities/subjects/sh85118553> <http://www.w3.org/2004/02/skos/core#prefLabel> "Science"@en .
<http://id.loc.gov/authorities/subjects/sh85118553> <http://www.w3.org/2004/02/skos/core#altLabel> "Natural science"@en .
<http://id.loc.gov/authorities/subjects/sh85118553> <http://www.w3.org/2004/02/skos/core#note> "General works on science"@en .
<http://id.loc.gov/authorities/subjects/sh85009003> <http://www.w3.org/2004/02/skos/core#prefLabel> "Astronomy"@en .
<http://id.loc.gov/authorities/subjects/sh85009003> <http://www.w3.org/2004/02/skos/core#broader> <http://id.loc.gov/authorities/subjects/sh85118553> .
<http://id.loc.gov/authorities/subjects/sh85118553> <http://www.w3.org/2004/02/skos/core#narrower> <http://id.loc.gov/authorities/subjects/sh85009003> .
<http://id.loc.gov/authorities/subjects/sh85014203> <http://www.w3.org/2004/02/skos/core#prefLabel> "Biology"@en .
<http://id.loc.gov/authorities/subjects/sh85014203> <http://www.w3.org/2004/02/skos/core#broader> <http://id.loc.gov/authorities/subjects/sh85118553> .
<http://id.loc.gov/authorities/subjects/sh85133067> <http://www.w3.org/2004/02/skos/core#prefLabel> "Technology"@en .
<http://id.loc.gov/authorities/subjects/sh85043000> <http://www.w3.org/2004/02/skos/core#prefLabel> "Engineering"@en .
<http://id.loc.gov/authorities/subjects/sh85043000> <http://www.w3.org/2004/02/skos/core#broader> <http://id.loc.gov/authorities/subjects/sh85133067> .
<http://id.loc.gov/authorities/subjects/sh85043000> <http://www.w3.org/2004/02/skos/core#definition> "The application of science"@en .
<http://id.loc.gov/authorities/subjects/sh85009003> <http://www.w3.org/2004/02/skos/core#related> <http://id.loc.gov/authorities/subjects/sh85014203> .
<http://id.loc.gov/authorities/subjects/sh99999999> <http://www.w3.org/2004/02/skos/core#prefLabel> "Unrelated topic"@en .
"""


@pytest.fixture
def sample_gz(tmp_path):
    """Create a gzipped N-Triples file from sample data."""
    gz_path = tmp_path / "test-lcsh.nt.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_NT)
    return gz_path


class TestStreamParse:
    def test_extracts_labels(self, sample_gz):
        data = _stream_parse(sample_gz)
        assert data["labels"]["sh85118553"] == "Science"
        assert data["labels"]["sh85009003"] == "Astronomy"
        assert data["labels"]["sh85133067"] == "Technology"

    def test_extracts_alt_labels(self, sample_gz):
        data = _stream_parse(sample_gz)
        assert "Natural science" in data["alt_labels"].get("sh85118553", [])

    def test_extracts_broader(self, sample_gz):
        data = _stream_parse(sample_gz)
        assert "sh85118553" in data["broader"]["sh85009003"]

    def test_extracts_narrower(self, sample_gz):
        data = _stream_parse(sample_gz)
        assert "sh85009003" in data["narrower"]["sh85118553"]

    def test_extracts_related(self, sample_gz):
        data = _stream_parse(sample_gz)
        assert "sh85014203" in data["related"]["sh85009003"]

    def test_extracts_notes(self, sample_gz):
        data = _stream_parse(sample_gz)
        assert data["notes"]["sh85118553"] == "General works on science"

    def test_extracts_definitions(self, sample_gz):
        data = _stream_parse(sample_gz)
        assert data["definitions"]["sh85043000"] == "The application of science"


class TestBfsSubtree:
    def test_finds_descendants(self):
        narrower = {"root": ["child1", "child2"], "child1": ["grandchild"]}
        broader = {"child1": ["root"], "child2": ["root"], "grandchild": ["child1"]}
        result = _bfs_subtree(["root"], narrower, broader)
        assert result == {"root", "child1", "child2", "grandchild"}

    def test_multiple_roots(self):
        narrower = {"r1": ["c1"], "r2": ["c2"]}
        broader = {"c1": ["r1"], "c2": ["r2"]}
        result = _bfs_subtree(["r1", "r2"], narrower, broader)
        assert result == {"r1", "r2", "c1", "c2"}

    def test_empty(self):
        result = _bfs_subtree(["root"], {}, {})
        assert result == {"root"}

    def test_uses_reverse_broader(self):
        narrower = {}
        broader = {"child": ["root"]}
        result = _bfs_subtree(["root"], narrower, broader)
        assert result == {"root", "child"}


class TestBuildPaths:
    def test_root_path(self):
        paths = _build_paths(
            {"r1"}, broader={}, labels={"r1": "Root"}, root_ids={"r1"}
        )
        assert paths["r1"] == "Root"

    def test_child_path(self):
        paths = _build_paths(
            {"r1", "c1"},
            broader={"c1": ["r1"]},
            labels={"r1": "Root", "c1": "Child"},
            root_ids={"r1"},
        )
        assert paths["c1"] == "Root > Child"

    def test_grandchild_path(self):
        paths = _build_paths(
            {"r1", "c1", "gc1"},
            broader={"c1": ["r1"], "gc1": ["c1"]},
            labels={"r1": "R", "c1": "C", "gc1": "GC"},
            root_ids={"r1"},
        )
        assert paths["gc1"] == "R > C > GC"


class TestComputeLevels:
    def test_root_is_zero(self):
        levels = _compute_levels({"r1"}, broader={}, root_ids={"r1"})
        assert levels["r1"] == 0

    def test_child_levels(self):
        levels = _compute_levels(
            {"r1", "c1", "gc1"},
            broader={"c1": ["r1"], "gc1": ["c1"]},
            root_ids={"r1"},
        )
        assert levels["r1"] == 0
        assert levels["c1"] == 1
        assert levels["gc1"] == 2


class TestParseLoc:
    def test_end_to_end(self, sample_gz):
        with patch("src.parsers.loc._download_bulk", return_value=sample_gz):
            records = parse_loc(max_depth=6)

        labels = {r["id"]: r["label"] for r in records}
        assert "sh85118553" in labels  # Science root
        assert "sh85133067" in labels  # Technology root
        assert "sh85009003" in labels  # Astronomy (child of Science)
        assert "sh85014203" in labels  # Biology (child of Science)
        assert "sh85043000" in labels  # Engineering (child of Technology)
        assert "sh99999999" not in labels  # Unrelated, not in subtree

    def test_record_schema(self, sample_gz):
        with patch("src.parsers.loc._download_bulk", return_value=sample_gz):
            records = parse_loc(max_depth=6)

        r = next(r for r in records if r["id"] == "sh85009003")
        required_keys = {"id", "label", "definition", "parent_id", "type", "uri",
                         "full_path", "level", "aliases", "cross_refs", "version"}
        assert required_keys.issubset(set(r.keys()))
        assert r["type"] == "subject_heading"
        assert r["uri"].startswith("https://id.loc.gov/")
        assert r["parent_id"] == "sh85118553"
        assert r["level"] == 1

    def test_depth_filter(self, sample_gz):
        with patch("src.parsers.loc._download_bulk", return_value=sample_gz):
            records = parse_loc(max_depth=0)

        ids = {r["id"] for r in records}
        assert "sh85118553" in ids
        assert "sh85133067" in ids
        assert "sh85009003" not in ids  # depth 1, excluded

    def test_paths(self, sample_gz):
        with patch("src.parsers.loc._download_bulk", return_value=sample_gz):
            records = parse_loc(max_depth=6)

        r = next(r for r in records if r["id"] == "sh85009003")
        assert r["full_path"] == "Science > Astronomy"

    def test_cross_refs_filtered_to_subtree(self, sample_gz):
        with patch("src.parsers.loc._download_bulk", return_value=sample_gz):
            records = parse_loc(max_depth=6)

        r = next(r for r in records if r["id"] == "sh85009003")
        # Biology is in subtree, so should appear as cross_ref
        assert "sh85014203" in r["cross_refs"]

    def test_definitions_and_notes(self, sample_gz):
        with patch("src.parsers.loc._download_bulk", return_value=sample_gz):
            records = parse_loc(max_depth=6)

        sci = next(r for r in records if r["id"] == "sh85118553")
        assert sci["definition"] == "General works on science"

        eng = next(r for r in records if r["id"] == "sh85043000")
        assert eng["definition"] == "The application of science"
