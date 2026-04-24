"""Tests for OpenAlex parser with mocked API responses."""

import json
import pytest
from unittest.mock import MagicMock, patch, call

from src.parsers.openalex import (
    parse_openalex,
    _extract_openalex_id,
    _get_parent_id,
    _build_full_path,
    _normalize_entity,
    _extract_keywords_from_topics,
    _fetch_all_pages,
)


# --- Sample API response fixtures ---

SAMPLE_DOMAIN = {
    "id": "https://openalex.org/domains/1",
    "display_name": "Life Sciences",
    "description": "Study of living organisms",
    "keywords": [],
}

SAMPLE_FIELD = {
    "id": "https://openalex.org/fields/12",
    "display_name": "Ecology",
    "description": "Study of ecosystems",
    "domain": {"id": "https://openalex.org/domains/1", "display_name": "Life Sciences"},
    "keywords": [],
}

SAMPLE_SUBFIELD = {
    "id": "https://openalex.org/subfields/1205",
    "display_name": "Aquatic Science",
    "description": "Study of aquatic environments",
    "domain": {"id": "https://openalex.org/domains/1", "display_name": "Life Sciences"},
    "field": {"id": "https://openalex.org/fields/12", "display_name": "Ecology"},
    "keywords": [],
}

SAMPLE_TOPIC = {
    "id": "https://openalex.org/T12345",
    "display_name": "Marine Biodiversity",
    "description": "Study of marine species diversity",
    "domain": {"id": "https://openalex.org/domains/1", "display_name": "Life Sciences"},
    "field": {"id": "https://openalex.org/fields/12", "display_name": "Ecology"},
    "subfield": {"id": "https://openalex.org/subfields/1205", "display_name": "Aquatic Science"},
    "keywords": [
        {"display_name": "coral reefs"},
        {"display_name": "ocean biodiversity"},
        {"display_name": "Marine Biodiversity"},  # same as label, should be skipped
    ],
}

SAMPLE_TOPIC_STRING_KEYWORDS = {
    "id": "https://openalex.org/T99999",
    "display_name": "Forest Ecology",
    "description": None,
    "domain": {"id": "https://openalex.org/domains/1", "display_name": "Life Sciences"},
    "field": {"id": "https://openalex.org/fields/12", "display_name": "Ecology"},
    "subfield": {"id": "https://openalex.org/subfields/1205", "display_name": "Aquatic Science"},
    "keywords": ["canopy", "understory"],
}


def _mock_response(results, next_cursor=None):
    """Create a mock response object for a single page."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "results": results,
        "meta": {"next_cursor": next_cursor, "count": len(results)},
    }
    return resp


class TestExtractOpenalexId:
    def test_full_url(self):
        assert _extract_openalex_id({"id": "https://openalex.org/T12345"}) == "T12345"

    def test_short_id(self):
        assert _extract_openalex_id({"id": "T12345"}) == "T12345"

    def test_domain_id(self):
        assert _extract_openalex_id({"id": "https://openalex.org/domains/1"}) == "1"

    def test_empty(self):
        assert _extract_openalex_id({}) == ""


class TestGetParentId:
    def test_domain_no_parent(self):
        assert _get_parent_id(SAMPLE_DOMAIN, "domain") is None

    def test_field_parent_is_domain(self):
        assert _get_parent_id(SAMPLE_FIELD, "field") == "1"

    def test_subfield_parent_is_field(self):
        assert _get_parent_id(SAMPLE_SUBFIELD, "subfield") == "12"

    def test_topic_parent_is_subfield(self):
        assert _get_parent_id(SAMPLE_TOPIC, "topic") == "1205"


class TestBuildFullPath:
    def test_domain_path(self):
        path = _build_full_path(SAMPLE_DOMAIN, "domain")
        assert path == "Life Sciences"

    def test_field_path(self):
        path = _build_full_path(SAMPLE_FIELD, "field")
        assert path == "Life Sciences > Ecology"

    def test_subfield_path(self):
        path = _build_full_path(SAMPLE_SUBFIELD, "subfield")
        assert path == "Life Sciences > Ecology > Aquatic Science"

    def test_topic_path(self):
        path = _build_full_path(SAMPLE_TOPIC, "topic")
        assert path == "Life Sciences > Ecology > Aquatic Science > Marine Biodiversity"


class TestNormalizeEntity:
    def test_domain_record(self):
        rec = _normalize_entity(SAMPLE_DOMAIN, "domain", 0)
        assert rec["id"] == "1"
        assert rec["label"] == "Life Sciences"
        assert rec["definition"] == "Study of living organisms"
        assert rec["parent_id"] is None
        assert rec["type"] == "domain"
        assert rec["level"] == 0
        assert rec["uri"] == "https://openalex.org/domains/1"

    def test_topic_record(self):
        rec = _normalize_entity(SAMPLE_TOPIC, "topic", 3)
        assert rec["id"] == "T12345"
        assert rec["label"] == "Marine Biodiversity"
        assert rec["parent_id"] == "1205"
        assert rec["type"] == "topic"
        assert rec["level"] == 3
        # Keywords matching the label should be excluded from aliases
        assert "Marine Biodiversity" not in rec["aliases"]
        assert "coral reefs" in rec["aliases"]
        assert "ocean biodiversity" in rec["aliases"]

    def test_topic_string_keywords(self):
        rec = _normalize_entity(SAMPLE_TOPIC_STRING_KEYWORDS, "topic", 3)
        assert "canopy" in rec["aliases"]
        assert "understory" in rec["aliases"]

    def test_all_fields_present(self):
        rec = _normalize_entity(SAMPLE_DOMAIN, "domain", 0)
        expected_keys = {
            "id", "label", "definition", "parent_id", "type",
            "uri", "full_path", "aliases", "level", "cross_refs", "version",
        }
        assert set(rec.keys()) == expected_keys


class TestExtractKeywords:
    def test_keyword_extraction(self):
        topic_records = [_normalize_entity(SAMPLE_TOPIC, "topic", 3)]
        raw_topics = [SAMPLE_TOPIC]
        kw_records = _extract_keywords_from_topics(topic_records, raw_topics)
        kw_labels = {r["label"] for r in kw_records}
        assert "coral reefs" in kw_labels
        assert "ocean biodiversity" in kw_labels
        # label-matching keyword excluded
        assert "Marine Biodiversity" not in kw_labels

    def test_keyword_parent_id(self):
        topic_records = [_normalize_entity(SAMPLE_TOPIC, "topic", 3)]
        raw_topics = [SAMPLE_TOPIC]
        kw_records = _extract_keywords_from_topics(topic_records, raw_topics)
        assert all(r["parent_id"] == "T12345" for r in kw_records)
        assert all(r["level"] == 4 for r in kw_records)
        assert all(r["type"] == "keyword" for r in kw_records)

    def test_keyword_deduplication(self):
        """Same keyword across two topics should only appear once."""
        topic2 = {**SAMPLE_TOPIC, "id": "https://openalex.org/T99999",
                  "display_name": "Ocean Studies",
                  "keywords": [{"display_name": "coral reefs"}]}
        records = [
            _normalize_entity(SAMPLE_TOPIC, "topic", 3),
            _normalize_entity(topic2, "topic", 3),
        ]
        raw = [SAMPLE_TOPIC, topic2]
        kw_records = _extract_keywords_from_topics(records, raw)
        coral_count = sum(1 for r in kw_records if r["label"] == "coral reefs")
        assert coral_count == 1

    def test_string_keywords(self):
        topic_records = [_normalize_entity(SAMPLE_TOPIC_STRING_KEYWORDS, "topic", 3)]
        raw_topics = [SAMPLE_TOPIC_STRING_KEYWORDS]
        kw_records = _extract_keywords_from_topics(topic_records, raw_topics)
        kw_labels = {r["label"] for r in kw_records}
        assert "canopy" in kw_labels
        assert "understory" in kw_labels


class TestFetchAllPages:
    @patch("src.parsers.openalex.time.sleep")
    def test_single_page(self, mock_sleep):
        session = MagicMock()
        session.get.return_value = _mock_response([SAMPLE_DOMAIN], next_cursor=None)
        results = _fetch_all_pages("domains", session, "domain")
        assert len(results) == 1
        assert results[0]["display_name"] == "Life Sciences"
        mock_sleep.assert_not_called()

    @patch("src.parsers.openalex.time.sleep")
    def test_multi_page(self, mock_sleep):
        """Simulate two pages of results."""
        page1_results = [SAMPLE_DOMAIN] * 200  # full page
        page2_results = [SAMPLE_DOMAIN] * 50   # partial page

        session = MagicMock()
        session.get.side_effect = [
            _mock_response(page1_results, next_cursor="abc123"),
            _mock_response(page2_results, next_cursor=None),
        ]
        results = _fetch_all_pages("domains", session, "domain")
        assert len(results) == 250
        assert session.get.call_count == 2
        mock_sleep.assert_called_once()


class TestParseOpenalex:
    @patch("src.parsers.openalex.time.sleep")
    @patch("src.parsers.openalex._fetch_all_pages")
    def test_full_parse(self, mock_fetch, mock_sleep):
        """Integration test with mocked fetch."""
        mock_fetch.side_effect = [
            [SAMPLE_DOMAIN],           # domains
            [SAMPLE_FIELD],            # fields
            [SAMPLE_SUBFIELD],         # subfields
            [SAMPLE_TOPIC],            # topics
        ]

        records = parse_openalex(session=MagicMock())

        # 1 domain + 1 field + 1 subfield + 1 topic + 2 keywords
        assert len(records) == 6

        types = {r["type"] for r in records}
        assert types == {"domain", "field", "subfield", "topic", "keyword"}

        domains = [r for r in records if r["type"] == "domain"]
        assert len(domains) == 1
        assert domains[0]["label"] == "Life Sciences"

    @patch("src.parsers.openalex.time.sleep")
    @patch("src.parsers.openalex._fetch_all_pages")
    def test_handles_fetch_failure(self, mock_fetch, mock_sleep):
        """Should continue if one level fails."""
        mock_fetch.side_effect = [
            [SAMPLE_DOMAIN],
            Exception("Network error"),
            [SAMPLE_SUBFIELD],
            [SAMPLE_TOPIC],
        ]

        records = parse_openalex(session=MagicMock())
        types = {r["type"] for r in records}
        assert "domain" in types
        assert "field" not in types  # failed
        assert "subfield" in types

    @patch("src.parsers.openalex.time.sleep")
    @patch("src.parsers.openalex._fetch_all_pages")
    def test_no_keywords_when_no_topics(self, mock_fetch, mock_sleep):
        """If topics fetch fails, no keyword extraction."""
        mock_fetch.side_effect = [
            [SAMPLE_DOMAIN],
            [SAMPLE_FIELD],
            [SAMPLE_SUBFIELD],
            Exception("timeout"),
        ]

        records = parse_openalex(session=MagicMock())
        assert not any(r["type"] == "keyword" for r in records)
