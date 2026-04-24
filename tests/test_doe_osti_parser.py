"""Tests for DOE OSTI Subject Categories parser."""

import pytest
from unittest.mock import MagicMock, patch

from src.parsers.doe_osti import (
    parse_doe_osti,
    _build_records_from_tuples,
    _build_code_to_group,
    _try_fetch_api,
    FALLBACK_CATEGORIES,
    CATEGORY_GROUPS,
)


class TestBuildCodeToGroup:
    def test_all_fallback_codes_mapped(self):
        """Every code in FALLBACK_CATEGORIES should map to a group."""
        mapping = _build_code_to_group()
        for code, label, _ in FALLBACK_CATEGORIES:
            assert code in mapping, f"Code {code} ({label}) has no group mapping"

    def test_no_duplicate_codes_across_groups(self):
        """Each code should belong to exactly one group."""
        seen = {}
        for group_id, group in CATEGORY_GROUPS.items():
            for code in group["codes"]:
                assert code not in seen, f"Code {code} in both {seen[code]} and {group_id}"
                seen[code] = group_id


class TestBuildRecordsFromTuples:
    def test_creates_group_parents(self):
        records = _build_records_from_tuples(FALLBACK_CATEGORIES)
        group_records = [r for r in records if r["id"].startswith("osti-group-")]
        assert len(group_records) == len(CATEGORY_GROUPS)

    def test_creates_category_records(self):
        records = _build_records_from_tuples(FALLBACK_CATEGORIES)
        cat_records = [r for r in records if not r["id"].startswith("osti-group-")]
        assert len(cat_records) == len(FALLBACK_CATEGORIES)

    def test_total_record_count(self):
        records = _build_records_from_tuples(FALLBACK_CATEGORIES)
        expected = len(FALLBACK_CATEGORIES) + len(CATEGORY_GROUPS)
        assert len(records) == expected

    def test_record_schema(self):
        records = _build_records_from_tuples([("14", "Solar Energy", "Solar stuff")])
        # Should have 1 group parent + 1 category (group for renewable_energy)
        cat = [r for r in records if r["id"] == "osti-14"]
        assert len(cat) == 1
        r = cat[0]
        required_keys = {"id", "label", "definition", "parent_id", "type", "uri",
                         "full_path", "level", "aliases", "cross_refs", "version"}
        assert required_keys.issubset(set(r.keys()))
        assert r["type"] == "subject_category"
        assert r["label"] == "Solar Energy"
        assert r["definition"] == "Solar stuff"
        assert r["level"] == 1

    def test_parent_id_links_to_group(self):
        records = _build_records_from_tuples(FALLBACK_CATEGORIES)
        solar = [r for r in records if r["id"] == "osti-14"][0]
        assert solar["parent_id"] == "osti-group-renewable_energy"

    def test_full_path_includes_group(self):
        records = _build_records_from_tuples(FALLBACK_CATEGORIES)
        solar = [r for r in records if r["id"] == "osti-14"][0]
        assert solar["full_path"] == "Renewable Energy > Solar Energy"

    def test_aliases_include_code(self):
        records = _build_records_from_tuples([("14", "Solar Energy", "")])
        cat = [r for r in records if r["id"] == "osti-14"][0]
        assert "OSTI 14" in cat["aliases"]
        assert "DOE Subject 14" in cat["aliases"]

    def test_group_parents_have_no_parent(self):
        records = _build_records_from_tuples(FALLBACK_CATEGORIES)
        groups = [r for r in records if r["id"].startswith("osti-group-")]
        assert all(r["parent_id"] is None for r in groups)
        assert all(r["level"] == 0 for r in groups)


class TestTryFetchApi:
    @patch("src.parsers.doe_osti.get_session")
    def test_returns_none_on_failure(self, mock_get_session):
        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 404
        session.get.return_value = resp
        assert _try_fetch_api(session) is None

    @patch("src.parsers.doe_osti.get_session")
    def test_returns_data_on_success(self, mock_get_session):
        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = [{"code": "14", "name": "Solar Energy"}]
        session.get.return_value = resp
        result = _try_fetch_api(session)
        assert result is not None
        assert len(result) == 1


class TestParseDoeOsti:
    @patch("src.parsers.doe_osti._try_scrape_page", return_value=None)
    @patch("src.parsers.doe_osti._try_fetch_api", return_value=None)
    def test_fallback_returns_records(self, mock_api, mock_scrape):
        """When API and scraping both fail, should use fallback list."""
        records = parse_doe_osti(session=MagicMock())
        assert len(records) > 0
        # Should have groups + categories
        assert len(records) == len(FALLBACK_CATEGORIES) + len(CATEGORY_GROUPS)

    @patch("src.parsers.doe_osti._try_scrape_page", return_value=None)
    @patch("src.parsers.doe_osti._try_fetch_api")
    def test_api_data_used_when_available(self, mock_api, mock_scrape):
        mock_api.return_value = [
            {"code": "14", "name": "Solar Energy", "description": "Solar stuff"},
            {"code": "15", "name": "Geothermal Energy", "description": "Geo stuff"},
        ]
        records = parse_doe_osti(session=MagicMock())
        labels = [r["label"] for r in records]
        assert "Solar Energy" in labels
        assert "Geothermal Energy" in labels

    def test_all_records_have_type(self):
        with patch("src.parsers.doe_osti._try_fetch_api", return_value=None), \
             patch("src.parsers.doe_osti._try_scrape_page", return_value=None):
            records = parse_doe_osti(session=MagicMock())
        assert all(r["type"] == "subject_category" for r in records)

    def test_no_empty_labels(self):
        with patch("src.parsers.doe_osti._try_fetch_api", return_value=None), \
             patch("src.parsers.doe_osti._try_scrape_page", return_value=None):
            records = parse_doe_osti(session=MagicMock())
        assert all(r["label"] for r in records)

    def test_all_records_have_uri(self):
        with patch("src.parsers.doe_osti._try_fetch_api", return_value=None), \
             patch("src.parsers.doe_osti._try_scrape_page", return_value=None):
            records = parse_doe_osti(session=MagicMock())
        assert all(r["uri"] for r in records)
