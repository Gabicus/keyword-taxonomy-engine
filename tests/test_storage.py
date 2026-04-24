"""Tests for DuckDB storage layer."""

import pytest
from pathlib import Path
from datetime import datetime, timezone

from src.storage import KeywordStore


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "test.duckdb"
    s = KeywordStore(db_path=db_path)
    yield s
    s.close()


def _make_record(id="test-1", label="Aerosols", **kwargs):
    defaults = {
        "id": id,
        "label": label,
        "definition": "Test definition",
        "parent_id": None,
        "type": "sciencekeywords",
        "uri": None,
        "full_path": "EARTH SCIENCE > ATMOSPHERE > AEROSOLS",
        "aliases": ["AOD"],
        "level": 2,
        "cross_refs": [],
        "version": "23.7",
    }
    defaults.update(kwargs)
    return defaults


class TestKeywordStore:
    def test_upsert_empty(self, store):
        result = store.upsert([], "TEST")
        assert result["inserted"] == 0

    def test_upsert_inserts(self, store):
        records = [_make_record(), _make_record(id="test-2", label="Clouds")]
        result = store.upsert(records, "TEST")
        assert result["inserted"] == 2
        assert result["previous"] == 0
        assert store.count("TEST") == 2

    def test_upsert_replaces(self, store):
        store.upsert([_make_record()], "TEST")
        store.upsert([_make_record(label="Updated Aerosols")], "TEST")
        assert store.count("TEST") == 1
        results = store.search("Updated", "TEST")
        assert len(results) == 1

    def test_upsert_is_source_scoped(self, store):
        store.upsert([_make_record(id="a1")], "SOURCE_A")
        store.upsert([_make_record(id="b1")], "SOURCE_B")
        assert store.count("SOURCE_A") == 1
        assert store.count("SOURCE_B") == 1

        store.upsert([_make_record(id="a2"), _make_record(id="a3")], "SOURCE_A")
        assert store.count("SOURCE_A") == 2
        assert store.count("SOURCE_B") == 1

    def test_search(self, store):
        store.upsert([
            _make_record(id="1", label="Aerosols"),
            _make_record(id="2", label="Aerosol Optical Depth"),
            _make_record(id="3", label="Clouds"),
        ], "TEST")
        results = store.search("aerosol")
        assert len(results) == 2

    def test_validate_pass(self, store):
        store.upsert([
            _make_record(id="root", label="Earth Science", parent_id=None, level=0),
            _make_record(id="child", label="Atmosphere", parent_id="root", level=1),
        ], "TEST")
        v = store.validate("TEST")
        assert v["valid"]
        assert v["count"] == 2

    def test_validate_orphans(self, store):
        store.upsert([
            _make_record(id="child", label="Orphan", parent_id="missing"),
        ], "TEST")
        v = store.validate("TEST")
        assert not v["valid"]
        assert v["orphans"] == 1

    def test_export_parquet(self, store, tmp_path):
        store.upsert([_make_record()], "TEST")
        path = store.export_parquet("TEST", tmp_path)
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_stats(self, store):
        store.upsert([_make_record(id="1")], "A")
        store.upsert([_make_record(id="2"), _make_record(id="3")], "B")
        stats = store.stats()
        assert stats["total"] == 3
        assert stats["by_source"]["A"] == 1
        assert stats["by_source"]["B"] == 2
