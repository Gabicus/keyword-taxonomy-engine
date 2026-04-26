"""Tests for database schema creation."""

import pytest
import duckdb

from src.schema import init_all_tables


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


class TestInitAllTables:
    def test_creates_all_tables(self, conn):
        init_all_tables(conn)
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchdf()
        names = set(tables["table_name"])
        assert "raw_nasa_gcmd" in names
        assert "raw_unesco" in names
        assert "raw_openalex" in names
        assert "raw_ncbi" in names
        assert "raw_loc" in names
        assert "raw_doe_osti" in names
        assert "keywords" in names
        assert "cross_taxonomy_alignment" in names

    def test_idempotent(self, conn):
        init_all_tables(conn)
        init_all_tables(conn)  # should not error
        count = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchone()[0]
        assert count == 21

    def test_raw_nasa_columns(self, conn):
        init_all_tables(conn)
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_nasa_gcmd'"
        ).fetchdf()
        col_names = set(cols["column_name"])
        assert "uuid" in col_names
        assert "pref_label" in col_names
        assert "definition" in col_names
        assert "is_leaf" in col_names
        assert "broader_json" in col_names
        assert "resources_json" in col_names

    def test_raw_unesco_columns(self, conn):
        init_all_tables(conn)
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_unesco'"
        ).fetchdf()
        col_names = set(cols["column_name"])
        assert "exact_match" in col_names
        assert "close_match" in col_names
        assert "pref_label_fr" in col_names
        assert "notation" in col_names

    def test_raw_openalex_columns(self, conn):
        init_all_tables(conn)
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_openalex'"
        ).fetchdf()
        col_names = set(cols["column_name"])
        assert "works_count" in col_names
        assert "cited_by_count" in col_names
        assert "keywords_json" in col_names

    def test_raw_ncbi_columns(self, conn):
        init_all_tables(conn)
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_ncbi'"
        ).fetchdf()
        col_names = set(cols["column_name"])
        assert "genetic_code" in col_names
        assert "division_id" in col_names
        assert "lineage" in col_names

    def test_alignment_columns(self, conn):
        init_all_tables(conn)
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'cross_taxonomy_alignment'"
        ).fetchdf()
        col_names = set(cols["column_name"])
        assert "confidence" in col_names
        assert "method" in col_names
        assert "match_type" in col_names
        assert "reviewed" in col_names

    def test_can_insert_into_all_tables(self, conn):
        init_all_tables(conn)
        conn.execute("INSERT INTO raw_nasa_gcmd (uuid, pref_label, keyword_type) VALUES ('test', 'Test', 'sciencekeywords')")
        conn.execute("INSERT INTO raw_unesco (concept_uri, pref_label_en) VALUES ('http://test', 'Test')")
        conn.execute("INSERT INTO raw_openalex (openalex_id, display_name, entity_type) VALUES ('T1', 'Test', 'topic')")
        conn.execute("INSERT INTO raw_ncbi (tax_id, scientific_name, rank) VALUES (1, 'Test', 'order')")
        conn.execute("INSERT INTO raw_loc (loc_id, auth_label) VALUES ('sh123', 'Test')")
        conn.execute("INSERT INTO raw_doe_osti (category_code, category_name) VALUES ('01', 'Test')")
        conn.execute("INSERT INTO keywords (id, label, source) VALUES ('t', 'Test', 'TEST')")
        conn.execute("""INSERT INTO cross_taxonomy_alignment
            (source_id, source_name, target_id, target_name, match_type, method)
            VALUES ('a', 'NASA', 'b', 'UNESCO', 'exact', 'manual')""")
        assert conn.execute("SELECT COUNT(*) FROM raw_nasa_gcmd").fetchone()[0] == 1
