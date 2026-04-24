"""Unit tests for NCBI taxonomy parser using small sample data."""

import pytest
from src.parsers.ncbi import (
    _parse_nodes_text,
    _parse_names_text,
    _find_filtered_parent,
    _build_full_path,
    RANK_LEVEL,
)

# Sample nodes.dmp content (tab-pipe-tab separated, lines end with \t|\n)
SAMPLE_NODES = """\
1\t|\t1\t|\tno rank\t|\t\t|\t8\t|\t0\t|\t1\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
2\t|\t131567\t|\tsuperkingdom\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
131567\t|\t1\t|\tno rank\t|\t\t|\t8\t|\t0\t|\t1\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
2759\t|\t131567\t|\tsuperkingdom\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
33208\t|\t2759\t|\tkingdom\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
7711\t|\t33208\t|\tphylum\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
40674\t|\t32524\t|\tclass\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
32524\t|\t7711\t|\tno rank\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
9443\t|\t314146\t|\torder\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
314146\t|\t40674\t|\tno rank\t|\t\t|\t0\t|\t0\t|\t11\t|\t0\t|\t0\t|\t0\t|\t0\t|\t0\t|\t\t|
"""

# Sample names.dmp content
SAMPLE_NAMES = """\
1\t|\troot\t|\t\t|\tscientific name\t|
2\t|\tBacteria\t|\tBacteria <bacteria>\t|\tscientific name\t|
2\t|\tMonera\t|\tMonera <Bacteria>\t|\tsynonym\t|
131567\t|\tcellular organisms\t|\t\t|\tscientific name\t|
2759\t|\tEukaryota\t|\t\t|\tscientific name\t|
2759\t|\teukaryotes\t|\t\t|\tgenbank common name\t|
33208\t|\tMetazoa\t|\t\t|\tscientific name\t|
33208\t|\tAnimalia\t|\t\t|\tsynonym\t|
33208\t|\tanimals\t|\t\t|\tgenbank common name\t|
7711\t|\tChordata\t|\t\t|\tscientific name\t|
32524\t|\tAmniota\t|\t\t|\tscientific name\t|
40674\t|\tMammalia\t|\t\t|\tscientific name\t|
40674\t|\tmammals\t|\t\t|\tgenbank common name\t|
314146\t|\tEuarchontoglires\t|\t\t|\tscientific name\t|
9443\t|\tPrimates\t|\t\t|\tscientific name\t|
"""


@pytest.fixture
def nodes():
    return _parse_nodes_text(SAMPLE_NODES)


@pytest.fixture
def names():
    return _parse_names_text(SAMPLE_NAMES)


class TestParseNodes:
    def test_parses_all_nodes(self, nodes):
        assert len(nodes) == 10

    def test_root_is_self_parent(self, nodes):
        assert nodes["1"] == ("1", "no rank")

    def test_superkingdom(self, nodes):
        assert nodes["2"] == ("131567", "superkingdom")
        assert nodes["2759"] == ("131567", "superkingdom")

    def test_intermediate_rank(self, nodes):
        assert nodes["131567"] == ("1", "no rank")
        assert nodes["32524"] == ("7711", "no rank")

    def test_order_rank(self, nodes):
        assert nodes["9443"] == ("314146", "order")


class TestParseNames:
    def test_scientific_names(self, names):
        sci, _ = names
        assert sci["2"] == "Bacteria"
        assert sci["2759"] == "Eukaryota"
        assert sci["9443"] == "Primates"

    def test_aliases_synonym(self, names):
        _, aliases = names
        assert "Monera" in aliases["2"]
        assert "Animalia" in aliases["33208"]

    def test_aliases_genbank_common(self, names):
        _, aliases = names
        assert "eukaryotes" in aliases["2759"]
        assert "mammals" in aliases["40674"]

    def test_no_scientific_in_aliases(self, names):
        _, aliases = names
        # scientific names should not appear in aliases
        assert "Bacteria" not in aliases.get("2", [])


class TestFindFilteredParent:
    def test_superkingdom_has_no_parent(self, nodes):
        allowed = {"superkingdom", "kingdom", "phylum", "class", "order"}
        # Bacteria (2) parent is 131567 (no rank) -> 1 (no rank, root)
        parent = _find_filtered_parent("2", nodes, allowed)
        assert parent is None

    def test_kingdom_parent_is_superkingdom(self, nodes):
        allowed = {"superkingdom", "kingdom", "phylum", "class", "order"}
        # Metazoa (33208, kingdom) -> 2759 (superkingdom)
        parent = _find_filtered_parent("33208", nodes, allowed)
        assert parent == "2759"

    def test_skips_no_rank_intermediates(self, nodes):
        allowed = {"superkingdom", "kingdom", "phylum", "class", "order"}
        # Mammalia (40674, class) -> 32524 (no rank) -> 7711 (phylum)
        parent = _find_filtered_parent("40674", nodes, allowed)
        assert parent == "7711"

    def test_order_skips_to_class(self, nodes):
        allowed = {"superkingdom", "kingdom", "phylum", "class", "order"}
        # Primates (9443, order) -> 314146 (no rank) -> 40674 (class)
        parent = _find_filtered_parent("9443", nodes, allowed)
        assert parent == "40674"


class TestBuildFullPath:
    def test_superkingdom_path(self, nodes, names):
        sci, _ = names
        allowed = {"superkingdom", "kingdom", "phylum", "class", "order"}
        path = _build_full_path("2759", nodes, sci, allowed)
        assert path == "Eukaryota"

    def test_kingdom_path(self, nodes, names):
        sci, _ = names
        allowed = {"superkingdom", "kingdom", "phylum", "class", "order"}
        path = _build_full_path("33208", nodes, sci, allowed)
        assert path == "Eukaryota > Metazoa"

    def test_deep_path_skips_intermediates(self, nodes, names):
        sci, _ = names
        allowed = {"superkingdom", "kingdom", "phylum", "class", "order"}
        path = _build_full_path("9443", nodes, sci, allowed)
        assert path == "Eukaryota > Metazoa > Chordata > Mammalia > Primates"


class TestRankLevel:
    def test_superkingdom_is_zero(self):
        assert RANK_LEVEL["superkingdom"] == 0

    def test_order_is_four(self):
        assert RANK_LEVEL["order"] == 4

    def test_all_ranks_present(self):
        expected = {"superkingdom", "kingdom", "phylum", "class", "order"}
        assert set(RANK_LEVEL.keys()) == expected


class TestRecordSchema:
    """Verify record structure matches unified schema."""

    def _make_records(self, nodes, names):
        """Build records from sample data manually (avoids download)."""
        sci, aliases_map = names
        allowed = {"superkingdom", "kingdom", "phylum", "class", "order"}
        filtered_ids = {tid for tid, (pid, rank) in nodes.items() if rank in allowed}
        records = []
        for tax_id in sorted(filtered_ids, key=int):
            _, rank = nodes[tax_id]
            records.append({
                "id": tax_id,
                "label": sci.get(tax_id, f"taxid:{tax_id}"),
                "definition": None,
                "parent_id": _find_filtered_parent(tax_id, nodes, allowed),
                "type": rank,
                "uri": f"https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id={tax_id}",
                "full_path": _build_full_path(tax_id, nodes, sci, allowed),
                "level": RANK_LEVEL.get(rank, 0),
                "aliases": aliases_map.get(tax_id, []),
                "cross_refs": [],
                "version": None,
            })
        return records

    def test_correct_count(self, nodes, names):
        records = self._make_records(nodes, names)
        # 2 superkingdoms + 1 kingdom + 1 phylum + 1 class + 1 order = 6
        assert len(records) == 6

    def test_required_fields(self, nodes, names):
        records = self._make_records(nodes, names)
        required = {"id", "label", "definition", "parent_id", "type",
                     "uri", "full_path", "level", "aliases", "cross_refs", "version"}
        for r in records:
            assert set(r.keys()) == required

    def test_uri_format(self, nodes, names):
        records = self._make_records(nodes, names)
        for r in records:
            assert r["uri"].startswith("https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=")
            assert r["id"] in r["uri"]

    def test_primates_record(self, nodes, names):
        records = self._make_records(nodes, names)
        primates = [r for r in records if r["label"] == "Primates"][0]
        assert primates["id"] == "9443"
        assert primates["type"] == "order"
        assert primates["level"] == 4
        assert primates["parent_id"] == "40674"
        assert primates["full_path"] == "Eukaryota > Metazoa > Chordata > Mammalia > Primates"

    def test_bacteria_aliases(self, nodes, names):
        records = self._make_records(nodes, names)
        bacteria = [r for r in records if r["label"] == "Bacteria"][0]
        assert "Monera" in bacteria["aliases"]
