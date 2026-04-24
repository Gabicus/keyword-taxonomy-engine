"""Tests for NetworkX graph module."""

import pytest
from src.graph import (
    build_graph, get_roots, get_leaves, get_ancestors, get_full_path,
    get_descendants, get_children, get_siblings, get_subtree, get_depth,
    get_stats, find_common_ancestor, find_by_label, to_tree_dict,
)

SAMPLE_RECORDS = [
    {"id": "root", "label": "Science", "parent_id": None, "level": 0, "source": "TEST"},
    {"id": "phys", "label": "Physics", "parent_id": "root", "level": 1, "source": "TEST"},
    {"id": "chem", "label": "Chemistry", "parent_id": "root", "level": 1, "source": "TEST"},
    {"id": "qm", "label": "Quantum Mechanics", "parent_id": "phys", "level": 2, "source": "TEST"},
    {"id": "thermo", "label": "Thermodynamics", "parent_id": "phys", "level": 2, "source": "TEST"},
    {"id": "orgchem", "label": "Organic Chemistry", "parent_id": "chem", "level": 2, "source": "TEST"},
    {"id": "qft", "label": "Quantum Field Theory", "parent_id": "qm", "level": 3, "source": "TEST"},
]


@pytest.fixture
def graph():
    return build_graph(SAMPLE_RECORDS)


class TestBuildGraph:
    def test_node_count(self, graph):
        assert graph.number_of_nodes() == 7

    def test_edge_count(self, graph):
        assert graph.number_of_edges() == 6

    def test_node_attributes(self, graph):
        assert graph.nodes["phys"]["label"] == "Physics"
        assert graph.nodes["phys"]["source"] == "TEST"


class TestTraversal:
    def test_roots(self, graph):
        assert get_roots(graph) == ["root"]

    def test_leaves(self, graph):
        leaves = set(get_leaves(graph))
        assert leaves == {"qft", "thermo", "orgchem"}

    def test_ancestors(self, graph):
        assert get_ancestors(graph, "qft") == ["qm", "phys", "root"]

    def test_ancestors_root(self, graph):
        assert get_ancestors(graph, "root") == []

    def test_full_path(self, graph):
        assert get_full_path(graph, "qft") == "Science > Physics > Quantum Mechanics > Quantum Field Theory"

    def test_full_path_root(self, graph):
        assert get_full_path(graph, "root") == "Science"

    def test_descendants(self, graph):
        desc = set(get_descendants(graph, "phys"))
        assert desc == {"qm", "thermo", "qft"}

    def test_children(self, graph):
        children = set(get_children(graph, "root"))
        assert children == {"phys", "chem"}

    def test_siblings(self, graph):
        siblings = get_siblings(graph, "qm")
        assert siblings == ["thermo"]

    def test_depth(self, graph):
        assert get_depth(graph, "root") == 0
        assert get_depth(graph, "phys") == 1
        assert get_depth(graph, "qft") == 3


class TestAnalysis:
    def test_stats(self, graph):
        stats = get_stats(graph)
        assert stats["total_nodes"] == 7
        assert stats["roots"] == 1
        assert stats["leaves"] == 3
        assert stats["max_depth"] == 3

    def test_common_ancestor(self, graph):
        assert find_common_ancestor(graph, "qm", "thermo") == "phys"
        assert find_common_ancestor(graph, "qft", "orgchem") == "root"
        assert find_common_ancestor(graph, "qm", "qft") == "qm"

    def test_find_by_label(self, graph):
        assert find_by_label(graph, "quantum") == ["qm", "qft"]
        assert find_by_label(graph, "Physics", exact=True) == ["phys"]

    def test_subtree(self, graph):
        sub = get_subtree(graph, "phys")
        assert sub.number_of_nodes() == 4
        assert "chem" not in sub


class TestExport:
    def test_tree_dict(self, graph):
        tree = to_tree_dict(graph, "root")
        assert tree["label"] == "Science"
        assert len(tree["children"]) == 2
        phys_child = next(c for c in tree["children"] if c["id"] == "phys")
        assert len(phys_child["children"]) == 2

    def test_tree_dict_max_depth(self, graph):
        tree = to_tree_dict(graph, "root", max_depth=1)
        phys_child = next(c for c in tree["children"] if c["id"] == "phys")
        assert phys_child["children"] == []
