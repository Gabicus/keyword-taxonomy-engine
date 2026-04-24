"""NetworkX graph module for taxonomy traversal and analysis.

Builds in-memory directed graphs from keyword records, enabling
hierarchy traversal, path finding, and cross-taxonomy analysis.
"""

import networkx as nx
from collections import defaultdict


def build_graph(records: list[dict]) -> nx.DiGraph:
    """Build a directed graph from keyword records.

    Edges point from child → parent (broader direction).
    Each node stores the full record as attributes.
    """
    G = nx.DiGraph()

    for r in records:
        node_id = r["id"]
        G.add_node(node_id, **{
            k: v for k, v in r.items() if k != "id"
        })

        if r.get("parent_id"):
            G.add_edge(node_id, r["parent_id"])

    return G


def get_roots(G: nx.DiGraph) -> list[str]:
    """Get all root nodes (no parent / out-degree 0 in child→parent graph)."""
    return [n for n in G.nodes() if G.out_degree(n) == 0]


def get_leaves(G: nx.DiGraph) -> list[str]:
    """Get all leaf nodes (no children / in-degree 0 in child→parent graph)."""
    return [n for n in G.nodes() if G.in_degree(n) == 0]


def get_ancestors(G: nx.DiGraph, node_id: str) -> list[str]:
    """Get ordered list of ancestors from node to root (excluding node itself)."""
    path = []
    current = node_id
    visited = set()
    while True:
        successors = list(G.successors(current))
        if not successors or current in visited:
            break
        visited.add(current)
        parent = successors[0]
        if parent == current:
            break
        path.append(parent)
        current = parent
    return path


def get_full_path(G: nx.DiGraph, node_id: str) -> str:
    """Reconstruct full hierarchical path for a node."""
    if node_id not in G:
        return ""
    ancestors = get_ancestors(G, node_id)
    labels = []
    for a in reversed(ancestors):
        label = G.nodes[a].get("label", a)
        labels.append(label)
    labels.append(G.nodes[node_id].get("label", node_id))
    return " > ".join(labels)


def get_descendants(G: nx.DiGraph, node_id: str) -> list[str]:
    """Get all descendants of a node (children, grandchildren, etc.)."""
    return list(nx.ancestors(G, node_id))  # reversed edge direction


def get_children(G: nx.DiGraph, node_id: str) -> list[str]:
    """Get direct children of a node."""
    return list(G.predecessors(node_id))


def get_siblings(G: nx.DiGraph, node_id: str) -> list[str]:
    """Get siblings (nodes sharing same parent), excluding self."""
    parents = list(G.successors(node_id))
    if not parents:
        return []
    siblings = []
    for p in parents:
        for child in G.predecessors(p):
            if child != node_id:
                siblings.append(child)
    return siblings


def get_subtree(G: nx.DiGraph, node_id: str) -> nx.DiGraph:
    """Extract the subtree rooted at node_id."""
    desc = get_descendants(G, node_id)
    nodes = [node_id] + desc
    return G.subgraph(nodes).copy()


def get_depth(G: nx.DiGraph, node_id: str) -> int:
    """Get depth of node (distance to root). Root = 0."""
    return len(get_ancestors(G, node_id))


def get_stats(G: nx.DiGraph) -> dict:
    """Get summary statistics for the graph."""
    depths = defaultdict(int)
    for n in G.nodes():
        d = get_depth(G, n)
        depths[d] += 1

    return {
        "total_nodes": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "roots": len(get_roots(G)),
        "leaves": len(get_leaves(G)),
        "max_depth": max(depths.keys()) if depths else 0,
        "nodes_by_depth": dict(sorted(depths.items())),
    }


def find_common_ancestor(G: nx.DiGraph, node_a: str, node_b: str) -> str | None:
    """Find the lowest common ancestor of two nodes."""
    ancestors_a = set(get_ancestors(G, node_a)) | {node_a}
    ancestors_b = set(get_ancestors(G, node_b)) | {node_b}
    common = ancestors_a & ancestors_b
    if not common:
        return None
    # Return the deepest common ancestor
    return max(common, key=lambda n: get_depth(G, n))


def find_by_label(G: nx.DiGraph, label: str, exact: bool = False) -> list[str]:
    """Find nodes by label. Case-insensitive unless exact=True."""
    results = []
    for n, data in G.nodes(data=True):
        node_label = data.get("label", "")
        if exact:
            if node_label == label:
                results.append(n)
        else:
            if label.lower() in node_label.lower():
                results.append(n)
    return results


def to_tree_dict(G: nx.DiGraph, node_id: str, max_depth: int = None) -> dict:
    """Convert subtree to nested dict (useful for JSON export / visualization)."""
    data = dict(G.nodes[node_id])
    data["id"] = node_id
    if max_depth is not None and max_depth <= 0:
        data["children"] = []
        return data
    children = get_children(G, node_id)
    next_depth = max_depth - 1 if max_depth is not None else None
    data["children"] = [to_tree_dict(G, c, next_depth) for c in sorted(
        children, key=lambda c: G.nodes[c].get("label", "")
    )]
    return data
