"""
Utilities for converting network results to other formats (e.g., NetworkX).
"""

import pandas as pd
import networkx as nx

from .types import NetworkResult


def _is_directed(meta: dict) -> bool:
    """
    Helper to read the 'directed' flag from meta safely.
    Falls back to False if missing.
    """
    return bool(meta.get("directed", False))


def to_networkx(result: NetworkResult, weighted: bool = True) -> nx.Graph:
    """
    Convert a NetworkResult into a NetworkX graph.

    Parameters
    ----------
    result : NetworkResult
        The output of any network builder (user_interaction, entity_cooccurrence,
        bipartite, etc.)
    weighted : bool, default=True
        Whether to include the 'weight' column as an edge attribute.

    Returns
    -------
    nx.Graph or nx.DiGraph
    """
    directed = _is_directed(result.meta)

    if result.edges.empty:
        return nx.DiGraph() if directed else nx.Graph()

    G = nx.DiGraph() if directed else nx.Graph()

    # Add edges
    if weighted and "weight" in result.edges.columns:
        for _, row in result.edges.iterrows():
            G.add_edge(row["src"], row["dst"], weight=row["weight"])
    else:
        for _, row in result.edges.iterrows():
            G.add_edge(row["src"], row["dst"])

    # Add node metadata if available
    if result.nodes is not None and not result.nodes.empty:
        for _, row in result.nodes.iterrows():
            node_id = row["node_id"]
            attrs = row.drop("node_id").to_dict()
            # .update() is safe even if the node was auto-created by edges
            G.nodes[node_id].update(attrs)

    return G


def to_networkx_sample(
    edges: pd.DataFrame,
    directed: bool = False,
    weighted: bool = True,
    n: int = 10_000,
) -> nx.Graph:
    """
    Quickly sample a subset of edges and convert to a NetworkX graph.

    Parameters
    ----------
    edges : pd.DataFrame
        DataFrame with columns src, dst[, weight].
    directed : bool, default=False
        Whether to use DiGraph or Graph.
    weighted : bool, default=True
        Whether to include edge weights.
    n : int, default=10000
        Number of edges to sample.

    Returns
    -------
    nx.Graph or nx.DiGraph
    """
    if edges.empty:
        return nx.DiGraph() if directed else nx.Graph()

    df = edges.sample(n=min(len(edges), n), random_state=42)
    G = nx.DiGraph() if directed else nx.Graph()

    if weighted and "weight" in df.columns:
        for _, row in df.iterrows():
            G.add_edge(row["src"], row["dst"], weight=row["weight"])
    else:
        for _, row in df.iterrows():
            G.add_edge(row["src"], row["dst"])

    return G
