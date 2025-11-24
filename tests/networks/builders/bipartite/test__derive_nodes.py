import pandas as pd

from smdt.networks.builders.bipartite import BipartiteNetworkBuilder


def test_derive_nodes_combines_left_and_right(Weighted_network_spec) -> None:
    """_derive_nodes should return distinct nodes with bipartite flags and types."""
    edges = pd.DataFrame({"src": [1, 2], "dst": ["a", "b"], "weight": [1, 1]})
    spec = Weighted_network_spec()
    b = BipartiteNetworkBuilder(db=None, spec=spec)
    result = b._derive_nodes(edges)
    assert set(result["node_id"]) == {1, 2, "a", "b"}
    assert "bipartite" in result.columns
