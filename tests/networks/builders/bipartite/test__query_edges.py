import pandas as pd

from smdt.networks.builders.bipartite import BipartiteNetworkBuilder
import smdt.networks.base as base_mod


def test_query_edges_binary_and_edge_type(monkeypatch, Weighted_network_spec) -> None:
    """_query_edges should set binary weights and construct edge_type."""
    # Prepare fake DataFrame returned by NetworkBuilder._query_edges
    df = pd.DataFrame({"src": [1, 2], "dst": ["a", "b"], "weight": [5, 3]})

    def fake_super_query(self):
        return df.copy()

    monkeypatch.setattr(base_mod.NetworkBuilder, "_query_edges", fake_super_query)

    spec = Weighted_network_spec()
    b = BipartiteNetworkBuilder(db=None, spec=spec)
    result = b._query_edges()
    # binary weighting should set all weights to 1
    assert (result["weight"] == 1).all()
    assert "BIPARTITE" in result["edge_type"].iloc[0]
