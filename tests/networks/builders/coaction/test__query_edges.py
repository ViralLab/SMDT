import pandas as pd

import smdt.networks.base as base_mod
from smdt.networks.builders.coaction import CoActionNetworkBuilder


def test_query_edges_sets_edge_type_and_binary(
    monkeypatch, Weighted_network_spec
) -> None:
    """_query_edges should set binary weights and CO_<ACTION> edge_type."""
    df = pd.DataFrame({"src": [1], "dst": [2], "weight": [7]})

    def fake_super(self):
        return df.copy()

    monkeypatch.setattr(base_mod.NetworkBuilder, "_query_edges", fake_super)

    spec = Weighted_network_spec(edge_kind="share")
    b = CoActionNetworkBuilder(db=None, spec=spec)
    result = b._query_edges()
    assert (result["weight"] == 1).all()
    assert result["edge_type"].iloc[0] == "CO_SHARE"
