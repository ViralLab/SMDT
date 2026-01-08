import pandas as pd

import smdt.networks.base as base_mod
from smdt.networks.builders.user_interaction import UserInteractionNetworkBuilder


def test_query_edges_sets_weight_and_edge_type(
    monkeypatch, Weighted_network_spec
) -> None:
    """_query_edges should set binary weight when requested and set edge_type to edge_kind upper."""
    df = pd.DataFrame({"src": [10], "dst": [20], "weight": [9]})

    def fake_super(self):
        return df.copy()

    monkeypatch.setattr(base_mod.NetworkBuilder, "_query_edges", fake_super)

    spec = Weighted_network_spec(edge_kind="quote")
    b = UserInteractionNetworkBuilder(db=None, spec=spec)
    result = b._query_edges()
    assert (result["weight"] == 1).all()
    assert result["edge_type"].iloc[0] == "QUOTE"
