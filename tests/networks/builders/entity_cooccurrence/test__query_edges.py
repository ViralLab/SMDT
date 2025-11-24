import pandas as pd

import smdt.networks.base as base_mod
from smdt.networks.builders.entity_cooccurrence import EntityCooccurrenceNetworkBuilder


def test_query_edges_sets_edge_type_and_binary(
    monkeypatch, Weighted_network_spec
) -> None:
    """_query_edges should set binary weights and add COOCCURRENCE suffix to edge_type."""
    df = pd.DataFrame({"src": ["a"], "dst": ["b"], "weight": [4]})

    def fake_super(self):
        return df.copy()

    monkeypatch.setattr(base_mod.NetworkBuilder, "_query_edges", fake_super)

    spec = Weighted_network_spec()
    b = EntityCooccurrenceNetworkBuilder(db=None, spec=spec)
    result = b._query_edges()
    assert (result["weight"] == 1).all()
    assert result["edge_type"].iloc[0].endswith("_COOCCURRENCE")
