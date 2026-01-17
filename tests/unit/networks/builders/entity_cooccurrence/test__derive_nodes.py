import pandas as pd

from smdt.networks.builders.entity_cooccurrence import EntityCooccurrenceNetworkBuilder


def test_derive_nodes_returns_labels_and_type(Network_spec) -> None:
    """_derive_nodes should return node_id, label and type columns for entities."""
    edges = pd.DataFrame({"src": ["a"], "dst": ["b"], "weight": [1]})
    spec = Network_spec()
    b = EntityCooccurrenceNetworkBuilder(db=None, spec=spec)
    result = b._derive_nodes(edges)
    assert list(result.columns) == ["node_id", "label", "type"]
    assert set(result["node_id"]) == {"a", "b"}
