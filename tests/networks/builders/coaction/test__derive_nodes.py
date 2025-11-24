import pandas as pd

from smdt.networks.builders.coaction import CoActionNetworkBuilder


def test_derive_nodes_returns_unique_node_ids(Network_spec) -> None:
    """_derive_nodes should aggregate unique src and dst into node_id column."""
    edges = pd.DataFrame({"src": [1, 2], "dst": [2, 3], "weight": [1, 1]})
    spec = Network_spec()
    b = CoActionNetworkBuilder(db=None, spec=spec)
    result = b._derive_nodes(edges)
    assert set(result["node_id"]) == {1, 2, 3}
    assert (result["type"] == "ACCOUNT").all()
