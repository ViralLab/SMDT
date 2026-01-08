import pandas as pd

from smdt.networks.builders.user_interaction import UserInteractionNetworkBuilder


def test_no_derive_nodes_override_default(Network_spec) -> None:
    """UserInteractionNetworkBuilder should inherit default _derive_nodes behavior when not overridden."""
    edges = pd.DataFrame({"src": [1, 2], "dst": [3, 4], "weight": [1, 1]})
    spec = Network_spec()
    b = UserInteractionNetworkBuilder(db=None, spec=spec)
    result = b._derive_nodes(edges)
    assert set(result["node_id"]) == {1, 2, 3, 4}
