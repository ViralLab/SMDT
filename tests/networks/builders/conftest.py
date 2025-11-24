import pytest


@pytest.fixture(autouse=True)
def Network_spec():
    class _Spec:
        def __init__(self, edge_kind="comment"):
            self.edge_kind = edge_kind
            self.filters = {}

    return _Spec


@pytest.fixture(autouse=True)
def Weighted_network_spec():
    class _Spec:
        def __init__(self, weighting="binary", edge_kind="comment", filters=None):
            self.edge_kind = edge_kind
            self.weighting = weighting
            self.filters = filters or {"left": "account", "right": "hashtag"}

    return _Spec
