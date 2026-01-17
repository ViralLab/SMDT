import pytest


@pytest.fixture
def Network_spec():
    """Provide a simple spec class for builder tests (no weighting)."""

    class _Spec:
        def __init__(self, edge_kind="comment", filters=None):
            self.edge_kind = edge_kind
            self.filters = filters or {}

    return _Spec


@pytest.fixture
def Weighted_network_spec():
    """Provide a spec class with weighting for builder tests."""

    class _Spec:
        def __init__(self, weighting="binary", edge_kind="comment", filters=None):
            self.edge_kind = edge_kind
            self.weighting = weighting
            self.filters = filters or {"left": "account", "right": "hashtag"}

    return _Spec
