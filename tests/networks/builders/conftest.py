import pytest


@pytest.fixture
def _Spec():
    """Provide a simple `_Spec` class for builder tests.

    Tests call `_Spec(...)` to create lightweight spec objects used by builders.
    """

    class _Spec:
        def __init__(self, filters=None, edge_kind="comment", weighting="count"):
            self.filters = filters or {}
            self.edge_kind = edge_kind
            self.weighting = weighting

    return _Spec
