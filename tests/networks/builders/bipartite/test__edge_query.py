import pytest

from smdt.networks.builders.bipartite import BipartiteNetworkBuilder


def test_edge_query_account_hashtag(Weighted_network_spec) -> None:
    """_edge_query should produce SQL and params for account–hashtag bipartite."""
    spec = Weighted_network_spec(filters={"left": "account", "right": "hashtag"})
    b = BipartiteNetworkBuilder(db=None, spec=spec)
    result_sql, result_params = b._edge_query()
    assert "p.account_id" in result_sql
    assert result_params["entity_type"] == "HASHTAG"


def test_edge_query_unknown_right_raises(Weighted_network_spec) -> None:
    """_edge_query should raise NotImplementedError for unsupported right side."""
    spec = Weighted_network_spec(filters={"left": "account", "right": "unknown"})
    b = BipartiteNetworkBuilder(db=None, spec=spec)
    with pytest.raises(NotImplementedError):
        b._edge_query()
