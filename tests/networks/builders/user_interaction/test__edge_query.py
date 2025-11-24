import pytest

from smdt.networks.builders.user_interaction import UserInteractionNetworkBuilder


def test_edge_query_includes_action_type_upper(Network_spec) -> None:
    """_edge_query should include uppercased action_type in params for user interactions."""
    spec = Network_spec(edge_kind="Follow")
    b = UserInteractionNetworkBuilder(db=None, spec=spec)
    result_sql, result_params = b._edge_query()
    assert result_params["action_type"] == "FOLLOW"
    assert "originator_account_id" in result_sql
