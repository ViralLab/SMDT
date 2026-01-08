from smdt.networks.builders.coaction import CoActionNetworkBuilder


def test_edge_query_contains_action_type_upper(Network_spec) -> None:
    """_edge_query should include the uppercased action_type in params."""
    spec = Network_spec(edge_kind="Comment")
    b = CoActionNetworkBuilder(db=None, spec=spec)
    result_sql, result_params = b._edge_query()
    assert result_params["action_type"] == "COMMENT"
    assert "filtered_actions" in result_sql
