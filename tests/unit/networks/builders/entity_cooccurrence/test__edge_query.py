from smdt.networks.builders.entity_cooccurrence import EntityCooccurrenceNetworkBuilder


def test_edge_query_uses_upper_entity_type(Weighted_network_spec) -> None:
    """_edge_query should uppercase entity_type from filters into params."""
    spec = Weighted_network_spec(filters={"entity_type": "Hashtag"})
    b = EntityCooccurrenceNetworkBuilder(db=None, spec=spec)
    result_sql, result_params = b._edge_query()
    assert result_params["entity_type"] == "HASHTAG"
    assert "filtered_entities" in result_sql
