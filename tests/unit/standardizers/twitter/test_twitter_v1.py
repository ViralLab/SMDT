import pytest

from smdt.standardizers.twitter.twitter_v1 import (
    TwitterV1Standardizer,
    map2int,
    _point_ewkt,
)
from smdt.standardizers.base import SourceInfo
from smdt.store.models.posts import Posts
from smdt.store.models.accounts import Accounts


@pytest.mark.parametrize("input_value,expected", [("3", 3), (None, None)])
def test_map2int(input_value, expected):
    """map2int should convert numeric strings to int and return None for None."""
    assert map2int(input_value) == expected


@pytest.mark.parametrize(
    "coords,expected_ewkt",
    [([1.0, 2.0], "SRID=4326;POINT(1.0 2.0)"), ([0, 0], "SRID=4326;POINT(0.0 0.0)")],
)
def test_point_ewkt(coords, expected_ewkt):
    """_point_ewkt should produce the correct EWKT string for coordinate pairs."""
    geo = {"coordinates": coords}
    assert _point_ewkt(geo) == expected_ewkt


@pytest.mark.parametrize("expected_cls", [Accounts, Posts])
def test_standardize_produces_model(expected_cls):
    """standardize should produce instances of the expected model class for a minimal tweet record.

    This test is parametrized so each assertion checks a single expectation (one test -> one thing).
    """
    std = TwitterV1Standardizer()
    src = SourceInfo(path="tweet.json", member="tweet.json")
    record = {
        "id_str": "t1",
        "text": "hello",
        "created_at": "Wed Oct 10 20:19:24 +0000 2018",
        "user": {
            "id_str": "u1",
            "screen_name": "bob",
            "created_at": "Wed Oct 10 20:19:24 +0000 2018",
        },
        "entities": {"hashtags": [], "user_mentions": []},
    }
    out = std.standardize((record, src))
    assert any(isinstance(o, expected_cls) for o in out)
