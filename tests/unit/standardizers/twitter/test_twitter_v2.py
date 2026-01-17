from datetime import datetime, timezone
from typing import Any, Mapping

import pytest

from smdt.standardizers.twitter.twitter_v2 import (
    _dt,
    _point_ewkt,
    map2int,
    _find_user_includes,
    _account_from_user,
    TwitterV2Standardizer,
)
from smdt.standardizers.base import SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities
from smdt.store.models.actions import Actions


@pytest.fixture
def twitter_std() -> TwitterV2Standardizer:
    return TwitterV2Standardizer()


def test_dt_parses_utc():
    """_dt should parse an ISO datetime in UTC and return a datetime object."""
    result = _dt("2023-01-01T00:00:00Z")
    assert result is not None


def test_dt_parses_with_offset():
    """_dt should parse an ISO datetime with a timezone offset and preserve tzinfo."""
    result = _dt("2023-01-01T00:00:00+02:00")
    assert result.tzinfo is not None


def test_dt_returns_none_for_none():
    """_dt should return None when given None as input."""
    result = _dt(None)
    assert result is None


@pytest.mark.parametrize("input_value,expected", [("5", 5), ("nan", None)])
def test_map2int_parametrized(input_value, expected):
    """map2int should convert numeric strings to int and return None for non-numeric values."""
    result = map2int(input_value)
    assert result == expected


def test_point_ewkt_single():
    """_point_ewkt should convert a place object with nested coordinates into EWKT."""
    place = {"coordinates": {"coordinates": [10.0, 20.0]}}
    result = _point_ewkt(place)
    assert result == "SRID=4326;POINT(10.0 20.0)"


def test_find_user_includes_single():
    """_find_user_includes should find a user dict by id inside an includes block."""
    rec = {
        "includes": {
            "users": [
                {"id": "u1", "created_at": "2020-01-01T00:00:00Z", "username": "alice"}
            ]
        }
    }
    result = _find_user_includes(rec, "u1")
    assert result and result.get("username") == "alice"


def test_account_from_user_single():
    """_account_from_user should convert a user include into an Accounts model instance."""
    u = {"id": "u1", "username": "alice", "created_at": "2020-01-01T00:00:00Z"}
    now = datetime.now(timezone.utc)
    result = _account_from_user(u, now)
    assert result is not None
    assert result.username == "alice"


def test_twitter_v2_standardize_roundtrip(twitter_std: TwitterV2Standardizer):
    """TwitterV2Standardizer.standardize should produce Accounts, Posts, Entities, and Actions from a minimal record."""
    # Arrange: minimal V2-style tweet record with includes for author and referenced tweet
    record: Mapping[str, Any] = {
        "data": {
            "id": "t1",
            "author_id": "u1",
            "created_at": "2023-01-01T12:00:00Z",
            "text": "Hello world #tag @other https://example.com contact@test.com",
            "public_metrics": {
                "like_count": 5,
                "retweet_count": 1,
                "reply_count": 0,
                "quote_count": 0,
                "impression_count": 100,
            },
            "referenced_tweets": [
                {
                    "type": "replied_to",
                    "tweet": {
                        "id": "t0",
                        "author_id": "u0",
                        "created_at": "2022-12-31T11:00:00Z",
                        "text": "orig",
                    },
                }
            ],
        },
        "includes": {
            "users": [
                {
                    "id": "u1",
                    "username": "alice",
                    "name": "Alice",
                    "created_at": "2020-01-01T00:00:00Z",
                    "public_metrics": {
                        "tweet_count": 10,
                        "following_count": 2,
                        "followers_count": 3,
                    },
                    "verified": False,
                },
                {
                    "id": "u0",
                    "username": "bob",
                    "name": "Bob",
                    "created_at": "2019-01-01T00:00:00Z",
                    "public_metrics": {
                        "tweet_count": 5,
                        "following_count": 1,
                        "followers_count": 2,
                    },
                    "verified": False,
                },
            ],
            "tweets": [
                {
                    "id": "t0",
                    "author_id": "u0",
                    "created_at": "2022-12-31T11:00:00Z",
                    "text": "orig text",
                    "public_metrics": {
                        "like_count": 2,
                        "retweet_count": 0,
                        "reply_count": 0,
                        "quote_count": 0,
                        "impression_count": 50,
                    },
                }
            ],
        },
    }

    src = SourceInfo(path="twitter.jsonl")

    # Act
    result = list(twitter_std.standardize((record, src)))

    # Assert: we should have at least one Accounts, one Posts, Entities for hashtag/mention/link/email, and an Action
    assert any(isinstance(o, Accounts) for o in result)
    assert any(isinstance(o, Posts) for o in result)
    assert any(isinstance(o, Entities) for o in result)
    assert any(isinstance(o, Actions) for o in result)

    # Find the main post and check fields
    posts = [o for o in result if isinstance(o, Posts)]
    main_post = next((p for p in posts if p.post_id == "t1"), None)
    assert main_post is not None
    assert main_post.account_id == "u1"
    assert main_post.like_count == 5
    # 'view_count' may be mapped to 'impression_count' in the standardizer; assert the numeric value is present
    assert getattr(main_post, "view_count", None) == 100
