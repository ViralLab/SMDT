import pytest
from smdt.standardizers.truthsocial.truthsocial_USC import (
    _sum_engagements,
    _map2int,
    TruthSocialUSCStandardizer,
)
from smdt.standardizers.base import SourceInfo
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.accounts import Accounts
from smdt.store.models.actions import Actions, ActionType


def test_sum_engagements():
    """sum_engagements should return the sum of engagement counts in the record."""
    rec = {"like_count": 2, "reply_count": 1, "retruth_count": 3}
    result = _sum_engagements(rec)
    assert result == 6


@pytest.mark.parametrize(
    "input_value, expected",
    [
        ("5", 5),
        ("x", None),
    ],
)
def test_map2int_parametrized(input_value, expected):
    """map2int should convert numeric strings to int and return None for non-numeric."""
    result = _map2int(input_value)
    assert result == expected


def test_standardizer_emits_post():
    """TruthSocialUSCStandardizer.standardize should emit a Posts instance for a valid record."""
    std = TruthSocialUSCStandardizer()
    ts = "2023-01-01T00:00:00"
    rec = {
        "url": "https://truthsocial.com/u/12345",
        "author_username": "@Alice",
        "status": "Hello #Tag @bob check@mail.com",
        "timestamp": ts,
        "associated_tags": "['#Tag']",
        "tagged_accounts": "['@bob']",
        "status_links": "['http://ex.com']",
        "media_urls": "['http://media/video.mp4']",
        "like_count": "2",
        "reply_count": "1",
        "retruth_count": "0",
        "is_reply": True,
        "replying_to": "['@target']",
    }
    result = list(std.standardize((rec, SourceInfo(path="usc.csv", member="usc.csv"))))
    assert any(isinstance(o, Posts) for o in result)


def test_standardizer_emits_expected_entity_types():
    """Standardizer should emit Entities with HASHTAG, EMAIL, USER_TAG, LINK, and VIDEO types."""
    std = TruthSocialUSCStandardizer()
    ts = "2023-01-01T00:00:00"
    rec = {
        "url": "https://truthsocial.com/u/12345",
        "author_username": "@Alice",
        "status": "Hello #Tag @bob check@mail.com",
        "timestamp": ts,
        "associated_tags": "['#Tag']",
        "tagged_accounts": "['@bob']",
        "status_links": "['http://ex.com']",
        "media_urls": "['http://media/video.mp4']",
        "like_count": "2",
        "reply_count": "1",
        "retruth_count": "0",
        "is_reply": True,
        "replying_to": "['@target']",
    }
    result = list(std.standardize((rec, SourceInfo(path="usc.csv", member="usc.csv"))))
    types = {o.entity_type for o in result if isinstance(o, Entities)}
    assert EntityType.HASHTAG in types
    assert EntityType.EMAIL in types
    assert EntityType.USER_TAG in types
    assert EntityType.LINK in types
    assert EntityType.VIDEO in types


def test_standardizer_creates_accounts():
    """Standardizer should create Accounts for the author or tagged accounts present in the record."""
    std = TruthSocialUSCStandardizer()
    ts = "2023-01-01T00:00:00"
    rec = {
        "url": "https://truthsocial.com/u/12345",
        "author_username": "@Alice",
        "status": "Hello #Tag @bob check@mail.com",
        "timestamp": ts,
        "associated_tags": "['#Tag']",
        "tagged_accounts": "['@bob']",
        "status_links": "['http://ex.com']",
        "media_urls": "['http://media/video.mp4']",
        "like_count": "2",
        "reply_count": "1",
        "retruth_count": "0",
        "is_reply": True,
        "replying_to": "['@target']",
    }
    result = list(std.standardize((rec, SourceInfo(path="usc.csv", member="usc.csv"))))
    assert any(isinstance(o, Accounts) for o in result)


def test_standardizer_emits_comment_action_when_reply_true():
    """When is_reply is True the standardizer should emit a COMMENT action."""
    std = TruthSocialUSCStandardizer()
    ts = "2023-01-01T00:00:00"
    rec = {
        "url": "https://truthsocial.com/u/12345",
        "author_username": "@Alice",
        "status": "Hello #Tag @bob check@mail.com",
        "timestamp": ts,
        "associated_tags": "['#Tag']",
        "tagged_accounts": "['@bob']",
        "status_links": "['http://ex.com']",
        "media_urls": "['http://media/video.mp4']",
        "like_count": "2",
        "reply_count": "1",
        "retruth_count": "0",
        "is_reply": True,
        "replying_to": "['@target']",
    }
    result = list(std.standardize((rec, SourceInfo(path="usc.csv", member="usc.csv"))))
    assert any(
        isinstance(o, Actions) and o.action_type == ActionType.COMMENT for o in result
    )


def test_missing_username_or_postid_returns_empty():
    """If url or author_username is missing the standardizer should return an empty list."""
    std = TruthSocialUSCStandardizer()
    rec = {"url": "", "author_username": ""}
    result = list(std.standardize((rec, SourceInfo(path="usc.csv", member="usc.csv"))))
    assert result == []
