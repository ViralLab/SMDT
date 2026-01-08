import pytest

from smdt.standardizers.twitter.twitter_USC import (
    nan_to_none,
    map2int,
    _dt,
    TwitterUSCStandardizer,
)
from smdt.standardizers.base import SourceInfo
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities


@pytest.mark.parametrize(
    "inp, expected",
    [
        (None, None),
        ("nan", None),
        (5, 5),
    ],
)
def test_nan_to_none(inp, expected):
    """nan_to_none should convert "nan" and None-like values to None, otherwise return the value."""
    result = nan_to_none(inp)
    if expected is None:
        assert result is None
    else:
        assert result == expected


@pytest.mark.parametrize(
    "inp, expected",
    [
        ("4", 4),
        ("nan", None),
    ],
)
def test_map2int(inp, expected):
    """map2int should convert numeric strings to int and 'nan' to None."""
    result = map2int(inp)
    if expected is None:
        assert result is None
    else:
        assert result == expected


def test_dt_parses_iso8601():
    """_dt should parse an ISO8601 timestamp string and return a non-None result."""
    result = _dt("2023-01-01T00:00:00Z")
    assert result is not None


def test_standardize_basic_post_and_entities_and_actions():
    """TwitterUSCStandardizer.standardize should produce Posts and Entities objects for a basic record."""
    std = TwitterUSCStandardizer()
    src = SourceInfo(path="usc.tsv", member="usc.tsv")
    rec = {
        "id_str": "p1",
        "epoch": "1609459200",
        "text": "hello @bob #tag http://x.com",
        "user__id_str": "u1",
        "user__created": "2020-01-01T00:00:00Z",
    }
    result = list(std.standardize((rec, src)))
    assert any(isinstance(o, Posts) for o in result)
    assert any(isinstance(o, Entities) for o in result)
