from smdt.standardizers.bluesky import bluesky_dataset as ds
from smdt.standardizers.base import SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import ActionType
import pytest


@pytest.mark.parametrize(
    "input_dt, expect_not_none",
    [
        ("202301011200", True),  # compact yyyymmddHHMM
        ("2023-01-01 12:00:00", True),
        (None, False),
    ],
)
def test_dt_variants(input_dt, expect_not_none):
    """_dt should parse known datetime string formats and return None for None input."""
    result = ds._dt(input_dt)
    if expect_not_none:
        assert result is not None
    else:
        assert result is None


@pytest.mark.parametrize(
    "src, expected_kind",
    [
        (SourceInfo(path="feed_posts.json", member="feed_posts.json"), "posts"),
        (
            SourceInfo(path="interactions.tsv", member="interactions.tsv"),
            "interactions",
        ),
        (
            SourceInfo(path="feed_posts_likes.csv", member="feed_posts_likes.csv"),
            "likes",
        ),
    ],
)
def test_member_kind_variants(src, expected_kind):
    """_member_kind should infer the member kind from SourceInfo path/member."""
    result = ds._member_kind(src)
    assert result == expected_kind


@pytest.mark.parametrize(
    "metrics, keys, expected",
    [
        ({"a": 1, "b": 2}, ["a", "b"], 3),
        (None, ["a"], None),
    ],
)
def test_sum_metrics(metrics, keys, expected):
    """_sum_metrics should return the sum of listed metric keys or None when metrics is None."""
    result = ds._sum_metrics(metrics, keys)
    assert result == expected


def test_posts_branch_extracts_entities():
    """Standardizing a post record should produce Accounts, Posts and Entities (hashtag, mention, link, email)."""
    src = SourceInfo(path="feed_posts.json", member="feed_posts.json")
    rec = {
        "post_id": 1,
        "user_id": 42,
        "text": "Hello #tag @bob http://example.com a@b.com",
        "date": "2023-01-01 00:00:00",
        "like_count": 5,
        "reply_count": 1,
        "repost_count": 0,
    }
    result = ds.BlueSkyDatasetStandardizer().standardize((rec, src))
    assert any(isinstance(o, Accounts) for o in result)
    assert any(isinstance(o, Posts) for o in result)
    ents = [o for o in result if isinstance(o, Entities)]
    types = {e.entity_type for e in ents}
    assert EntityType.HASHTAG in types
    assert EntityType.USER_TAG in types
    assert EntityType.LINK in types
    assert EntityType.EMAIL in types


def test_interactions_list_parses_comment():
    """A list-like interaction row should produce Accounts and a COMMENT action."""
    src = SourceInfo(path="interactions.tsv", member="interactions.tsv")
    row = ["1", "2", "3", "", "", "202301011200"]
    result = ds.BlueSkyDatasetStandardizer().standardize((row, src))
    assert any(isinstance(o, Accounts) for o in result)
    assert any(getattr(o, "action_type", None) == ActionType.COMMENT for o in result)


def test_interactions_mapping_raises_attribute_error():
    """A mapping-style interaction row with None action_type should raise AttributeError in current implementation."""
    src = SourceInfo(path="interactions.tsv", member="interactions.tsv")
    mapping_row = {0: "5", 1: "None", 2: "7", 3: "", 4: "", 5: "2023-01-01 12:00:00"}
    with pytest.raises(AttributeError):
        # call once and expect an AttributeError
        ds.BlueSkyDatasetStandardizer().standardize((mapping_row, src))


@pytest.mark.parametrize(
    "row, expect_upvote",
    [
        (
            ["10", "20", "p1", "2023-01-02 00:00:00"],
            True,
        ),  # list-like should yield UPVOTE
        (
            {0: "11", 1: "21", 2: "p2", 3: "2023-01-03 00:00:00"},
            False,
        ),  # mapping-like: only check accounts present
    ],
)
def test_likes_branch_list_and_mapping(row, expect_upvote):
    """Like rows (list or mapping) should produce Accounts; list-like should include UPVOTE actions."""
    src = SourceInfo(path="feed_posts_likes.csv", member="feed_posts_likes.csv")
    result = ds.BlueSkyDatasetStandardizer().standardize((row, src))
    assert any(isinstance(o, Accounts) for o in result)
    if expect_upvote:
        assert any(getattr(o, "action_type", None) == ActionType.UPVOTE for o in result)


def test_unknown_kind_fallback_parses_post_shape():
    """Unknown member kinds should fall back to parsing as post-like records producing Posts and Accounts."""
    src = SourceInfo(path="random.json", member="random.json")
    rec = {
        "id": "9",
        "author": "7",
        "text": "hi #x @y http://z e@f.com",
        "date": "2023-01-05",
    }
    result = ds.BlueSkyDatasetStandardizer().standardize((rec, src))
    assert any(isinstance(o, Posts) for o in result)
    assert any(isinstance(o, Accounts) for o in result)
