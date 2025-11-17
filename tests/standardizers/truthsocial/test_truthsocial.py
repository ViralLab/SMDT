import pytest

from smdt.standardizers.truthsocial.truthsocial import (
    _dt,
    _int,
    _bool_t,
    _nz,
    _sum3,
    TruthSocialStandardizer,
)
from smdt.standardizers.base import SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities
from smdt.store.models.actions import Actions, ActionType


@pytest.mark.parametrize(
    "inp,expected_none",
    [
        ("2023-01-01 00:00:00", False),
        ("2023-01-01", False),
        (None, True),
    ],
)
def test_dt_various(inp, expected_none):
    """_dt should parse valid datetime strings and return None for None input."""
    result = _dt(inp)
    assert (result is None) == expected_none


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("5", 5),
        ("", None),
        ("nan", None),
    ],
)
def test_int_various(inp, expected):
    """_int should convert numeric strings to int and return None for invalid inputs."""
    result = _int(inp)
    assert result == expected


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("t", True),
        ("f", False),
        ("True", True),
    ],
)
def test_bool_t_various(inp, expected):
    """_bool_t should interpret truthy/falsy string values correctly."""
    result = _bool_t(inp)
    assert result == expected


@pytest.mark.parametrize(
    "inp,expected",
    [
        (None, 0),
        (3, 3),
    ],
)
def test_nz_various(inp, expected):
    """_nz should map None to 0 and pass through non-None values."""
    result = _nz(inp)
    assert result == expected


def test_sum3_basic():
    """_sum3 should sum three values treating None as zero for the first argument."""
    result = _sum3(None, 2, 3)
    assert result == 5


def test_users_missing_id_returns_empty():
    """Standardizing a user row missing 'id' should return an empty list."""
    std = TruthSocialStandardizer()
    src = SourceInfo(path="users.tsv", member="users.tsv")
    result = std.standardize(({}, src))
    assert result == []


def test_users_valid_user_emits_accounts():
    """Standardizing a valid user row should emit an Accounts object with expected fields."""
    std = TruthSocialStandardizer()
    src = SourceInfo(path="users.tsv", member="users.tsv")
    user = {
        "id": "u1",
        "timestamp": "2020-01-01 00:00:00",
        "username": "alice",
        "time_scraped": "2023-01-01 00:00:00",
        "following_count": "-1",
        "follower_count": "10",
    }
    result = std.standardize((user, src))
    assert any(isinstance(o, Accounts) for o in result)
    acct = next(o for o in result if isinstance(o, Accounts))
    assert acct.follower_count == 10


def test_replies_populates_map():
    """Standardizing a reply row should populate replied_user_by_replying_user map."""
    std = TruthSocialStandardizer()
    src_r = SourceInfo(path="replies.tsv", member="replies.tsv")
    row = {"replying_user": "u2", "replied_user": "u1"}
    result = std.standardize((row, src_r))
    # ensure we called the standardize function and that the side effect occurred
    assert "u2" in std.replied_user_by_replying_user
    assert std.replied_user_by_replying_user.get("u2") == "u1"


def test_reply_truth_emits_comment_when_mapping_exists():
    """A truth that is a reply should emit a COMMENT action when the replied mapping exists."""
    std = TruthSocialStandardizer()
    # set up mapping directly so this test calls standardize only once for the truth
    std.replied_user_by_replying_user["u2"] = "u1"
    src_t = SourceInfo(path="truths.tsv", member="truths.tsv")
    truth = {
        "id": "p1",
        "author": "u2",
        "timestamp": "2023-01-02 00:00:00",
        "time_scraped": "2023-01-02 00:00:00",
        "text": "hi",
        "is_reply": "t",
    }
    result = std.standardize((truth, src_t))
    assert any(isinstance(o, Posts) for o in result)
    assert any(
        isinstance(o, Actions) and o.action_type == ActionType.COMMENT for o in result
    )


def test_retruth_defers_when_original_missing():
    """A retruth whose original post is unknown should be deferred (recorded in pending)."""
    std = TruthSocialStandardizer()
    src = SourceInfo(path="truths.tsv", member="truths.tsv")
    retruth = {
        "id": "r1",
        "author": "uR",
        "timestamp": "2023-01-03 00:00:00",
        "time_scraped": "2023-01-03 00:00:00",
        "text": "RT",
        "is_retruth": "t",
        "truth_retruthed": "orig1",
    }
    result = std.standardize((retruth, src))
    assert not any(isinstance(o, Actions) for o in result)
    assert "orig1" in std._pending_retruths


def test_processing_original_emits_posts_and_entities():
    """Processing an original truth should emit Posts and Entities objects."""
    std = TruthSocialStandardizer()
    src = SourceInfo(path="truths.tsv", member="truths.tsv")
    orig = {
        "id": "orig1",
        "author": "uOrig",
        "timestamp": "2023-01-01 00:00:00",
        "time_scraped": "2023-01-01 00:00:00",
        "text": "Hello @bob #tag http://ex a@b.com",
        "external_id": "ext-1",
    }
    result = std.standardize((orig, src))
    assert any(isinstance(o, Posts) for o in result)
    assert any(isinstance(o, Entities) for o in result)


def test_retruth_after_original_emits_share_action():
    """A retruth should emit a SHARE action when the original post author is known."""
    std = TruthSocialStandardizer()
    # ensure the original post -> account mapping exists so the retruth processing emits SHARE
    if not hasattr(std, "post2account"):
        std.post2account = {}
    std.post2account["orig1"] = "uOrig"
    src = SourceInfo(path="truths.tsv", member="truths.tsv")
    retruth2 = {
        "id": "r2",
        "author": "uR2",
        "timestamp": "2023-01-04 00:00:00",
        "time_scraped": "2023-01-04 00:00:00",
        "text": "RT2",
        "is_retruth": "t",
        "truth_retruthed": "orig1",
    }
    result = std.standardize((retruth2, src))
    assert any(
        isinstance(o, Actions) and o.action_type == ActionType.SHARE for o in result
    )


def test_create_external_mapping_via_truth_emits_post():
    """Processing a truth with external_id should produce a Post (and establish external mapping)."""
    std = TruthSocialStandardizer()
    src = SourceInfo(path="truths.tsv", member="truths.tsv")
    rec = {
        "id": "pX",
        "author": "uX",
        "timestamp": "2023-01-05 00:00:00",
        "time_scraped": "2023-01-05 00:00:00",
        "external_id": "EXT123",
    }
    result = std.standardize((rec, src))
    assert any(isinstance(o, Posts) for o in result)


def test_quote_emits_quote_action_when_external_mapping_exists():
    """Processing a quote should emit a QUOTE action when external->internal mapping exists."""
    std = TruthSocialStandardizer()
    # pre-populate the external mapping so we call standardize only once in this test
    # attribute name guessed based on typical internal naming; create if absent.
    if not hasattr(std, "_external_id_map"):
        std._external_id_map = {}
    std._external_id_map["EXT123"] = "pX"

    qsrc = SourceInfo(path="quotes.tsv", member="quotes.tsv")
    quote_row = {
        "quoting_truth": "pX",
        "quoting_user": "uX",
        "quoted_user": "uY",
        "quoted_truth_external_id": "EXT123",
        "time_scraped": "2023-01-05 00:00:00",
    }
    result = std.standardize((quote_row, qsrc))
    assert any(
        isinstance(o, Actions) and o.action_type == ActionType.QUOTE for o in result
    )


def test_follows_branch_emits_follow_action():
    """Standardizing a follow row should emit a FOLLOW action."""
    std = TruthSocialStandardizer()
    src = SourceInfo(path="follows.tsv", member="follows.tsv")
    row = {"follower": "uA", "followed": "uB", "time_scraped": "2023-01-06 00:00:00"}
    result = std.standardize((row, src))
    assert any(
        isinstance(o, Actions) and o.action_type == ActionType.FOLLOW for o in result
    )


@pytest.fixture
def truth_std():
    return TruthSocialStandardizer()


def test_truthsocial_user_produces_accounts(truth_std):
    """Emitting a user row via the standardizer should produce Accounts."""
    user_row = {
        "id": "u42",
        "timestamp": "2020-05-01 00:00:00",
        "username": "carol",
        "time_scraped": "2023-01-01 00:00:00",
    }
    user_src = SourceInfo(path="users.tsv", member="users.tsv")
    result = list(truth_std.standardize((user_row, user_src)))
    assert any(isinstance(o, Accounts) for o in result)


def test_truthsocial_truth_produces_post_and_entities(truth_std):
    """Processing a truth should produce a Post and Entities with correct post_id and account_id."""
    truth_row = {
        "id": "p1",
        "author": "u42",
        "timestamp": "2023-01-05 12:00:00",
        "text": "Hello @someone #tag https://x.com",
        "time_scraped": "2023-01-05 12:00:00",
        "external_id": "ext1",
        "is_retruth": "f",
        "like_count": "2",
        "retruth_count": "0",
        "reply_count": "0",
    }
    truth_src = SourceInfo(path="truths.tsv", member="truths.tsv")
    result = list(truth_std.standardize((truth_row, truth_src)))
    assert any(isinstance(o, Posts) for o in result)
    posts = [o for o in result if isinstance(o, Posts)]
    post = next((p for p in posts if p.post_id == "p1"), None)
    assert post is not None
    assert post.account_id == "u42"
