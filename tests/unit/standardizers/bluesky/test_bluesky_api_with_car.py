from datetime import datetime
import pytest

from smdt.standardizers.bluesky import bluesky_api_with_car as carmod
from smdt.standardizers.base import SourceInfo
from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities, EntityType
from smdt.store.models.actions import ActionType


@pytest.mark.parametrize(
    "ts,expect_not_none",
    [
        ("2023-01-01T00:00:00Z", True),
        (None, False),
    ],
)
def test_parse_ts(ts, expect_not_none):
    """parse_ts should return a datetime for valid timestamps and None for None input."""
    res = carmod.parse_ts(ts)
    if expect_not_none:
        assert isinstance(res, datetime)
    else:
        assert res is None


def test_get_record_created_at():
    """get_record_created_at should return the createdAt string from a record."""
    rec = {"createdAt": "2023-02-02T12:00:00Z"}
    assert carmod.get_record_created_at(rec) == "2023-02-02T12:00:00Z"


def test_find_created_at_recursive():
    """find_created_at should locate createdAt nested inside structures."""
    nested = {"a": {"b": [{"createdAt": "2020-01-01T00:00:00Z"}]}}
    assert carmod.find_created_at(nested) == "2020-01-01T00:00:00Z"


def test_safe_parse_created_at():
    """safe_parse_created_at should parse createdAt when present and return a datetime-like object."""
    rec = {"createdAt": "2023-02-02T12:00:00Z"}
    assert carmod.safe_parse_created_at(rec) is not None


@pytest.mark.parametrize(
    "entries,index,expected,exc",
    [
        # first entry with p==0 should decode directly for index 0
        ([{"p": 0, "k": b"first"}], 0, "first", None),
        # second entry should use prefix of previous (first) up to 3 chars + suffix
        ([{"p": 0, "k": b"first"}, {"p": 3, "k": b"XYZ"}], 1, "firXYZ", None),
        # if first entry has non-zero p, ValueError
        ([{"p": 2, "k": b"x"}], 0, None, ValueError),
    ],
)
def test_reconstruct_key(entries, index, expected, exc):
    if exc:
        with pytest.raises(exc):
            carmod.reconstruct_key(entries, entries[index])
    else:
        assert carmod.reconstruct_key(entries, entries[index]) == expected


def test_get_mapper_with_malformed_file(tmp_path):
    """Ensure get_mapper returns a dict even if the CAR text is malformed / does not match expected lines."""
    f = tmp_path / "test.car"
    # write lines that do not match expected block so parsing yields empty mapping
    f.write_text("# comment\nnot a valid line\n")
    res = carmod.get_mapper(str(f))
    assert isinstance(res, dict)


def test_extract_text_urls():
    """Test extraction of link facets into URLs list."""
    rec = {
        "facets": [
            {"features": [{"$type": "app.bsky.richtext.facet#link", "uri": "http://x"}]}
        ]
    }
    assert carmod.extract_text_urls(rec) == ["http://x"]


def test_extract_target_from_uri():
    """Test parsing of at:// URIs into account DID and post id."""
    acc, post = carmod.extract_target_from_uri("at://did:me/app.bsky.feed.post/ppp")
    assert acc.startswith("did:") and post == "ppp"


def test_BlueSkyAPICARStandardizer_all_users_branch_missing_createdAt():
    """Missing createdAt in all_users.jsonl should produce no output."""
    std = carmod.BlueSkyAPICARStandardizer()
    src = SourceInfo(path="all_users.jsonl")
    assert std.standardize(({}, src)) == []


def test_BlueSkyAPICARStandardizer_all_users_branch_valid_user():
    """Valid all_users.jsonl record should produce an Accounts object."""
    std = carmod.BlueSkyAPICARStandardizer()
    src = SourceInfo(path="all_users.jsonl")
    rec = {"did": "did:plc:abc", "handle": "bob", "createdAt": "2022-01-02T03:04:05Z"}
    out = std.standardize((rec, src))
    assert any(isinstance(o, Accounts) for o in out)


def test_BlueSkyAPICARStandardizer_cars_post_with_reply(monkeypatch):
    """Post with reply should produce a Posts object and COMMENT action."""
    std = carmod.BlueSkyAPICARStandardizer()
    monkeypatch.setattr(
        carmod,
        "get_mapper",
        lambda _: {"cid-1": "did-plc-abc/app.bsky.feed.post/post123"},
    )
    src = SourceInfo(path="/some/path/cars/did-plc-abc.jsonl")
    record = {
        "$type": "app.bsky.feed.post",
        "enriched_cid": "cid-1",
        "text": "hi @u",
        "reply": {
            "parent": {"uri": "at://did:target/app.bsky.feed.post/t1"},
            "root": {"uri": "at://did:root/app.bsky.feed.post/root1"},
        },
        "createdAt": "2023-03-03T00:00:00Z",
    }
    result = std.standardize((record, src))
    assert any(isinstance(o, Posts) for o in result)
    assert any(getattr(o, "action_type", None) == ActionType.COMMENT for o in result)


def test_BlueSkyAPICARStandardizer_cars_embed_record_quote(monkeypatch):
    """Embed record should produce a QUOTE action."""
    std = carmod.BlueSkyAPICARStandardizer()
    monkeypatch.setattr(
        carmod,
        "get_mapper",
        lambda _: {"cid-1": "did-plc-abc/app.bsky.feed.post/post123"},
    )
    src = SourceInfo(path="/some/path/cars/did-plc-abc.jsonl")
    record = {
        "$type": "app.bsky.feed.post",
        "enriched_cid": "cid-1",
        "embed": {
            "$type": "app.bsky.embed.record",
            "record": {"uri": "at://did:someone/app.bsky.feed.post/p2"},
        },
        "createdAt": "2023-03-04T00:00:00Z",
    }
    result = std.standardize((record, src))
    assert any(getattr(o, "action_type", None) == ActionType.QUOTE for o in result)


def test_BlueSkyAPICARStandardizer_cars_images_embed_entities_image(monkeypatch):
    """Images embed should produce Entities with IMAGE type."""
    std = carmod.BlueSkyAPICARStandardizer()
    monkeypatch.setattr(
        carmod,
        "get_mapper",
        lambda path: {"cid-1": "did-plc-abc/app.bsky.feed.post/post123"},
    )
    src = SourceInfo(path="/some/path/cars/did-plc-abc.jsonl")
    record = {
        "$type": "app.bsky.feed.post",
        "enriched_cid": "cid-1",
        "embed": {
            "$type": "app.bsky.embed.images",
            "images": [{"fullsize": "https://img/1.jpg"}],
        },
        "createdAt": "2023-03-05T00:00:00Z",
    }
    result = std.standardize((record, src))
    assert any(
        isinstance(o, Entities) and o.entity_type == EntityType.IMAGE for o in result
    )


def test_BlueSkyAPICARStandardizer_like_upvote(monkeypatch):
    """Like record should produce an UPVOTE action."""
    std = carmod.BlueSkyAPICARStandardizer()
    monkeypatch.setattr(
        carmod,
        "get_mapper",
        lambda _: {"cid-1": "did-plc-abc/app.bsky.feed.post/post123"},
    )
    src = SourceInfo(path="/some/path/cars/did-plc-abc.jsonl")
    record = {
        "$type": "app.bsky.feed.like",
        "createdAt": "2023-03-06T00:00:00Z",
        "subject": {"uri": "at://did:target/app.bsky.feed.post/tx"},
    }
    result = std.standardize((record, src))
    assert any(getattr(o, "action_type", None) == ActionType.UPVOTE for o in result)


def test_BlueSkyAPICARStandardizer_repost_share(monkeypatch):
    """Repost (share) should produce a SHARE action when enriched_cid mapping exists."""
    std = carmod.BlueSkyAPICARStandardizer()
    monkeypatch.setattr(
        carmod,
        "get_mapper",
        lambda _: {"cid-1": "did-plc-abc/app.bsky.feed.post/post123"},
    )
    src = SourceInfo(path="/some/path/cars/did-plc-abc.jsonl")
    record = {
        "$type": "app.bsky.feed.repost",
        "enriched_cid": "cid-1",
        "createdAt": "2023-03-07T00:00:00Z",
        "subject": {"uri": "at://did:target/app.bsky.feed.post/r1"},
    }
    result = std.standardize((record, src))
    assert any(getattr(o, "action_type", None) == ActionType.SHARE for o in result)


@pytest.mark.parametrize(
    "rtype,atype",
    [
        ("app.bsky.graph.follow", ActionType.FOLLOW),
        ("app.bsky.graph.block", ActionType.BLOCK),
    ],
)
def test_graph_follow_block_actions(rtype, atype, monkeypatch):
    """Parametrized test for graph follow/block actions: ensure an Action of expected type is produced when subject is present."""
    # replicate minimal environment used by other CAR-based tests
    std = carmod.BlueSkyAPICARStandardizer()
    monkeypatch.setattr(
        carmod,
        "get_mapper",
        lambda path: {"cid-1": "did-plc-abc/app.bsky.feed.post/post123"},
    )
    src = SourceInfo(path="/some/path/cars/did-plc-abc.jsonl")

    record = {
        "$type": rtype,
        "createdAt": "2023-03-06T00:00:00Z",
        "subject": "at://did:target/app.bsky.feed.post/tx",
    }
    result = std.standardize((record, src))
    assert any(getattr(o, "action_type", None) == atype for o in result)
