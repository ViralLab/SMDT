from __future__ import annotations
import pytest

from smdt.standardizers.bluesky.bluesky_api import (
    BlueSkyAPIStandardizer,
    _s,
    _as_id,
    _dt_iso,
    _guess_kind,
    _post_id_from_uri,
    _did_from_uri,
    _emit_facets_entities,
    _emit_post_relationship_actions,
)
from smdt.store.models.actions import ActionType
from smdt.standardizers.base import SourceInfo
from smdt.store.models.posts import Posts
from smdt.store.models.entities import Entities
from smdt.store.models.actions import ActionType


@pytest.fixture
def bluesky_std():
    return BlueSkyAPIStandardizer()


def test_bluesky_post_and_entities(bluesky_std):
    """A Bluesky post with facets and embed should produce a Post and Entities (hashtag + image)."""
    # Arrange: a Bluesky post with facets and embed image
    record = {
        "$type": "app.bsky.feed.post",
        "author": "did:example:alice",
        "uri": "at://did:example:collection/rc1",
        "createdAt": "2023-01-02T10:00:00Z",
        "text": "Check this out #cool",
        "facets": [
            {"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "#cool"}]}
        ],
        "embed": {
            "$type": "app.bsky.embed.images",
            "images": [{"fullsize": "https://img.example/1.jpg"}],
        },
    }

    src = SourceInfo(path="stream.ndjson", member="feed.ndjson")

    # Act
    result = list(bluesky_std.standardize((record, src)))

    # Assert: expect a Post, at least one Entity (hashtag) and an Image entity
    assert any(isinstance(o, Posts) for o in result)
    entities = [o for o in result if isinstance(o, Entities)]
    assert any(e.entity_type for e in entities)
    assert any("img.example" in (e.body or "") for e in entities)


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("x", "x"),
        (None, ""),
        (123, "123"),
    ],
)
def test__s_helper(inp, expected):
    """_s should stringify values and return empty string for None."""
    assert _s(inp) == expected


@pytest.mark.parametrize(
    "inp,expected",
    [
        (None, None),
        ("", None),
        (" abc ", "abc"),
    ],
)
def test__as_id_helper(inp, expected):
    """_as_id should normalize id-like strings or return None for empty inputs."""
    assert _as_id(inp) == expected


@pytest.mark.parametrize(
    "inp,expected_ok",
    [
        ("2023-01-01T00:00:00Z", True),
        ("2023-01-01T00:00:00+02:00", True),
        ("2023-01-01T00:00:00.123Z", True),
        ("not-a-date", False),
        (None, False),
        ("", False),
    ],
)
def test__dt_iso(inp, expected_ok):
    """Parametrized checks for _dt_iso: valid ISO strings -> datetime w/ tz, invalid -> None."""
    dt = _dt_iso(inp)
    if expected_ok:
        assert dt is not None and dt.tzinfo is not None
    else:
        assert dt is None


@pytest.mark.parametrize(
    "src,rec,expected",
    [
        (SourceInfo(path="users.jsonl"), {"did": "d1", "handle": "h"}, "user"),
        (SourceInfo(path="feed.ndjson"), {"$type": "app.bsky.feed.post"}, "post"),
        (SourceInfo(path="x"), {"$type": "app.bsky.feed.repost"}, "repost"),
        (SourceInfo(path="x"), {"$type": "app.bsky.feed.like"}, "like"),
        (SourceInfo(path="x"), {"foo": "bar"}, "other"),
    ],
)
def test__guess_kind(src, rec, expected):
    """Parametrized checks for _guess_kind across multiple record types."""
    assert _guess_kind(src, rec) == expected


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("at://did:ex/col/abc123", "abc123"),
        (None, None),
        ("", None),
        ("at://did:ex/col/", None),
        ("not-a-uri", None),
    ],
)
def test__post_id_from_uri(inp, expected):
    """_post_id_from_uri: extract post id from at:// URIs or return None for invalid inputs."""
    assert _post_id_from_uri(inp) == expected


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("at://did:ex/col/abc123", "did:ex"),
        (None, None),
        ("", None),
        ("at://did:example/collection/rc1", "did:example"),
        ("not-a-uri", None),
    ],
)
def test__did_from_uri(inp, expected):
    """_did_from_uri: extract DID from at:// URIs or return None for invalid inputs."""
    assert _did_from_uri(inp) == expected


def test_emit_facets_entities():
    """_emit_facets_entities should produce entity types and include embed bodies (images)."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    obj = {
        "facets": [
            {
                "features": [
                    {"$type": "app.bsky.richtext.facet#tag", "tag": "#cool"},
                    {"$type": "app.bsky.richtext.facet#mention", "did": "did:someone"},
                    {"$type": "app.bsky.richtext.facet#link", "uri": "http://x"},
                ]
            }
        ],
        "embed": {
            "$type": "app.bsky.embed.images",
            "images": [{"fullsize": "https://img.example/1.jpg"}],
        },
    }
    ents = list(_emit_facets_entities("p1", "did:me", now, now, obj))
    types = {e.entity_type for e in ents}
    assert any(t for t in types)
    assert any(e.body and "img.example" in e.body for e in ents)


@pytest.mark.parametrize(
    "obj,expected_action",
    [
        (
            {"reply": {"parent": {"uri": "at://did:someone/app.bsky.feed.post/pp"}}},
            ActionType.COMMENT,
        ),
        (
            {
                "embed": {
                    "$type": "app.bsky.embed.record",
                    "record": {"uri": "at://did:someone/app.bsky.feed.post/qq"},
                }
            },
            ActionType.QUOTE,
        ),
    ],
)
def test_emit_post_relationship_actions(obj, expected_action):
    """Parametrized check: reply -> COMMENT, embed record -> QUOTE."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    acts = list(_emit_post_relationship_actions(obj, "did:me", now, now))
    assert any(a.action_type == expected_action for a in acts)
