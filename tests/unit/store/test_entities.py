from datetime import datetime, timezone

import pytest

from smdt.store.models.entities import Entities, EntityType

_NOW = datetime.now(timezone.utc)


def test_body_none_raises():
    """Regression test: entities.body is NOT NULL in the SQL schema, but the
    Python model used to accept body=None silently -- the failure only
    surfaced later as a Postgres constraint violation, caught (and the row
    dropped) by StandardDB's savepoint fallback instead of failing at
    construction time."""
    with pytest.raises(ValueError, match="body is required"):
        Entities(created_at=_NOW, entity_type=EntityType.LINK, post_id="p1", body=None)


def test_body_non_string_raises():
    with pytest.raises(ValueError, match="body must be a string"):
        Entities(created_at=_NOW, entity_type=EntityType.LINK, post_id="p1", body=123)


def test_valid_body_constructs_fine():
    e = Entities(
        created_at=_NOW, entity_type=EntityType.LINK, post_id="p1", body="http://example.com"
    )
    assert e.body == "http://example.com"


def test_requires_post_id_or_community_id():
    with pytest.raises(ValueError, match="post_id or community_id"):
        Entities(created_at=_NOW, entity_type=EntityType.HASHTAG, body="tag")
