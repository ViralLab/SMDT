"""Integration tests for Entities model with database."""
import pytest
from psycopg.types.json import Jsonb

MODELS = True
try:
    from smdt.store.models.entities import Entities, EntityType
except Exception:
    MODELS = False


def _adapt_jsonb(cols, vals, jsonb_cols=("body",)):
    """Return a new tuple where JSONB columns are wrapped in Jsonb()."""
    vals = list(vals)
    for i, c in enumerate(cols):
        if c in jsonb_cols and vals[i] is not None and not isinstance(vals[i], Jsonb):
            vals[i] = Jsonb(vals[i])
    return tuple(vals)


@pytest.mark.skipif(not MODELS, reason="Entities model not importable")
def test_entities_jsonb_and_enum(conn, now):
    """Insert an entity with JSONB body and verify enum and JSON are stored correctly."""
    e = Entities(
        created_at=now,
        entity_type=EntityType.HASHTAG,
        account_id="a",
        post_id="p",
        body={"k": "v"},
    )
    cols = e.insert_columns()
    vals = _adapt_jsonb(cols, e.insert_values())  # wrap JSONB field(s)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO entities ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
            vals,
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT entity_type, body->>'k' FROM entities")
        result = cur.fetchall()

    assert result == [("HASHTAG", "v")]
