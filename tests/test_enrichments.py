import pytest, psycopg
from psycopg.types.json import Jsonb

MODELS = True
try:
    from smdt.store.models.account_enrichments import AccountEnrichments
    from smdt.store.models.post_enrichments import PostEnrichments
except Exception:
    MODELS = False


def _adapt_jsonb(cols, vals, jsonb_cols=("body",)):
    """Return a new tuple where JSONB columns are wrapped in Jsonb()."""
    vals = list(vals)
    for i, c in enumerate(cols):
        if c in jsonb_cols and vals[i] is not None and not isinstance(vals[i], Jsonb):
            vals[i] = Jsonb(vals[i])
    return tuple(vals)


@pytest.mark.skipif(not MODELS, reason="Enrichment models not importable")
def test_account_enrichments_unique_and_conflict(conn, now):
    e1 = AccountEnrichments(
        model_id="m1", account_id="a1", body={"x": 1}, created_at=now
    )
    e2 = AccountEnrichments(
        model_id="m1", account_id="a1", body={"x": 2}, created_at=now
    )

    cols = e1.insert_columns()
    placeholders = ", ".join(["%s"] * len(cols))

    # Insert first row (wrap JSONB)
    vals1 = _adapt_jsonb(cols, e1.insert_values())
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO account_enrichments ({', '.join(cols)}) VALUES ({placeholders})",
            vals1,
        )
    conn.commit()

    # Duplicate insert should violate UNIQUE(model_id, account_id)
    vals2 = _adapt_jsonb(cols, e2.insert_values())
    with pytest.raises(psycopg.errors.UniqueViolation):
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO account_enrichments ({', '.join(cols)}) VALUES ({placeholders})",
                vals2,
            )
    conn.rollback()

    # Insert duplicate with ON CONFLICT DO NOTHING → no error
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO account_enrichments ({', '.join(cols)}) VALUES ({placeholders}) "
            "ON CONFLICT (model_id, account_id) DO NOTHING",
            vals2,
        )
    conn.commit()


@pytest.mark.skipif(not MODELS, reason="Enrichment models not importable")
def test_post_enrichments_unique(conn, now):
    p1 = PostEnrichments(model_id="m1", post_id="p1", body={"a": True}, created_at=now)
    p2 = PostEnrichments(model_id="m1", post_id="p1", body={"a": False}, created_at=now)

    cols = p1.insert_columns()
    placeholders = ", ".join(["%s"] * len(cols))

    # First insert
    vals1 = _adapt_jsonb(cols, p1.insert_values())
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO post_enrichments ({', '.join(cols)}) VALUES ({placeholders})",
            vals1,
        )
    conn.commit()

    # Duplicate insert should fail on UNIQUE(model_id, post_id)
    vals2 = _adapt_jsonb(cols, p2.insert_values())
    with pytest.raises(psycopg.errors.UniqueViolation):
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO post_enrichments ({', '.join(cols)}) VALUES ({placeholders})",
                vals2,
            )
    conn.rollback()
