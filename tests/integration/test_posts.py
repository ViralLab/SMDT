"""Integration tests for Posts model with database."""

import pytest

MODELS = True
try:
    from smdt.store.models.posts import Posts
except Exception:
    MODELS = False


@pytest.mark.skipif(not MODELS, reason="Posts model not importable")
def test_posts_multi_same_created_at(conn, now):
    """Multiple posts with same created_at should be allowed (different id)."""
    p1 = Posts(created_at=now, account_id="a", post_id="p1")
    p2 = Posts(created_at=now, account_id="a", post_id="p2")
    for p in (p1, p2):
        cols, vals = p.insert_columns(), p.insert_values()
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO posts ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})",
                vals,
            )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM posts WHERE created_at = %s", (now,))
        result = cur.fetchone()[0]
    assert result == 2
