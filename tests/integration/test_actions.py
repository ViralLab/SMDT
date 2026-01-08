"""Integration tests for Actions model with database."""
import pytest

MODELS = True
try:
    from smdt.store.models.actions import Actions
except Exception:
    MODELS = False


@pytest.mark.skipif(not MODELS, reason="Actions model not importable")
def test_actions_check_and_enum(conn, now):
    """Insert an action and verify enum is stored correctly."""
    a = Actions(
        created_at=now,
        action_type="follow",
        originator_account_id="u1",
        target_account_id="u2",
    )
    cols, vals = a.insert_columns(), a.insert_values()
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO actions ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})",
            vals,
        )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT action_type FROM actions")
        result = cur.fetchone()[0]
    assert result == "FOLLOW"


@pytest.mark.skipif(not MODELS, reason="Actions model not importable")
def test_actions_python_mirrors_check(conn, now):
    """Python model should enforce originator requirement."""
    from smdt.store.models.actions import Actions

    with pytest.raises(ValueError, match="originator"):
        Actions(created_at=now, action_type="BLOCK", target_account_id="x")
