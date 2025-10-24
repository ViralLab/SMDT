import pytest

MODELS = True
try:
    from smdt.store.models.actions import Actions
except Exception:
    MODELS = False


@pytest.mark.skipif(not MODELS, reason="Actions model not importable")
def test_actions_check_and_enum(conn, now):
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
        assert cur.fetchone()[0] == "FOLLOW"


@pytest.mark.skipif(not MODELS, reason="Actions model not importable")
def test_actions_python_mirrors_check(conn, now):
    from datetime import timezone, datetime
    from smdt.store.models.actions import Actions

    with pytest.raises(ValueError, match="originator"):
        Actions(created_at=now, action_type="BLOCK", target_account_id="x")
