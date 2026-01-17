"""Integration tests for Accounts model with database."""

import pytest
from datetime import timezone, datetime


MODELS = True
try:
    from smdt.store.models.accounts import Accounts
except Exception as e:
    MODELS = False
    print(e)


@pytest.mark.skipif(not MODELS, reason="Accounts model not importable")
def test_insert_and_read_accounts(conn, now):
    """Insert an account and verify it can be read back."""
    a = Accounts(created_at=now, account_id="  user_x  ", follower_count=42)
    cols = a.insert_columns()
    vals = a.insert_values()
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO accounts ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})",
            vals,
        )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT account_id, follower_count FROM accounts")
        rows = cur.fetchall()
    result = rows
    assert result == [("user_x", 42)]


@pytest.mark.skipif(not MODELS, reason="Accounts model not importable")
def test_negative_counts_rejected_by_python():
    """Negative follower_count should raise ValueError in Python model."""
    with pytest.raises(ValueError):
        Accounts(created_at=datetime.now(timezone.utc), follower_count=-1)
