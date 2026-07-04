"""Integration tests for StandardDB.bulk_copy_insert against a real,
throwaway Postgres database.

Like test_erasure.py/test_multistore.py, StandardDB connects by database name
via DBConfig (host/user/password from the environment), so this file manages
its own scratch database rather than reusing the shared dsn/schema/conn
fixtures used elsewhere in this directory.
"""

import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from smdt.config import DBConfig, STANDARD_SCHEMA_SQL_PATH
from smdt.store.models import Accounts
from smdt.store.standard_db import StandardDB

pytestmark = pytest.mark.integration


def _admin_conn():
    cfg = DBConfig()
    try:
        conn = psycopg.connect(
            dbname="postgres",
            user=cfg.user,
            password=cfg.password,
            host=cfg.host,
            port=cfg.port,
            connect_timeout=2,
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        pytest.skip(f"No local Postgres reachable for bulk_copy_insert integration tests: {e}")


@pytest.fixture
def bulk_copy_db():
    """Create a throwaway database with the full standard schema, drop after."""
    db_name = f"smdt_bulk_copy_test_{uuid.uuid4().hex[:10]}"
    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    cfg = DBConfig()
    schema_sql = open(STANDARD_SCHEMA_SQL_PATH).read()
    conn = psycopg.connect(
        dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.close()

    yield db_name

    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    admin.close()


def _now():
    return datetime.now(timezone.utc)


def test_bulk_copy_insert_actually_copies_rows(bulk_copy_db) -> None:
    """Regression test: bulk_copy_insert used to call `cur.copy(sql, buf)`,
    which in psycopg3 passes `buf` as the (unused) `params` argument rather
    than writing data through the returned Copy context manager -- so the
    COPY silently never executed and no rows were ever inserted, with no
    exception raised."""
    db = StandardDB(bulk_copy_db)
    db.bulk_copy_insert([Accounts(created_at=_now(), account_id="a1", bio="hello")])

    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT account_id, bio FROM accounts")
        rows = cur.fetchall()
    conn.close()
    assert rows == [("a1", "hello")]


def test_bulk_copy_insert_respects_custom_csv_null(bulk_copy_db) -> None:
    """Regression test: the row-building loop checked
    `to_copy_text(v) is None`, but to_copy_text(None) already returns the
    literal string "\\N", so that check never fired -- a custom csv_null was
    silently ignored and every None was written as literal "\\N" regardless
    of what csv_null said, while the COPY statement's NULL clause expected
    the caller-supplied marker."""
    db = StandardDB(bulk_copy_db)
    db.bulk_copy_insert(
        [Accounts(created_at=_now(), account_id="a1", bio=None)],
        csv_null="CUSTOM_NULL_MARKER",
    )

    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT account_id, bio FROM accounts")
        rows = cur.fetchall()
    conn.close()
    assert rows == [("a1", None)]


def test_bulk_copy_insert_on_conflict_branch_preserves_identity_column(bulk_copy_db) -> None:
    """Regression test: the on_conflict branch created its staging temp table
    with `LIKE ... INCLUDING DEFAULTS`, which does not preserve identity-
    column generation (`id BIGINT GENERATED ALWAYS AS IDENTITY`) -- since the
    COPY never includes `id`, the temp table's `id` column had no way to
    generate a value and every insert violated its NOT NULL constraint."""
    db = StandardDB(bulk_copy_db)
    now = _now()
    db.bulk_copy_insert(
        [Accounts(created_at=now, account_id="a3", bio="first")],
        on_conflict="(account_id, created_at) DO NOTHING",
    )
    db.bulk_copy_insert(
        [Accounts(created_at=now, account_id="a3", bio="second")],
        on_conflict="(account_id, created_at) DO NOTHING",
    )

    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT account_id, bio FROM accounts WHERE account_id = 'a3'")
        rows = cur.fetchall()
    conn.close()
    # ON CONFLICT DO NOTHING: the second insert should be a no-op.
    assert rows == [("a3", "first")]
