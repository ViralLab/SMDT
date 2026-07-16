"""Integration tests for Pseudonymizer's num_workers > 1 transform path
against a real, throwaway Postgres database.

Follows the same throwaway-database-per-test pattern as
test_pipeline_parallel.py. Unlike ingestion's file-level parallelism,
workers here never open a DB connection at all (see
Pseudonymizer._copy_table_parallel's docstring) -- only the CPU-bound
transform step is dispatched to a process pool, fetch and flush stay
single-owner in the main process.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from smdt.config import DBConfig, STANDARD_SCHEMA_SQL_PATH
from smdt.pseudonymizer.pseudonymizer import Pseudonymizer, PseudonymizeConfig

pytestmark = pytest.mark.integration

PEPPER = b"test-pepper-fixed-for-comparison"
N_POSTS = 8000
N_ACCOUNTS = 50


def _admin_conn():
    cfg = DBConfig()
    try:
        conn = psycopg.connect(
            dbname="postgres", user=cfg.user, password=cfg.password,
            host=cfg.host, port=cfg.port, connect_timeout=2,
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        pytest.skip(f"No local Postgres reachable for pseudonymizer parallel integration tests: {e}")


def _make_db_with_schema() -> str:
    db_name = f"smdt_pseudo_parallel_test_{uuid.uuid4().hex[:10]}"
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
    return db_name


def _make_empty_db() -> str:
    db_name = f"smdt_pseudo_parallel_test_{uuid.uuid4().hex[:10]}"
    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()
    return db_name


def _drop_db(db_name: str) -> None:
    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    admin.close()


def _seed_posts(db_name: str, n_posts: int = N_POSTS) -> None:
    cfg = DBConfig()
    conn = psycopg.connect(
        dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port
    )
    base = datetime(2023, 1, 5, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        for i in range(N_ACCOUNTS):
            cur.execute(
                "INSERT INTO accounts (account_id, username, bio, platform, created_at) "
                "VALUES (%s, %s, %s, 'test', %s)",
                (f"acct{i}", f"user{i}", f"bio for user{i} contact @user{(i+1) % N_ACCOUNTS}", base),
            )
        for i in range(n_posts):
            body = (
                f"hello @user{(i + 1) % N_ACCOUNTS} check http://example.com/thread/{i} "
                f"#tag{i % 11} some additional filler text so redaction does real regex "
                f"work over a realistically-sized body string, not a trivial one-liner"
            )
            cur.execute(
                "INSERT INTO posts (post_id, account_id, body, platform, created_at) "
                "VALUES (%s, %s, %s, 'test', %s)",
                (f"post{i}", f"acct{i % N_ACCOUNTS}", body, base),
            )
    conn.commit()
    conn.close()


def _dump_posts(db_name: str):
    cfg = DBConfig()
    conn = psycopg.connect(
        dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port
    )
    with conn.cursor() as cur:
        cur.execute("SELECT post_id, account_id, body FROM posts ORDER BY post_id")
        rows = sorted(cur.fetchall())
    conn.close()
    return rows


@pytest.fixture
def seeded_src():
    src = _make_db_with_schema()
    _seed_posts(src)
    yield src
    _drop_db(src)


@pytest.fixture
def two_empty_dst_dbs():
    dst1, dst4 = _make_empty_db(), _make_empty_db()
    yield dst1, dst4
    _drop_db(dst1)
    _drop_db(dst4)


def test_num_workers_parallel_matches_serial_output(seeded_src, two_empty_dst_dbs):
    dst1, dst4 = two_empty_dst_dbs

    cfg1 = PseudonymizeConfig(
        src_db_name=seeded_src, dst_db_name=dst1, pepper=PEPPER, ask_reinit=False, num_workers=1
    )
    p1 = Pseudonymizer(cfg1)
    p1.prepare_destination()
    n1 = p1._copy_table("posts")

    cfg4 = PseudonymizeConfig(
        src_db_name=seeded_src, dst_db_name=dst4, pepper=PEPPER, ask_reinit=False,
        num_workers=4, transform_chunk_size=250,
    )
    p4 = Pseudonymizer(cfg4)
    p4.prepare_destination()
    n4 = p4._copy_table("posts")

    assert n1 == n4 == N_POSTS
    # Same pepper on both sides -> hashing is deterministic, so the actual
    # redacted content (not just row counts) must match exactly, regardless
    # of which process did the hashing/redaction.
    assert _dump_posts(dst1) == _dump_posts(dst4)


def test_num_workers_parallel_is_faster_wall_clock(seeded_src, two_empty_dst_dbs):
    dst1, dst4 = two_empty_dst_dbs

    cfg1 = PseudonymizeConfig(
        src_db_name=seeded_src, dst_db_name=dst1, pepper=PEPPER, ask_reinit=False, num_workers=1
    )
    p1 = Pseudonymizer(cfg1)
    p1.prepare_destination()
    t0 = time.perf_counter()
    p1._copy_table("posts")
    serial_elapsed = time.perf_counter() - t0

    cfg4 = PseudonymizeConfig(
        src_db_name=seeded_src, dst_db_name=dst4, pepper=PEPPER, ask_reinit=False,
        num_workers=4, transform_chunk_size=250,
    )
    p4 = Pseudonymizer(cfg4)
    p4.prepare_destination()
    t0 = time.perf_counter()
    p4._copy_table("posts")
    parallel_elapsed = time.perf_counter() - t0

    # Real transform work (regex redaction + hashing) over N_POSTS real rows
    # -- no artificial sleep needed. Not asserting a specific ratio (shared-
    # machine timing noise), just that it's clearly faster.
    assert parallel_elapsed < serial_elapsed * 0.75, (
        f"expected parallel ({parallel_elapsed:.2f}s) to be clearly faster "
        f"than serial ({serial_elapsed:.2f}s)"
    )
