"""Integration tests for run_pipeline's num_workers > 1 path against a real,
throwaway Postgres database.

Follows the same throwaway-database-per-test pattern as
test_standard_db_bulk_copy_insert.py. The standardizers here must be
top-level (module-level) classes, not closures -- ProcessPoolExecutor needs
to pickle them to send into worker processes.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from smdt.config import DBConfig, STANDARD_SCHEMA_SQL_PATH
from smdt.ingest.pipeline import PipelineConfig, run_pipeline
from smdt.ingest.plan import plan_directories
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
        pytest.skip(f"No local Postgres reachable for pipeline parallel integration tests: {e}")


def _make_db():
    db_name = f"smdt_pipeline_parallel_test_{uuid.uuid4().hex[:10]}"
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


def _drop_db(db_name: str) -> None:
    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    admin.close()


def _account_count(db_name: str) -> int:
    cfg = DBConfig()
    conn = psycopg.connect(
        dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port
    )
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM accounts")
        (n,) = cur.fetchone()
    conn.close()
    return n


class _AccountsStandardizer:
    """Top-level (picklable) standardizer: one raw {"id": ...} record -> one Accounts row."""

    name = "test_accounts"
    platform = "test"

    def standardize(self, input_record):
        record, _src = input_record
        return [
            Accounts(
                created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
                account_id=str(record["id"]),
                platform="test",
            )
        ]


class _SlowAccountsStandardizer(_AccountsStandardizer):
    """Same as above, but each record costs real wall-clock time -- used to
    prove num_workers > 1 actually delivers a wall-clock speedup, not just
    correctness."""

    name = "test_accounts_slow"

    def standardize(self, input_record):
        time.sleep(0.05)
        return super().standardize(input_record)


def _write_files(tmp_path, num_files: int, records_per_file: int, prefix: str = "f"):
    for f in range(num_files):
        p = tmp_path / f"{prefix}{f}.jsonl"
        lines = [
            f'{{"id": "{prefix}-{f}-{i}"}}\n' for i in range(records_per_file)
        ]
        p.write_text("".join(lines), encoding="utf-8")


@pytest.fixture
def two_dbs():
    db1, db2 = _make_db(), _make_db()
    yield db1, db2
    _drop_db(db1)
    _drop_db(db2)


def test_num_workers_parallel_matches_single_threaded_row_counts(tmp_path, two_dbs):
    db1_name, db2_name = two_dbs
    num_files, records_per_file = 4, 25

    dir1 = tmp_path / "serial"
    dir1.mkdir()
    _write_files(dir1, num_files, records_per_file)

    dir2 = tmp_path / "parallel"
    dir2.mkdir()
    _write_files(dir2, num_files, records_per_file)

    db1 = StandardDB(db1_name)
    plan1 = plan_directories([str(dir1)])
    run_pipeline(
        plan1, db1, _AccountsStandardizer(),
        config=PipelineConfig(on_conflict={Accounts: "DO NOTHING"}, num_workers=1),
    )

    db2 = StandardDB(db2_name)
    plan2 = plan_directories([str(dir2)])
    checkpoint_file = tmp_path / "checkpoint.txt"
    run_pipeline(
        plan2, db2, _AccountsStandardizer(),
        config=PipelineConfig(
            on_conflict={Accounts: "DO NOTHING"},
            num_workers=4,
            checkpoint_file=str(checkpoint_file),
        ),
    )

    expected_total = num_files * records_per_file
    assert _account_count(db1_name) == expected_total
    assert _account_count(db2_name) == expected_total

    # Checkpoint file: every file marked exactly once, nothing missing/duplicated.
    completed = checkpoint_file.read_text().splitlines()
    assert len(completed) == num_files
    assert len(set(completed)) == num_files


def test_num_workers_parallel_is_actually_faster_wall_clock(tmp_path, two_dbs):
    db1_name, db2_name = two_dbs
    num_files, records_per_file = 4, 5  # 0.05s/record -> ~1s of work per file

    dir1 = tmp_path / "serial"
    dir1.mkdir()
    _write_files(dir1, num_files, records_per_file, prefix="s")

    dir2 = tmp_path / "parallel"
    dir2.mkdir()
    _write_files(dir2, num_files, records_per_file, prefix="p")

    db1 = StandardDB(db1_name)
    plan1 = plan_directories([str(dir1)])
    t0 = time.perf_counter()
    run_pipeline(
        plan1, db1, _SlowAccountsStandardizer(),
        config=PipelineConfig(on_conflict={Accounts: "DO NOTHING"}, num_workers=1),
    )
    serial_elapsed = time.perf_counter() - t0

    db2 = StandardDB(db2_name)
    plan2 = plan_directories([str(dir2)])
    t0 = time.perf_counter()
    run_pipeline(
        plan2, db2, _SlowAccountsStandardizer(),
        config=PipelineConfig(on_conflict={Accounts: "DO NOTHING"}, num_workers=4),
    )
    parallel_elapsed = time.perf_counter() - t0

    expected_total = num_files * records_per_file
    assert _account_count(db1_name) == expected_total
    assert _account_count(db2_name) == expected_total

    # 4 files, each ~0.25s of sleep-bound work, on 4 workers should be
    # meaningfully faster than strictly serial -- not asserting a specific
    # ratio (shared-machine timing noise), just that it's clearly faster.
    assert parallel_elapsed < serial_elapsed * 0.75, (
        f"expected parallel ({parallel_elapsed:.2f}s) to be clearly faster "
        f"than serial ({serial_elapsed:.2f}s)"
    )
