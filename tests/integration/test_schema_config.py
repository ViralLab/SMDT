"""Integration tests for apply_hypertable_config()/init_schema(hypertable_config=...)
against a real, throwaway Postgres database.

Follows the same throwaway-database pattern as
test_standard_db_bulk_copy_insert.py.
"""

from dataclasses import replace
from datetime import timedelta

import psycopg
import pytest
import uuid

from smdt.config import DBConfig, STANDARD_SCHEMA_SQL_PATH
from smdt.store.schema_config import SchemaConfig
from smdt.store.standard_db import StandardDB

pytestmark = pytest.mark.integration


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
        pytest.skip(f"No local Postgres reachable for schema_config integration tests: {e}")


def _make_empty_db() -> str:
    db_name = f"smdt_schema_config_test_{uuid.uuid4().hex[:10]}"
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


def _dimensions(db_name: str):
    cfg = DBConfig()
    conn = psycopg.connect(dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT hypertable_name, column_name, num_partitions,
                   EXTRACT(epoch FROM time_interval)::bigint AS interval_secs
            FROM timescaledb_information.dimensions ORDER BY 1, dimension_number
        """)
        rows = cur.fetchall()
    conn.close()
    return rows


def _compression_policies(db_name: str):
    cfg = DBConfig()
    conn = psycopg.connect(dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT hypertable_name FROM timescaledb_information.jobs "
            "WHERE proc_name = 'policy_compression'"
        )
        rows = {r[0] for r in cur.fetchall()}
    conn.close()
    return rows


@pytest.fixture
def empty_db():
    name = _make_empty_db()
    yield name
    _drop_db(name)


def test_default_hypertable_config_matches_previous_hardcoded_schema(empty_db):
    db = StandardDB(empty_db)
    db.init_schema(STANDARD_SCHEMA_SQL_PATH)  # hypertable_config=None -> SchemaConfig() defaults

    dims = _dimensions(empty_db)
    by_table = {}
    for table, col, n_parts, interval in dims:
        by_table.setdefault(table, []).append((col, n_parts, interval))

    assert by_table["posts"] == [("created_at", None, 7 * 86400)]
    assert by_table["accounts"] == [("created_at", None, 30 * 86400)]
    assert ("entity_type", 6, None) in by_table["entities"]
    assert ("action_type", 8, None) in by_table["actions"]
    assert _compression_policies(empty_db) == set(), "no compression policy scheduled by default"


def test_custom_hypertable_config_changes_real_chunk_interval(empty_db):
    custom = SchemaConfig(tables={
        **SchemaConfig().tables,
        "posts": replace(SchemaConfig().tables["posts"], chunk_time_interval=timedelta(hours=6)),
    })
    db = StandardDB(empty_db)
    db.init_schema(STANDARD_SCHEMA_SQL_PATH, hypertable_config=custom)

    by_table = {}
    for table, col, n_parts, interval in _dimensions(empty_db):
        by_table.setdefault(table, []).append((col, n_parts, interval))
    assert by_table["posts"] == [("created_at", None, 6 * 3600)]


def test_custom_hypertable_config_can_remove_space_partitioning(empty_db):
    custom = SchemaConfig(tables={
        **SchemaConfig().tables,
        "entities": replace(SchemaConfig().tables["entities"], space_partition_column=None, space_partitions=None),
    })
    db = StandardDB(empty_db)
    db.init_schema(STANDARD_SCHEMA_SQL_PATH, hypertable_config=custom)

    entities_dims = [d for d in _dimensions(empty_db) if d[0] == "entities"]
    assert len(entities_dims) == 1, f"expected only the time dimension, got {entities_dims}"


def test_custom_hypertable_config_can_schedule_compression(empty_db):
    custom = SchemaConfig(tables={
        **SchemaConfig().tables,
        "accounts": replace(SchemaConfig().tables["accounts"], compress_after=timedelta(days=14)),
    })
    db = StandardDB(empty_db)
    db.init_schema(STANDARD_SCHEMA_SQL_PATH, hypertable_config=custom)

    assert _compression_policies(empty_db) == {"accounts"}


def test_init_schema_is_idempotent(empty_db):
    """Calling init_schema() (and therefore apply_hypertable_config())
    twice must not error -- resumed benchmark runs rely on this."""
    db = StandardDB(empty_db)
    db.init_schema(STANDARD_SCHEMA_SQL_PATH)
    db.init_schema(STANDARD_SCHEMA_SQL_PATH)  # must not raise

    dims_after_twice = _dimensions(empty_db)
    assert len(dims_after_twice) > 0
