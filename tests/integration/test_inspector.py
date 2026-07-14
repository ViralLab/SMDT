"""Integration tests for Inspector against a real, throwaway Postgres
database.

Follows the same throwaway-database pattern as
test_standard_db_bulk_copy_insert.py. Covers both the original exact
(full-scan) mode -- must stay byte-for-byte correct, since existing
callers depend on it -- and the new sample_pct mode, added because a
snapshot() of a real 64M-row hypertable took 18+ seconds in exact mode.
"""

import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from smdt.config import DBConfig, STANDARD_SCHEMA_SQL_PATH
from smdt.inspector import Inspector, load_snapshot, save_snapshot
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
        pytest.skip(f"No local Postgres reachable for inspector integration tests: {e}")


def _make_db_with_data(n_hashtag: int, n_link: int) -> str:
    db_name = f"smdt_inspector_test_{uuid.uuid4().hex[:10]}"
    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    cfg = DBConfig()
    conn = psycopg.connect(
        dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port
    )
    conn.autocommit = True
    base = datetime(2023, 1, 5, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(open(STANDARD_SCHEMA_SQL_PATH).read())
        for i in range(n_hashtag):
            cur.execute(
                "INSERT INTO entities (account_id, post_id, body, entity_type, created_at) "
                "VALUES (%s, %s, %s, 'HASHTAG', %s)",
                (f"acct{i % 5}", f"post{i}", f"#tag{i}", base),
            )
        for i in range(n_link):
            cur.execute(
                "INSERT INTO entities (account_id, post_id, body, entity_type, created_at) "
                "VALUES (%s, %s, %s, 'LINK', %s)",
                (f"acct{i % 5}", f"post_link{i}", f"http://example.com/{i}", base),
            )
        for i in range(10):
            cur.execute(
                "INSERT INTO actions (originator_account_id, target_account_id, action_type, created_at) "
                "VALUES (%s, %s, 'SHARE', %s)",
                (f"acct{i % 5}", f"acct{(i + 1) % 5}", base),
            )
    conn.close()
    return db_name


def _drop_db(db_name: str) -> None:
    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    admin.close()


@pytest.fixture
def small_db():
    name = _make_db_with_data(n_hashtag=30, n_link=20)
    yield name
    _drop_db(name)


def _make_two_period_db() -> str:
    """entities split across two clearly-separated created_at periods, so
    time_window filtering has something real to distinguish."""
    db_name = f"smdt_inspector_test_{uuid.uuid4().hex[:10]}"
    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    cfg = DBConfig()
    conn = psycopg.connect(
        dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port
    )
    conn.autocommit = True
    jan = datetime(2023, 1, 5, tzinfo=timezone.utc)
    feb = datetime(2023, 2, 5, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(open(STANDARD_SCHEMA_SQL_PATH).read())
        for i in range(10):
            cur.execute(
                "INSERT INTO entities (account_id, post_id, body, entity_type, created_at) "
                "VALUES (%s, %s, %s, 'HASHTAG', %s)",
                (f"acct{i}", f"post{i}", f"#jan{i}", jan),
            )
        for i in range(6):
            cur.execute(
                "INSERT INTO entities (account_id, post_id, body, entity_type, created_at) "
                "VALUES (%s, %s, %s, 'LINK', %s)",
                (f"acct{i}", f"post_feb{i}", f"http://example.com/{i}", feb),
            )
    conn.close()
    return db_name


@pytest.fixture
def two_period_db():
    name = _make_two_period_db()
    yield name
    _drop_db(name)


def test_exact_mode_is_correct(small_db):
    db = StandardDB(small_db)
    insp = Inspector(db, schema="public")
    snap = insp.snapshot(only_tables=["entities"])
    e = snap["entities"]

    assert e.est_rows == 50
    assert e.is_estimated is False
    assert e.columns["entity_type"].completeness == 1.0
    counts = dict((v, c) for v, c, _p in e.columns["entity_type"].enum_counts)
    assert counts == {"HASHTAG": 30, "LINK": 20}


def test_sampled_mode_is_marked_estimated_and_stays_close(small_db):
    """A 100% sample must reproduce the exact result -- this both confirms
    the sampled code path is correct and gives a noise-free check (a 1%
    sample of only 50 rows would be too small to compare reliably)."""
    db = StandardDB(small_db)
    insp = Inspector(db, schema="public", sample_pct=100.0)
    snap = insp.snapshot(only_tables=["entities"])
    e = snap["entities"]

    assert e.is_estimated is True
    assert e.columns["entity_type"].completeness == 1.0
    counts = dict((v, c) for v, c, _p in e.columns["entity_type"].enum_counts)
    assert counts == {"HASHTAG": 30, "LINK": 20}


def test_sampled_mode_escalates_on_a_small_table(small_db):
    """A 1% TABLESAMPLE of a 50-row table will very likely come back empty
    on the first attempt (page-based sampling) -- must escalate to 10%/100%
    rather than silently reporting no data."""
    db = StandardDB(small_db)
    insp = Inspector(db, schema="public", sample_pct=1.0)
    snap = insp.snapshot(only_tables=["entities"])
    e = snap["entities"]

    assert e.columns["entity_type"].completeness == 1.0
    assert e.columns["entity_type"].enum_counts, "escalation should have found rows, not come back empty"


def test_sampled_mode_actions_link_stats_still_populate(small_db):
    db = StandardDB(small_db)
    insp = Inspector(db, schema="public", sample_pct=1.0)
    snap = insp.snapshot(only_tables=["actions"])
    a = snap["actions"]

    assert a.extra is not None
    per_type = a.extra["actions_links_per_type"]
    assert any(row["action_type"] == "SHARE" for row in per_type)
    share_row = next(row for row in per_type if row["action_type"] == "SHARE")
    assert share_row["pct"]["target_account_id"] == 1.0
    assert share_row["pct"]["originator_account_id"] == 1.0


def test_exact_and_sampled_report_schemas_do_not_error(small_db, capsys, tmp_path):
    """report_schemas() must render without error in both modes -- this is
    mostly a smoke test for the rendering code path (column widths, the new
    'sampled estimate' suffix), not a content assertion."""
    from smdt.inspector import report_schemas

    db = StandardDB(small_db)
    report_schemas(
        [Inspector(db, schema="public")],
        only_tables=["entities", "actions"],
        save_dir=tmp_path,
    )
    report_schemas(
        [Inspector(db, schema="public", sample_pct=100.0)],
        only_tables=["entities", "actions"],
        save_dir=tmp_path,
    )
    out = capsys.readouterr().out
    assert "sampled estimate" in out


def test_time_window_restricts_to_the_right_period(two_period_db):
    db = StandardDB(two_period_db)

    jan_insp = Inspector(
        db, schema="public",
        time_window=(datetime(2023, 1, 1, tzinfo=timezone.utc), datetime(2023, 2, 1, tzinfo=timezone.utc)),
    )
    jan_snap = jan_insp.snapshot(only_tables=["entities"])
    assert jan_snap["entities"].est_rows == 10
    jan_counts = dict((v, c) for v, c, _p in jan_snap["entities"].columns["entity_type"].enum_counts)
    assert jan_counts == {"HASHTAG": 10}

    feb_insp = Inspector(
        db, schema="public",
        time_window=(datetime(2023, 2, 1, tzinfo=timezone.utc), datetime(2023, 3, 1, tzinfo=timezone.utc)),
    )
    feb_snap = feb_insp.snapshot(only_tables=["entities"])
    assert feb_snap["entities"].est_rows == 6
    feb_counts = dict((v, c) for v, c, _p in feb_snap["entities"].columns["entity_type"].enum_counts)
    assert feb_counts == {"LINK": 6}


def test_time_window_label_shown_for_comparison(two_period_db, capsys, tmp_path):
    """The whole point of time_window is comparing the same window across
    multiple databases via report_schemas' existing multi-inspector
    comparison -- the label must show which window each column is."""
    from smdt.inspector import report_schemas

    db = StandardDB(two_period_db)
    jan_insp = Inspector(
        db, schema="public",
        time_window=(datetime(2023, 1, 1, tzinfo=timezone.utc), datetime(2023, 2, 1, tzinfo=timezone.utc)),
    )
    report_schemas([jan_insp], only_tables=["entities"], save_dir=tmp_path)
    out = capsys.readouterr().out
    assert "2023-01-01" in out and "2023-02-01" in out


def test_no_time_window_is_unaffected(small_db):
    """Regression: time_window=None (the default) must behave exactly as
    before -- no WHERE clause added anywhere."""
    db = StandardDB(small_db)
    insp = Inspector(db, schema="public")
    assert insp.time_window is None
    snap = insp.snapshot(only_tables=["entities"])
    assert snap["entities"].est_rows == 50


def test_save_and_load_snapshot_round_trips(small_db, tmp_path):
    """save_snapshot()/load_snapshot() must reconstruct the exact same
    TableStat/ColStat data as the live snapshot -- including enum_counts,
    which needs tuple reconstruction since JSON has no tuple type."""
    db = StandardDB(small_db)
    insp = Inspector(db, schema="public")
    live_snap = insp.snapshot(only_tables=["entities", "actions"])

    path = tmp_path / "snapshot.json"
    save_snapshot(insp, live_snap, path)
    metadata, loaded_snap = load_snapshot(path)

    assert metadata["db_name"] == small_db
    assert metadata["schema"] == "public"
    assert metadata["sample_pct"] is None
    assert metadata["time_window"] is None

    for tname, live_stat in live_snap.items():
        loaded_stat = loaded_snap[tname]
        assert loaded_stat.est_rows == live_stat.est_rows
        assert loaded_stat.is_estimated == live_stat.is_estimated
        assert loaded_stat.extra == live_stat.extra
        for cname, live_col in live_stat.columns.items():
            loaded_col = loaded_stat.columns[cname]
            assert loaded_col.data_type == live_col.data_type
            assert loaded_col.completeness == live_col.completeness
            assert loaded_col.enum_counts == live_col.enum_counts


def test_snapshot_and_save_writes_file_and_returns_snapshot(small_db, tmp_path):
    db = StandardDB(small_db)
    insp = Inspector(db, schema="public")

    path = tmp_path / "snapshot.json"
    snap = insp.snapshot_and_save(path, only_tables=["entities"])

    assert path.exists()
    assert snap["entities"].est_rows == 50
    _metadata, loaded_snap = load_snapshot(path)
    assert loaded_snap["entities"].est_rows == 50


def test_report_schemas_auto_saves_snapshots_by_default(small_db, tmp_path, capsys):
    """report_schemas() must, by default, write one JSON snapshot file per
    inspector under save_dir -- no separate snapshot_and_save() call should
    be needed to get structured output out of a report."""
    from smdt.inspector import report_schemas

    db = StandardDB(small_db)
    insp = Inspector(db, schema="public")
    report_schemas([insp], only_tables=["entities"], save_dir=tmp_path)
    capsys.readouterr()

    written = list(tmp_path.glob("*.json"))
    assert len(written) == 1
    metadata, loaded_snap = load_snapshot(written[0])
    assert metadata["db_name"] == small_db
    assert loaded_snap["entities"].est_rows == 50


def test_report_schemas_save_false_writes_nothing(small_db, tmp_path):
    from smdt.inspector import report_schemas

    db = StandardDB(small_db)
    insp = Inspector(db, schema="public")
    report_schemas([insp], only_tables=["entities"], save=False, save_dir=tmp_path)

    assert list(tmp_path.glob("*.json")) == []


def test_saved_snapshot_records_time_window_and_sample_pct(two_period_db, tmp_path):
    """The saved metadata must reflect whatever mode the inspector was
    actually run in, since a loaded snapshot is only meaningful alongside
    the settings that produced it."""
    db = StandardDB(two_period_db)
    jan = (datetime(2023, 1, 1, tzinfo=timezone.utc), datetime(2023, 2, 1, tzinfo=timezone.utc))
    insp = Inspector(db, schema="public", sample_pct=100.0, time_window=jan)

    path = tmp_path / "snapshot.json"
    insp.snapshot_and_save(path, only_tables=["entities"])
    metadata, loaded_snap = load_snapshot(path)

    assert metadata["sample_pct"] == 100.0
    assert metadata["time_window"] is not None
    assert loaded_snap["entities"].is_estimated is True
    assert loaded_snap["entities"].est_rows == 10
