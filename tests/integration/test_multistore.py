"""Integration tests for MultiStore against real, throwaway Postgres databases.

Like test_erasure.py, MultiStore/StandardDB connect by database name via
DBConfig (host/user/password from the environment), so this file manages its
own pair of scratch databases rather than reusing the shared dsn/schema/conn
fixtures used elsewhere in this directory.
"""

import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from smdt.config import DBConfig, STANDARD_SCHEMA_SQL_PATH
from smdt.multistore import MultiStore
from smdt.store.models import Accounts, Posts
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
        pytest.skip(f"No local Postgres reachable for multistore integration tests: {e}")


def _now():
    return datetime.now(timezone.utc)


@pytest.fixture
def two_datasets():
    """Create two throwaway, fully-schema'd datasets, seeded with one
    overlapping account (same username, different follower counts) and one
    post each. Drops both after the test."""
    suffix = uuid.uuid4().hex[:10]
    db_a = f"smdt_multistore_test_a_{suffix}"
    db_b = f"smdt_multistore_test_b_{suffix}"

    cfg = DBConfig()
    schema_sql = open(STANDARD_SCHEMA_SQL_PATH).read()

    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_a}"')
        cur.execute(f'CREATE DATABASE "{db_b}"')
    admin.close()

    for db_name in (db_a, db_b):
        conn = psycopg.connect(
            dbname=db_name, user=cfg.user, password=cfg.password, host=cfg.host, port=cfg.port
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.close()

    for db_name, followers, platform in [(db_a, 100, "twitter"), (db_b, 200, "bluesky")]:
        db = StandardDB(db_name)
        db.insert_with_fallbacks(
            [
                Accounts(
                    created_at=_now(),
                    account_id=f"alice_{platform}",
                    username="alice",
                    follower_count=followers,
                    platform=platform,
                )
            ]
        )
        db.insert_with_fallbacks(
            [
                Posts(
                    created_at=_now(),
                    account_id=f"alice_{platform}",
                    post_id=f"p1_{platform}",
                    body=f"hello from {platform}",
                    platform=platform,
                )
            ]
        )

    yield db_a, db_b

    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_a}"')
        cur.execute(f'DROP DATABASE IF EXISTS "{db_b}"')
    admin.close()


def test_attach_and_cross_db_join(two_datasets) -> None:
    db_a, db_b = two_datasets
    with MultiStore() as ms:
        ms.attach("twitter", db_name=db_a)
        ms.attach("bluesky", db_name=db_b)

        df = ms.query(
            """
            SELECT tw.username, tw.follower_count AS tw_followers, bs.follower_count AS bs_followers
            FROM twitter.accounts tw
            JOIN bluesky.accounts bs ON tw.username = bs.username
            """
        )
        assert len(df) == 1
        assert df.iloc[0]["username"] == "alice"
        assert df.iloc[0]["tw_followers"] == 100
        assert df.iloc[0]["bs_followers"] == 200


def test_union_across_datasets(two_datasets) -> None:
    db_a, db_b = two_datasets
    with MultiStore() as ms:
        ms.attach("twitter", db_name=db_a)
        ms.attach("bluesky", db_name=db_b)

        df = ms.query(
            """
            SELECT platform, body FROM twitter.posts
            UNION ALL
            SELECT platform, body FROM bluesky.posts
            ORDER BY platform
            """
        )
        assert list(df["platform"]) == ["bluesky", "twitter"]


def test_attach_rejects_duplicate_alias(two_datasets) -> None:
    db_a, _ = two_datasets
    with MultiStore() as ms:
        ms.attach("twitter", db_name=db_a)
        with pytest.raises(ValueError, match="already attached"):
            ms.attach("twitter", db_name=db_a)


def test_attach_rejects_invalid_alias(two_datasets) -> None:
    db_a, _ = two_datasets
    with MultiStore() as ms:
        with pytest.raises(ValueError, match="valid SQL identifier"):
            ms.attach("bad-alias!", db_name=db_a)


def test_detach_removes_dataset(two_datasets) -> None:
    db_a, db_b = two_datasets
    with MultiStore() as ms:
        ms.attach("twitter", db_name=db_a)
        ms.attach("bluesky", db_name=db_b)
        ms.detach("bluesky")
        assert set(ms.datasets) == {"twitter"}
        # Querying the detached alias should fail -- it's really gone.
        with pytest.raises(Exception):
            ms.query("SELECT * FROM bluesky.accounts")


def test_detach_unknown_alias_raises(two_datasets) -> None:
    db_a, _ = two_datasets
    with MultiStore() as ms:
        ms.attach("twitter", db_name=db_a)
        with pytest.raises(KeyError):
            ms.detach("nope")


def test_raw_passthrough_handles_postgis_geometry(two_datasets) -> None:
    """location comes through a normal attached scan as opaque bytes; raw()
    lets a caller apply PostGIS functions (e.g. ST_AsText) on the Postgres
    side before the value crosses into DuckDB."""
    db_a, _ = two_datasets
    admin_cfg = DBConfig()
    conn = psycopg.connect(
        dbname=db_a, user=admin_cfg.user, password=admin_cfg.password,
        host=admin_cfg.host, port=admin_cfg.port,
    )
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE accounts SET location = ST_SetSRID(ST_MakePoint(-122.4, 37.8), 4326) "
            "WHERE username = 'alice'"
        )
    conn.commit()
    conn.close()

    with MultiStore() as ms:
        ms.attach("twitter", db_name=db_a)

        opaque = ms.query("SELECT location FROM twitter.accounts")
        assert not isinstance(opaque.iloc[0]["location"], str)

        wkt = ms.raw("twitter", "SELECT ST_AsText(location) AS location_wkt FROM accounts")
        assert wkt.iloc[0]["location_wkt"] == "POINT(-122.4 37.8)"


def test_raw_on_unattached_alias_raises(two_datasets) -> None:
    with MultiStore() as ms:
        with pytest.raises(KeyError):
            ms.raw("nope", "SELECT 1")
