"""Integration tests for Eraser against a real, throwaway Postgres database.

Eraser/StandardDB connect by database name via DBConfig (host/user/password
from the environment), not an arbitrary DSN+schema like the other integration
tests in this directory -- so this file manages its own full scratch database
per test rather than reusing the shared `dsn`/`schema`/`conn` fixtures.
"""

import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from smdt.config import DBConfig, STANDARD_SCHEMA_SQL_PATH
from smdt.pseudonymizer import Eraser, ErasureMode, ErasureTarget
from smdt.pseudonymizer.pseudonyms import Algorithm, Hasher
from smdt.store.models import (
    AccountEnrichments,
    Accounts,
    ActionType,
    Actions,
    EntityType,
    Entities,
    PostEnrichments,
    Posts,
)
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
        pytest.skip(f"No local Postgres reachable for erasure integration tests: {e}")


@pytest.fixture
def erasure_db():
    """Create a throwaway database with the full standard schema, drop after."""
    db_name = f"smdt_erasure_test_{uuid.uuid4().hex[:10]}"
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


def _seed_alice_and_bob(db: StandardDB) -> None:
    """Alice posts P1; Bob replies with P2 and also acts on Alice directly."""
    db.insert_with_fallbacks(
        [
            Accounts(created_at=_now(), account_id="alice_real", username="AliceReal", bio="hi", platform="twitter"),
            Accounts(created_at=_now(), account_id="bob_real", username="BobReal", bio="hi", platform="twitter"),
        ]
    )
    db.insert_with_fallbacks(
        [
            Posts(created_at=_now(), account_id="alice_real", post_id="P1", conversation_id="P1", body="alice's post", platform="twitter"),
            Posts(created_at=_now(), account_id="bob_real", post_id="P2", conversation_id="P1", body="bob's reply", platform="twitter"),
        ]
    )
    db.insert_with_fallbacks(
        [
            # Bob acting on Alice's post: originator=bob (should survive).
            Actions(created_at=_now(), originator_account_id="bob_real", originator_post_id="P2", target_account_id="alice_real", target_post_id="P1", action_type=ActionType.COMMENT),
            # Bob following Alice directly: originator=bob (should survive, target cleared).
            Actions(created_at=_now(), originator_account_id="bob_real", target_account_id="alice_real", action_type=ActionType.FOLLOW),
            # Alice acting on Bob's post: originator=alice (should be deleted).
            Actions(created_at=_now(), originator_account_id="alice_real", target_account_id="bob_real", target_post_id="P2", action_type=ActionType.UPVOTE),
        ]
    )
    db.insert_with_fallbacks(
        [Entities(created_at=_now(), account_id="alice_real", post_id="P1", body="hello", entity_type=EntityType.HASHTAG)]
    )
    db.insert_with_fallbacks(
        [AccountEnrichments(created_at=_now(), account_id="alice_real", model_id="bot_v1", body={"score": 0.1})]
    )
    db.insert_with_fallbacks(
        [PostEnrichments(created_at=_now(), post_id="P1", model_id="tox_v1", body={"score": 0.1})]
    )


def test_scrub_mode_plaintext_target(erasure_db) -> None:
    """SCRUB on a plaintext DB: personal fields nulled, structure preserved."""
    db = StandardDB(erasure_db)
    _seed_alice_and_bob(db)

    eraser = Eraser(
        targets=[ErasureTarget(db_name=erasure_db, mode=ErasureMode.SCRUB, is_pseudonymized=False)]
    )
    report = eraser.erase("alice_real")[erasure_db]

    assert report["matched_account_ids"] == ["alice_real"]
    assert report["accounts_scrubbed"] == 1
    assert report["posts_scrubbed"] == 1
    assert report["entities_deleted"] == 1
    assert report["account_enrichments_deleted"] == 1
    assert report["post_enrichments_deleted"] == 1
    assert report["actions_deleted"] == 1  # Alice's own UPVOTE
    assert report["actions_target_cleared"] == 2  # Bob's COMMENT + FOLLOW

    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT account_id, username, bio FROM accounts")
        rows = cur.fetchall()
        assert len(rows) == 2
        alice_row = next(r for r in rows if r[0] != "bob_real")
        assert alice_row[1] is None and alice_row[2] is None
        placeholder = alice_row[0]
        assert placeholder.startswith("erased_")

        cur.execute("SELECT post_id, account_id, body, conversation_id FROM posts ORDER BY post_id")
        posts = {r[0]: r for r in cur.fetchall()}
        # Alice's post: scrubbed, same placeholder as her account row.
        assert posts["P1"][1] == placeholder
        assert posts["P1"][2] is None
        assert posts["P1"][3] is None
        # Bob's reply: completely untouched, still threaded to P1.
        assert posts["P2"] == ("P2", "bob_real", "bob's reply", "P1")

        cur.execute(
            "SELECT originator_account_id, target_account_id, action_type FROM actions ORDER BY action_type"
        )
        actions = cur.fetchall()
        assert len(actions) == 2  # Alice's UPVOTE is gone
        by_type = {a[2]: a for a in actions}
        assert by_type["COMMENT"] == ("bob_real", None, "COMMENT")
        assert by_type["FOLLOW"] == ("bob_real", None, "FOLLOW")

        for table in ("entities", "account_enrichments", "post_enrichments"):
            cur.execute(f"SELECT count(*) FROM {table}")
            assert cur.fetchone()[0] == 0
    conn.close()


def test_delete_mode_plaintext_target(erasure_db) -> None:
    """DELETE on a plaintext DB: accounts/posts hard-removed; Bob's action survives."""
    db = StandardDB(erasure_db)
    _seed_alice_and_bob(db)

    eraser = Eraser(
        targets=[ErasureTarget(db_name=erasure_db, mode=ErasureMode.DELETE, is_pseudonymized=False)]
    )
    report = eraser.erase("alice_real")[erasure_db]
    assert report["accounts_deleted"] == 1
    assert report["posts_deleted"] == 1

    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM accounts WHERE account_id = 'alice_real'")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM posts WHERE post_id = 'P1'")
        assert cur.fetchone()[0] == 0
        # Bob's own post/account remain.
        cur.execute("SELECT count(*) FROM accounts WHERE account_id = 'bob_real'")
        assert cur.fetchone()[0] == 1
        # Bob's actions survive with target cleared, even in DELETE mode.
        cur.execute(
            "SELECT originator_account_id, target_account_id FROM actions WHERE originator_account_id = 'bob_real'"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert all(r[1] is None for r in rows)
    conn.close()


def test_scrub_mode_pseudonymized_target(erasure_db) -> None:
    """SCRUB on an already-pseudonymized DB: identity resolved via hash, hash left as-is."""
    pepper = b"test-pepper"
    hasher = Hasher(algo=Algorithm.SHA256, pepper=pepper, normalizer=lambda s: s.strip().lower())
    alice_hash = hasher.make("alice_real")

    db = StandardDB(erasure_db)
    db.insert_with_fallbacks(
        [Accounts(created_at=_now(), account_id=alice_hash, username=hasher.make("AliceReal"), platform="twitter")]
    )
    db.insert_with_fallbacks(
        [Posts(created_at=_now(), account_id=alice_hash, post_id="P1hash", body=None, platform="twitter")]
    )

    eraser = Eraser(
        targets=[ErasureTarget(db_name=erasure_db, mode=ErasureMode.SCRUB, is_pseudonymized=True)],
        pepper=pepper,
        algorithm=Algorithm.SHA256,
    )
    report = eraser.erase("alice_real")[erasure_db]  # real identity in, hash resolved internally
    assert report["matched_account_ids"] == [alice_hash]

    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT account_id, username FROM accounts")
        row = cur.fetchone()
        # Hash is left unchanged (already opaque) -- no placeholder needed.
        assert row[0] == alice_hash
        assert row[1] is None

        cur.execute("SELECT account_id FROM posts")
        assert cur.fetchone()[0] == alice_hash
    conn.close()


def test_erase_by_username(erasure_db) -> None:
    """identity_column='username' resolves and reports the real account_id."""
    db = StandardDB(erasure_db)
    db.insert_with_fallbacks(
        [Accounts(created_at=_now(), account_id="alice_real", username="AliceReal", platform="twitter")]
    )
    eraser = Eraser(
        targets=[ErasureTarget(db_name=erasure_db, mode=ErasureMode.SCRUB, is_pseudonymized=False)]
    )
    report = eraser.erase("AliceReal", identity_column="username")[erasure_db]
    assert report["matched_account_ids"] == ["alice_real"]
    assert report["accounts_scrubbed"] == 1


def test_erase_no_match_is_a_noop(erasure_db) -> None:
    """A non-existent identity reports no matches and touches nothing."""
    db = StandardDB(erasure_db)
    db.insert_with_fallbacks(
        [Accounts(created_at=_now(), account_id="alice_real", username="AliceReal", platform="twitter")]
    )
    eraser = Eraser(
        targets=[ErasureTarget(db_name=erasure_db, mode=ErasureMode.SCRUB, is_pseudonymized=False)]
    )
    report = eraser.erase("nobody_such_account")[erasure_db]
    assert report == {"matched_account_ids": []}

    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM accounts")
        assert cur.fetchone()[0] == 1
    conn.close()
