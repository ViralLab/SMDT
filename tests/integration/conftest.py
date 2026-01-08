"""
Shared fixtures for integration tests requiring a database.

All tests in this directory are automatically marked with `@pytest.mark.integration`.
"""
import os
import uuid
from pathlib import Path
from datetime import datetime, timezone

import psycopg
import pytest

try:
    from dotenv import load_dotenv

    root = Path(__file__).parent.parent.parent
    if (root / ".env.test").exists():
        load_dotenv(dotenv_path=root / ".env.test", override=True)
    else:
        load_dotenv(dotenv_path=root / ".env", override=False)
except Exception:
    pass


def tznow():
    return datetime.now(timezone.utc)


def pytest_collection_modifyitems(items):
    """Auto-mark all tests in integration/ with @pytest.mark.integration."""
    for item in items:
        if "/integration/" in str(item.fspath) or "\\integration\\" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def dsn():
    """Database connection string from TEST_DATABASE_URL env var."""
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL for DB integration tests.")

    try:
        with psycopg.connect(url, connect_timeout=1):
            pass
    except Exception as e:
        pytest.skip(f"TEST_DATABASE_URL is set but DB is unreachable: {e}")

    return url


# Minimal schema builder (enums + tables)
SCHEMA_SQL = """
DO $$ BEGIN
  CREATE TYPE ENTITY_TYPE AS ENUM ('IMAGE','VIDEO','LINK','USER_TAG','HASHTAG','EMAIL');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE ACTION_TYPE AS ENUM ('UPVOTE','DOWNVOTE','SHARE','QUOTE','UNFOLLOW','FOLLOW','COMMENT','BLOCK');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS accounts (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  account_id TEXT,
  profile_name TEXT,
  bio TEXT,
  location TEXT,
  post_count BIGINT,
  friend_count BIGINT,
  follower_count BIGINT,
  is_verified BOOLEAN,
  profile_image_url TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  retrieved_at TIMESTAMPTZ,
  CHECK (post_count     IS NULL OR post_count     >= 0),
  CHECK (friend_count   IS NULL OR friend_count   >= 0),
  CHECK (follower_count IS NULL OR follower_count >= 0),
  PRIMARY KEY (created_at, id)
);

CREATE TABLE IF NOT EXISTS posts (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  post_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  conversation_id TEXT,
  body TEXT,
  engagement_count BIGINT,
  location TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  retrieved_at TIMESTAMPTZ,
  CHECK (engagement_count IS NULL OR engagement_count >= 0),
  PRIMARY KEY (created_at, id)
);

CREATE TABLE IF NOT EXISTS entities (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  account_id TEXT,
  post_id TEXT,
  body TEXT,
  entity_type ENTITY_TYPE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  retrieved_at TIMESTAMPTZ,
  PRIMARY KEY (created_at, entity_type, id)
);

CREATE TABLE IF NOT EXISTS actions (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  originator_account_id TEXT,
  originator_post_id TEXT,
  target_account_id TEXT,
  target_post_id TEXT,
  action_type ACTION_TYPE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  retrieved_at TIMESTAMPTZ,
  CHECK (
    (originator_account_id IS NOT NULL OR originator_post_id IS NOT NULL) AND
    (target_account_id   IS NOT NULL OR target_post_id IS NOT NULL)
  ),
  PRIMARY KEY (created_at, action_type, id)
);

CREATE TABLE IF NOT EXISTS account_enrichments (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  model_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  body JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  retrieved_at TIMESTAMPTZ,
  UNIQUE (model_id, account_id)
);

CREATE TABLE IF NOT EXISTS post_enrichments (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  model_id TEXT NOT NULL,
  post_id TEXT NOT NULL,
  body JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  retrieved_at TIMESTAMPTZ,
  UNIQUE (model_id, post_id)
);

CREATE TABLE IF NOT EXISTS hash_map (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  hash_key TEXT NOT NULL UNIQUE,
  hash_value TEXT,
  created_at TIMESTAMPTZ NOT NULL
);
"""


@pytest.fixture(scope="function")
def schema(dsn):
    """Create a unique temporary schema for each test, drop afterwards."""
    name = f"test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{name}";')
            cur.execute(f'SET search_path TO "{name}", public;')
            cur.execute(SCHEMA_SQL)
    try:
        yield name
    finally:
        with psycopg.connect(dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE;')


@pytest.fixture(scope="function")
def conn(schema, dsn):
    """Connection whose search_path points to the temp schema."""
    c = psycopg.connect(dsn)
    with c.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema}", public;')
    yield c
    c.close()


@pytest.fixture
def now():
    return tznow()
