CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

DO $$ BEGIN
  CREATE TYPE ENTITY_TYPE AS ENUM ('IMAGE','VIDEO','LINK','USER_TAG','HASHTAG','EMAIL');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE ACTION_TYPE AS ENUM ('UPVOTE','DOWNVOTE','SHARE','QUOTE','UNFOLLOW','FOLLOW','COMMENT','BLOCK');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---------- Core tables (ANON) ----------
-- Notes:
--   * ID/key columns will store pseudonyms (HMAC hex) produced by the exporter.
--   * bio/body; originals are not present.
--   * location columns are kept (schema compatibility); exporter must sanitize/coarsen.

CREATE TABLE IF NOT EXISTS accounts (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    account_id TEXT,            -- pseudonymized
    username TEXT,              -- pseudonymized
    profile_name TEXT,          -- pseudonymized
    bio TEXT,          -- redacted text (mentions/URLs/emails removed)
    location geometry(Point, 4326),
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
    post_id TEXT NOT NULL,      -- pseudonymized
    account_id TEXT NOT NULL,   -- pseudonymized
    conversation_id TEXT,       -- pseudonymized (or NULL if absent)
    body TEXT,         -- redacted text (mentions/URLs/emails removed)
    like_count BIGINT,
    view_count BIGINT,
    share_count BIGINT,
    comment_count BIGINT,
    quote_count BIGINT,
    bookmark_count BIGINT,
    location geometry(Point, 4326),
    created_at TIMESTAMPTZ NOT NULL,
    retrieved_at TIMESTAMPTZ,
    CHECK (like_count IS NULL OR like_count >= 0),
    CHECK (view_count IS NULL OR view_count >= 0),
    CHECK (share_count IS NULL OR share_count >= 0),
    CHECK (comment_count IS NULL OR comment_count >= 0),
    CHECK (quote_count IS NULL OR quote_count >= 0),
    CHECK (bookmark_count IS NULL OR bookmark_count >= 0),
    PRIMARY KEY (created_at, id)
);

CREATE TABLE IF NOT EXISTS entities (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    account_id TEXT,            -- pseudonymized
    post_id TEXT NOT NULL,      -- pseudonymized
    body TEXT NOT NULL, -- sanitized per type (see above)
    entity_type ENTITY_TYPE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    retrieved_at TIMESTAMPTZ,
    PRIMARY KEY (created_at, entity_type, id)
);

CREATE TABLE IF NOT EXISTS actions (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    originator_account_id TEXT, -- pseudonymized
    originator_post_id TEXT,    -- pseudonymized
    target_account_id TEXT,     -- pseudonymized
    target_post_id TEXT,        -- pseudonymized
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
    account_id TEXT NOT NULL,   -- pseudonymized
    body JSONB NOT NULL,        -- ensure no raw text is stored here
    created_at TIMESTAMPTZ NOT NULL,
    retrieved_at TIMESTAMPTZ,
    UNIQUE (model_id, account_id)
);

CREATE TABLE IF NOT EXISTS post_enrichments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_id TEXT NOT NULL,
    post_id TEXT NOT NULL,      -- pseudonymized
    body JSONB NOT NULL,        -- ensure no raw text is stored here
    created_at TIMESTAMPTZ NOT NULL,
    retrieved_at TIMESTAMPTZ,
    UNIQUE (model_id, post_id)
);


-- Hypertables
SELECT create_hypertable('accounts','created_at', chunk_time_interval => INTERVAL '30 days', if_not_exists => TRUE);
SELECT create_hypertable('posts',   'created_at', chunk_time_interval => INTERVAL '7 days',  if_not_exists => TRUE);
SELECT create_hypertable('entities','created_at', chunk_time_interval => INTERVAL '7 days',  if_not_exists => TRUE);
SELECT create_hypertable('actions', 'created_at', chunk_time_interval => INTERVAL '7 days',  if_not_exists => TRUE);

-- Space partitions
SELECT add_dimension('entities', 'entity_type', number_partitions => 6);
SELECT add_dimension('actions',  'action_type', number_partitions => 8);

-- Indexes
-- accounts
CREATE UNIQUE INDEX IF NOT EXISTS accounts_acct_created_uk ON accounts (account_id, created_at);

-- posts
CREATE INDEX IF NOT EXISTS posts_acct_time_idx  ON posts (account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS posts_convo_idx      ON posts (conversation_id);
CREATE INDEX IF NOT EXISTS posts_post_id_idx    ON posts (post_id);

-- entities
CREATE INDEX IF NOT EXISTS entities_post_idx  ON entities (post_id);
CREATE INDEX IF NOT EXISTS entities_acct_type_time_idx  ON entities (account_id, entity_type, created_at DESC);

-- actions
CREATE INDEX IF NOT EXISTS actions_type_time_idx   ON actions (action_type, created_at DESC);
CREATE INDEX IF NOT EXISTS actions_origin_time_idx ON actions (originator_account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS actions_target_time_idx ON actions (target_account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS actions_origin_post_idx ON actions (originator_post_id);
CREATE INDEX IF NOT EXISTS actions_target_post_idx ON actions (target_post_id);

-- Enrichment JSONB
CREATE INDEX IF NOT EXISTS acct_enrich_body_gin ON account_enrichments USING GIN (body);
CREATE INDEX IF NOT EXISTS post_enrich_body_gin ON post_enrichments   USING GIN (body);

-- BRIN indexes (for large tables)
CREATE INDEX IF NOT EXISTS accounts_created_at_brin ON accounts USING BRIN (created_at);
CREATE INDEX IF NOT EXISTS posts_created_at_brin   ON posts    USING BRIN (created_at);
CREATE INDEX IF NOT EXISTS actions_created_at_brin ON actions  USING BRIN (created_at);
CREATE INDEX IF NOT EXISTS entities_created_at_brin ON entities USING BRIN (created_at);

-- Compression (set & policy)
ALTER TABLE accounts SET (timescaledb.compress, timescaledb.compress_segmentby = 'account_id');
ALTER TABLE posts    SET (timescaledb.compress, timescaledb.compress_segmentby = 'account_id');
ALTER TABLE entities SET (timescaledb.compress, timescaledb.compress_segmentby = 'entity_type, account_id');
ALTER TABLE actions  SET (timescaledb.compress, timescaledb.compress_segmentby = 'action_type');

SELECT add_compression_policy('accounts', INTERVAL '30 days');
SELECT add_compression_policy('posts',    INTERVAL '7 days');
SELECT add_compression_policy('entities', INTERVAL '7 days');
SELECT add_compression_policy('actions',  INTERVAL '7 days');

SELECT add_reorder_policy('accounts','accounts_acct_created_uk');
SELECT add_reorder_policy('posts',   'posts_acct_time_idx');
SELECT add_reorder_policy('actions', 'actions_type_time_idx');
SELECT add_reorder_policy('entities','entities_acct_type_time_idx');

-- Spatial indexes (still useful if you keep location with safe transforms)
CREATE INDEX IF NOT EXISTS accounts_location_gix ON accounts USING GIST (location);
CREATE INDEX IF NOT EXISTS posts_location_gix    ON posts    USING GIST (location);