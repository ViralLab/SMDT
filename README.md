# SMDT — Social Media Data Toolkit

## Overview

SMDT is a lightweight toolkit for ingesting, normalizing, enriching, and analyzing social-media data. It focuses on streaming-friendly processing for large datasets and provides builders, utilities, and simple NLP/enrichment hooks so you can move from raw exports (JSONL/CSV) to edge lists and NetworkX graphs for analysis. The goal is to provide a flexible, consistent data model for social-media data to enable reproducible data analysis across and between platforms.

## Project layout

```
SMDT/
├── src/smdt/              # Main package
│   ├── anonymizer/        # Redaction and pseudonymization utilities
│   ├── config.py          # Configuration (DB, anonymization)
│   ├── enrichers/         # Text enrichment framework (local + server)
│   ├── ingest/            # Ingestion pipelines and deduplication
│   ├── inspector/         # Data quality inspection utilities
│   ├── io/                # Streaming readers and archive helpers
│   ├── networks/          # Network builders and streaming helpers
│   ├── standardizers/     # Platform-specific normalizers
│   └── store/             # DB models and StandardDB abstraction
├── tests/
│   ├── unit/              # Fast unit tests (no external deps)
│   │   ├── standardizers/ # Tests for base, row, utils + platform tests
│   │   └── networks/      # Tests for network builders
│   └── integration/       # DB integration tests (require Postgres)
├── scripts/               # Convenience scripts
├── prompt.yml             # Prompt templates for enrichers
└── pyproject.toml         # Project metadata and dependencies
```

### Source structure (what's in `src/smdt`)

A short tour of the main packages under `src/smdt` to help you find the right code to extend or reuse:

- `anonymizer/` — small utilities for redaction and pseudonymization (`anonymizer.py`, `redact.py`, `pseudonyms.py`, `policy.py`). Use these to strip or replace sensitive fields before sharing derived datasets.
- `enrichers/` — text enrichment framework:
  - `registry.py` and `runner.py` manage and execute enrichers.
  - `local/` contains lightweight on-device enrichers (language detection, detox models).
  - `server/` contains adapters for remote text-generation or model servers and prompt templates.
- `ingest/` — ingestion helpers and pipelines (`pipeline.py`, `dedup.py`, `plan.py`) used to assemble and clean normalized tables from raw inputs.
- `io/` — streaming readers and archive helpers. Look in `io/readers` for pluggable readers (`jsonl`, `json`, `csv_pd`, `zip`, `tar`) and `archive_stream.py` for streaming archives.
- `networks/` — core network-building logic and helpers:
  - `builders/` implements concrete builders: `user_interaction`, `entity_cooccurrence`, `bipartite`, `coaction`.
  - `api.py` exposes convenient functions like `user_interaction()` and streaming helpers `iter_user_interaction_edges()`.
  - `streams.py` and `io_utils.py` help stream large edge tables in chunks.
- `standardizers/` — platform-specific normalizers that map platform export formats (Twitter, Bluesky, TruthSocial) into the normalized table schema. See the subfolders `twitter/`, `bluesky/`, `truthsocial/` for examples.
- `store/` — models and the `StandardDB` abstraction for storing and querying normalized tables. Models live in `store/models/` (posts, accounts, actions, entities, enrichments). `standard_db.py` contains the Postgres-backed helper used by network builders.
- `inspector/` — small inspection utilities and notebooks/docs for exploring datasets and schemas.

If you're adding support for a new platform, implement a new `standardizers/<platform>/` module to map raw exports into the normalized tables, and add a test in `tests/`.

## Functionalities

- Ingest & standardize

  - Convert raw platform exports (JSON / JSONL / CSV) into normalized tables: `posts`, `users`, `entities`, and `interactions`.
  - Implementations for platform-specific formats live under `src/smdt/standardizers/` (examples: `twitter/`, `bluesky/`, `truthsocial/`).

- Anonymize & redact

  - Remove, redact, or pseudonymize sensitive fields before sharing derived datasets.
  - Policy-driven helpers and pseudonym maps are available in `src/smdt/anonymizer/`.

- Enrich & label

  - Add computed features or labels to posts (language, summaries, toxicity scores, embeddings) via the enrichers framework.
  - Check `src/smdt/enrichers/` for adapters (local and server-backed) and `prompt.yml` for prompt templates.

- Build networks
  - Produce edge lists and node tables for analysis: user–user interaction graphs, entity co-occurrence graphs, and bipartite graphs.
  - Network builders and streaming helpers are in `src/smdt/networks/` and support exporting to Parquet or converting to NetworkX for downstream analysis.

## Standardizers

The project uses the following data model:

- posts
  - post_id (string), user_id (string), created_at (ISO8601), text (string), lang (string)
  - reply_to_post_id, retweet_of_post_id, quote_of_post_id (nullable)
  - hashtags, urls, mentions (JSON/list), metadata (raw)
- users
  - user_id (string), screen_name, name, created_at, followers_count, friends_count, verified, metadata
- entities
  - post_id, entity_text, entity_type (e.g. HASHTAG, PERSON), start, end, normalized
- interactions
  - src_user_id, dst_user_id, interaction_type (reply/retweet/mention/quote), post_id, created_at

Include these column names when adapting your normalizer so builders and enrichers can consume the outputs.

## Enrichers & Anonymization

- `src/smdt/enrichers` contains a small registry and runner for text enrichers (language detection, detoxify wrappers, and server-backed prompt adapters).
- `src/smdt/anonymizer` provides simple pseudonymization and redaction utilities. Use these before sharing derived datasets.
- `prompt.yml` defines small prompt templates (e.g., `summarize_en`, `toxicity_label`) you can use with a text-generation/enricher adapter.

## Inspector

- `src/smdt/inspector` contains utilities to quickly inspect tables (either in a Postgres `StandardDB` or multiple databases) and report per-table completeness and enum/value distributions. It is useful for a quick data-quality check before heavy processing or sharing datasets.

Quick usage (assumes DB credentials / env vars are configured for `DBConfig`):

```python
from smdt.config import DBConfig
from smdt.store.standard_db import StandardDB
from smdt.inspector.inspector import Inspector, report_schemas

cfg = DBConfig()  # reads DB_* env vars
db = StandardDB(db_name=cfg.default_dbname or "mydb", cfg=cfg)
ins = Inspector(db, schema=getattr(cfg, "owner", "public"))
report_schemas([ins], only_tables=["posts", "actions", "users"])
```

The `report_schemas()` helper prints colored completeness and top enum values per column. You can create multiple `Inspector` instances (pointing at different DBs/schemas) to compare schemas side-by-side.

## Quickstart

Minimum requirement: Python 3.11+ (see `pyproject.toml`). Make sure you have `uv` available, and sync the environment from the project's `pyproject.toml` / `uv.lock`:

```bash
uv sync
```

### Running tests

```bash
# Run all tests
uv run python -m pytest

# Run only unit tests (fast, no external dependencies)
uv run python -m pytest tests/unit

# Run only integration tests (requires Postgres via TEST_DATABASE_URL)
uv run python -m pytest tests/integration

# Exclude integration tests
uv run python -m pytest -m "not integration"

# Verbose output with test names
uv run python -m pytest -v
```

Integration tests require a `TEST_DATABASE_URL` environment variable pointing to a Postgres database. You can set this in `.env.test`:

```env
TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/testdb
```

### What to run next

**1. Standardize raw exports**

Platform-specific standardizers convert raw exports (JSONL/CSV) into normalized DB models. Each standardizer's `.standardize(record)` method yields model instances (Posts, Accounts, Entities, Actions):

```python
from smdt.standardizers.twitter.twitter_v2 import TwitterV2Standardizer
from smdt.io.readers.jsonl import JSONLReader

standardizer = TwitterV2Standardizer()

# Stream through a JSONL export
for record in JSONLReader("tweets.jsonl"):
    for model in standardizer.standardize(record):
        # model is a Posts, Accounts, Entities, or Actions instance
        print(model)
```

**2. Inspect the content of a StandardDB**

```python
from smdt.config import DBConfig
from smdt.store.standard_db import StandardDB
from smdt.inspector.inspector import Inspector, report_schemas

cfg = DBConfig()
db = StandardDB(db_name=cfg.default_dbname or 'mydb', cfg=cfg)
ins = Inspector(db, schema=getattr(cfg, 'owner', 'public'))
report_schemas([ins], only_tables=['posts', 'actions', 'accounts'])
```

**3. Build networks**

Once you have normalized data in a DB, use the high-level API to build networks:

```python
from smdt.config import DBConfig
from smdt.store.standard_db import StandardDB
from smdt.networks.api import user_interaction, entity_cooccurrence

cfg = DBConfig()
db = StandardDB(db_name="mydb", cfg=cfg)

# User interaction network (who quoted whom)
result = user_interaction(db, interaction="QUOTE", weighting="count")
print(result.edges.head())  # DataFrame with src, dst, weight, edge_type

# Hashtag co-occurrence network
result = entity_cooccurrence(db, entity_type="HASHTAG", weighting="binary")
print(result.nodes.head())  # DataFrame with node_id, label, type
```

Export to Parquet/CSV for downstream analysis (NetworkX, Gephi, etc.):

```python
result.edges.to_parquet("edges.parquet")
result.nodes.to_csv("nodes.csv", index=False)
```

## Contribution and development notes

- Follow the existing code style and add tests for new behavior.
- **Unit tests** go in `tests/unit/` — these should be fast and not require external services.
- **Integration tests** go in `tests/integration/` — these require a database and are auto-marked with `@pytest.mark.integration`.
- If you add or change public APIs (builders, store interfaces), update tests accordingly.
- Use the network builders and streaming IO when working with datasets that don't fit in memory. Builders are designed to be incremental and work with Parquet/CSV or DB-backed tables.

### Test conventions

- Each test function tests one function, calling it once.
- Use `result` as the variable name for the function output being tested.
- Use parametrization (`@pytest.mark.parametrize`) for testing multiple inputs.
- Each test function should have a docstring describing what it tests.

## License
