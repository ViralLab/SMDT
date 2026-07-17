# SMDT: Social Media Data Toolkit

![diagram](assets/toolkit_diagram.png?raw=true)

**SMDT** is an open-source toolkit for ingesting, normalizing, enriching, pseudonymizing, and analyzing social media data across **11+ platforms** (Twitter/X, Bluesky, TruthSocial, Reddit, Telegram, Gab, Koo, Parler, Voat, Weibo, and more). It converts heterogeneous raw exports into a single unified relational schema backed by Postgres, TimescaleDB, and PostGIS, then provides enrichment, network building, and cross-platform querying on top.

📖 **Documentation:** [varollab.com/SMDT](https://varollab.com/SMDT) &nbsp;|&nbsp; 📄 **Paper:** *Social Media Data Toolkit: Standardization and Anonymization of Social Network Datasets* (Najafi, Iannucci, Kivelä, Varol)

## Table of Contents
- [Features](#features)
- [Prerequisites and Database Setup](#prerequisites-and-database-setup)
- [Installation and Quickstart](#installation-and-quickstart)
- [Usage](#usage)
    - [1. Standardize Raw Exports](#1-standardize-raw-exports)
    - [2. Pseudonymize and Protect Data](#2-pseudonymize-and-protect-data)
    - [3. Inspect Data Quality](#3-inspect-data-quality)
    - [4. Build Networks](#4-build-networks)
    - [5. Enrich Data](#5-enrich-data)
    - [6. Cross-Platform Analysis](#6-cross-platform-analysis)
    - [7. GDPR Erasure](#7-gdpr-erasure)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Performance](#performance)
- [Development and Testing](#development-and-testing)
- [Citation](#citation)
- [License](#license)

## Features

* **Ingest and Standardize:** Convert raw platform exports from 11+ platforms (15+ standardizer variants) into a unified relational schema with five core tables: `Communities`, `Accounts`, `Posts`, `Actions`, `Entities`.
* **Pseudonymize and Redact:** Three-layer configurable privacy pipeline: column-level hashing, regex-based redaction of mentions/emails/URLs, and optional Presidio-based PII detection for phone numbers, credit cards, and personal names.
* **GDPR Erasure:** Forward-only identity resolution. Hard-delete or scrub an individual's data across source and pseudonymized databases without orphaning other users' interactions.
* **Enrich and Label:** Apply computed features (language detection, toxicity, sentiment, embeddings, LLM labeling) via local models or hosted APIs, with a built-in privacy preprocessor that redacts PII before external transmission.
* **Build Networks:** Generate user interaction, entity co-occurrence, bipartite, and coaction graphs. Temporal window support for studying network evolution.
* **Cross-Platform Analysis:** Attach multiple per-dataset databases into one DuckDB connection with `MultiStore` and join or union across them with plain SQL.
* **Scale:** Benchmarked to 10M input records (86M DB rows, 44.6 GB). Single-threaded throughput of ~360 rec/s; up to 5.9x speedup with 8 parallel workers. Query latency from 0.33 ms (indexed point lookup) to 2,623 ms (unindexed sequential scan baseline).


## Prerequisites & Database Setup

**System Requirements:**
* Python 3.11+
* PostgreSQL 14.19+
* TimescaleDB Extension
* PostGIS Extension

### Database Installation
SMDT requires a PostgreSQL database with both the TimescaleDB and PostGIS extensions enabled.

<details>
<summary><strong>Click to expand detailed PostgreSQL, TimescaleDB & PostGIS Installation Guide</strong></summary>

### 1. Install PostgreSQL 14.19

**Windows**
1. Download version 14.19 from the [EDB PostgreSQL Archive](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).
2. Run the installer (Default port: `5432`). Set a password for the `postgres` user.
3. Add `C:\Program Files\PostgreSQL\14\bin` to your System `Path` environment variable.

**macOS (Homebrew)**
```bash
brew install postgresql@14
brew services start postgresql@14
brew link --force postgresql@14
```

**Linux (Ubuntu/Debian)**

```bash
sudo sh -c 'echo "deb [http://apt.postgresql.org/pub/repos/apt](http://apt.postgresql.org/pub/repos/apt) $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - [https://www.postgresql.org/media/keys/ACCC4CF8.asc](https://www.postgresql.org/media/keys/ACCC4CF8.asc) | sudo apt-key add -
sudo apt update
sudo apt install postgresql-14
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### 2. Install TimescaleDB

**Windows**

1.  Download the `.zip` for Windows (amd64) from [TimescaleDB Releases](https://github.com/timescale/timescaledb/releases).
    
2.  Extract the folder.
    
3.  Run PowerShell as Administrator, navigate to the folder, and run `.\setup.exe`.
    
4.  Restart the PostgreSQL service via `services.msc`.
    

**macOS**

```bash
brew tap timescale/tap
brew install timescaledb
timescaledb-tune --quiet --yes
brew services restart postgresql@14
```

**Linux**
```bash
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt-get update
sudo apt install timescaledb-2-postgresql-14
sudo timescaledb-tune --quiet --yes
sudo systemctl restart postgresql
```

### 3. Install PostGIS

**Windows**

1.  Open the **Stack Builder** utility (installed with PostgreSQL).
    
2.  Select your PostgreSQL 14 installation.
    
3.  Expand **Spatial Extensions** and check **PostGIS 3.x Bundle for PostgreSQL 14**.
    
4.  Follow the prompts to install, then restart the PostgreSQL service.
    

**macOS**

```bash
brew install postgis
brew services restart postgresql@14
```

**Linux** 

```bash
sudo apt install postgresql-14-postgis-3
sudo systemctl restart postgresql
```

### 4. Initialize Database

Run the following SQL commands to create the database and enable the extensions:

**Connect to Postgres:**

```bash
# Mac/Linux
psql -U postgres

# Windows
psql -U postgres
```

**Run SQL:** 

```sql
CREATE DATABASE project_db;
CREATE USER project_user WITH ENCRYPTED PASSWORD 'your_password_here';
GRANT ALL PRIVILEGES ON DATABASE project_db TO project_user;

\c project_db

-- Enable Extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- Verify installation
\dx
```

</details>


## Installation & Quickstart

This project uses `uv` for fast Python package management. For detailed instructions, refer to the [uv Installation Guide](https://docs.astral.sh/uv/getting-started/installation/).

1.  **Clone the Repository**
    ```bash
    git clone [https://github.com/ViralLab/SMDT](https://github.com/ViralLab/SMDT)
    cd SMDT
    ```
    
2.  **Initialize Environment**
 
    ```bash
    uv init
    uv venv
    source venv/bin/activate  # On Windows: .venv\Scripts\activate
    ```
    
3.  **Install Dependencies**
        
    ```bash
    uv sync
    ```
    
4.  **Configure Environment**
    
    Create a `.env` file or set environment variables for your database connection:
        
    ```
    DEFAULT_DB_NAME=project_db
    DB_USER=project_user
    DB_PASSWORD=your_password_here
    DB_HOST=localhost
    DB_PORT=5432
    ```
   

## Usage

### 1. Standardize Raw Exports

Convert raw JSONL data using the pipeline API:

```python
from smdt.io.readers import discover
from smdt.ingest.plan import plan_directories
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.store.standard_db import StandardDB
from smdt.standardizers import TwitterV2Standardizer
from smdt.store.models import Accounts, Posts, Entities

discover()
plan = plan_directories(roots=["/data/twitter/"], include=("*twitter*.jsonl",))
db = StandardDB("my_dataset", initialize=True)

run_pipeline(
    plan, db, TwitterV2Standardizer(),
    config=PipelineConfig(
        batch_size=100_000,
        on_conflict={Accounts: "DO NOTHING", Posts: "DO NOTHING", Entities: "DO NOTHING"},
    ),
)
```

See [`site/recipes/standardizing-twitter-v2.md`](site/recipes/standardizing-twitter-v2.md) for a complete walkthrough.

### 2. Pseudonymize and Protect Data

Transform a source database into a pseudonymized destination with configurable per-column policies:

```python
from smdt.pseudonymizer import Pseudonymizer, PseudonymizeConfig, Algorithm, DEFAULT_POLICY
from smdt.config import PseudonymizationVariables

pseudo_vars = PseudonymizationVariables()

cfg = PseudonymizeConfig(
    src_db_name="my_dataset",
    dst_db_name="my_dataset_pseudo",
    pepper=pseudo_vars.pepper,
    algorithm=Algorithm.SHA256,
    ask_reinit=True,
    chunk_rows=5_000,
)

Pseudonymizer(cfg, DEFAULT_POLICY).run()
```

See [`site/recipes/pseudonymization.md`](site/recipes/pseudonymization.md) for PII detection with Presidio, parallel processing, and custom policies.

### 3. Inspect Data Quality

Check the completeness and schema distributions of your normalized tables.

```python
from smdt.store.standard_db import StandardDB
from smdt.inspector.inspector import Inspector, report_schemas

db = StandardDB("my_dataset", initialize=False)
ins = Inspector(db)
report_schemas([ins], only_tables=['posts', 'actions', 'accounts'])
```

### 4. Build Networks

Generate interaction graphs for analysis.

```python
from smdt.config import DBConfig
from smdt.store.standard_db import StandardDB
from smdt.networks.api import user_interaction, entity_cooccurrence

cfg = DBConfig()
db = StandardDB(db_name="mydb", cfg=cfg)

# User interaction network (who quoted whom)
# Result is a DataFrame with src, dst, weight, edge_type
result = user_interaction(db, interaction="QUOTE", weighting="count")
print(result.edges.head())

# Export to Parquet for Gephi/NetworkX
result.edges.to_parquet("edges.parquet")
```

### 5. Enrich Data

Apply computed features (language detection, toxicity, embeddings, LLM labels, ...) and store results in `post_enrichments`/`account_enrichments`. Local enrichers run entirely in-process:

```python
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher

db = StandardDB(db_name="mydb")

# Local: no network calls, no API key needed
run_enricher("language_detection", db=db)
```

Server-backed enrichers (LLMs, embeddings APIs) use provider factories to cut config boilerplate, plus an optional built-in privacy layer to redact/hash post content before it leaves the machine:

```python
import os
from smdt.enrichers.text_generation import TextGenerationConfig

config = TextGenerationConfig.for_openai(
    model="gpt-4o-mini",
    api_key=os.environ["OPENAI_API_KEY"],
    user_template="Classify the sentiment of this post: {body}",
    privacy_fields=["body"],  # optional: redact PII before it leaves the machine
    pepper=os.environ["PSEUDONYMIZATION_PEPPER"].encode(),
)
run_enricher("text_generation", db=db, config=config)
```

See [`site/recipes/enrichment/nlp.md`](site/recipes/enrichment/nlp.md) for one full example per provider (OpenAI, Anthropic, Gemini, Ollama, Hugging Face).

### 6. Cross-Platform Analysis

Each dataset lives in its own database, but they all share the same schema. `MultiStore` attaches multiple datasets into one DuckDB connection so you can join or union across them with plain SQL:

```python
from smdt.multistore import MultiStore

with MultiStore() as ms:
    ms.attach("twitter", db_name="twitter_db")
    ms.attach("bluesky", db_name="bluesky_db")

    df = ms.query("""
        SELECT tw.username, tw.follower_count AS tw_followers, bs.follower_count AS bs_followers
        FROM twitter.accounts tw
        JOIN bluesky.accounts bs ON tw.username = bs.username
    """)
```

See [`site/recipes/analysis/multistore.md`](site/recipes/analysis/multistore.md) for identity-linking patterns, mixing in Parquet/CSV files, and a PostGIS `location`-column caveat.

### 7. GDPR Erasure

Handle deletion requests by recomputing a pseudonym for a given identity and locating their data across databases. Supports hard deletion or in-place scrubbing:

```python
from smdt.pseudonymizer import Eraser, ErasureTarget, ErasureMode
from smdt.config import PseudonymizationVariables

pseudo_vars = PseudonymizationVariables()

eraser = Eraser(
    targets=[
        ErasureTarget(db_name="my_dataset", mode=ErasureMode.DELETE, is_pseudonymized=False),
        ErasureTarget(db_name="my_dataset_pseudo", mode=ErasureMode.SCRUB, is_pseudonymized=True),
    ],
    pepper=pseudo_vars.pepper,
)

eraser.erase("real_account_id_123", identity_column="account_id")
```

No reverse-mapping table is stored. Erasure works by forward recomputation of the pepper-keyed hash.


## Project Structure

```
SMDT/
├── src/smdt/                  # Main package
│   ├── pseudonymizer/         # Hashing, redaction, PII detection, GDPR erasure
│   ├── config.py              # Configuration (DB, pseudonymization pepper)
│   ├── enrichers/             # NLP enrichment framework (local + server adapters)
│   ├── ingest/                # Ingestion pipelines and deduplication logic
│   ├── inspector/             # Data quality inspection utilities
│   ├── io/                    # Streaming readers (JSONL, CSV, Parquet, ZIP, TAR)
│   ├── multistore/            # Cross-dataset analysis via DuckDB (MultiStore)
│   ├── networks/              # Network builders and streaming helpers
│   ├── standardizers/         # Platform-specific normalizers (15+ variants)
│   └── store/                 # DB models, schema, and StandardDB abstraction
├── benchmark_scripts/         # Ingestion, query, and pseudonymization benchmarks
├── site/                      # Documentation site (VitePress)
├── tests/
│   ├── unit/                  # Fast unit tests (no external deps)
│   └── integration/           # DB integration tests (requires Postgres)
├── standardizer_scripts/      # Production driver scripts for specific datasets
├── prompt.yml                 # Prompt templates for enrichers
└── pyproject.toml             # Project metadata and dependencies
```


## Data Model

SMDT normalizes data into five core tables. If creating a new standardizer, ensure your output maps to these fields:

| Table | Key Fields |
| :--- | :--- |
| **Communities** | `community_id`, `community_type` (CHANNEL/GROUP), `community_username`, `community_name`, `bio`, `is_public`, `member_count`, `post_count`, `profile_image_url`, `owner_account_id`, `platform`, `created_at`, `retrieved_at` |
| **Accounts** | `account_id`, `username`, `profile_name`, `bio`, `location`, `post_count`, `friend_count`, `follower_count`, `is_verified`, `profile_image_url`, `platform`, `created_at`, `retrieved_at` |
| **Posts** | `post_id`, `account_id`, `conversation_id`, `community_id`, `body`, `like_count`, `dislike_count`, `view_count`, `share_count`, `comment_count`, `quote_count`, `bookmark_count`, `location`, `platform`, `created_at`, `retrieved_at` |
| **Entities** | `account_id`, `community_id`, `post_id`, `body`, `entity_type` (e.g. HASHTAG), `created_at`, `retrieved_at` |
| **Actions** | `originator_account_id`, `originator_post_id`, `target_account_id`, `target_post_id`, `originator_community_id`, `target_community_id`, `action_type` (e.g. SHARE), `created_at`, `retrieved_at` |

`platform` is the canonical source platform (e.g. `"twitter"`, `"weibo"`) and drives platform-aware behavior elsewhere in the toolkit (e.g. mention/hashtag pattern selection in `pseudonymizer`).

Beyond the core tables, a few auxiliary tables support enrichment and dataset bookkeeping rather than standardizer output:

| Table | Key Fields | Written by |
| :--- | :--- | :--- |
| **PostEnrichments** (`post_enrichments`) | `post_id`, `model_id`, `body` (JSONB), `created_at`, `retrieved_at` | The [enrichment framework](#5-enrich-data): one row per `(post_id, model_id)`. |
| **AccountEnrichments** (`account_enrichments`) | `account_id`, `model_id`, `body` (JSONB), `created_at`, `retrieved_at` | The enrichment framework: one row per `(account_id, model_id)`. |
| **DatasetMeta** (`dataset_meta`) | `platform`, `standardizer_name`, `dataset_description`, `created_at`, `updated_at` | One row per database, describing the dataset ingested into it. |

--- 
----------

## Performance

Benchmarked against a real 10M-post Twitter dataset (86M normalized DB rows, 44.6 GB):

| Metric | Single-worker | 8-worker parallel |
|---|---|---|
| Ingestion throughput | ~360 rec/s (flat across scale) | up to 2,092 rec/s (5.9x speedup) |
| Peak memory | 462 MB | 1,376 MB |
| Query latency (indexed point lookup) | 3.1 ms (p50) | n/a |
| Query latency (unindexed scan) | 2,623 ms (p50) | n/a |
| Pseudonymization (full 86M rows) | 8.7 hours | 2.7 hours (3.2x speedup) |

See [`benchmark_scripts/`](benchmark_scripts/) for the full benchmark suite and [`benchmark_scripts/benchmark_summary.pdf`](benchmark_scripts/benchmark_summary.pdf) for the summary figure.


## Development and Testing

Please ensure you follow existing code styles and add tests for new behaviors.

### Running Tests

Integration tests require a database. Set `TEST_DATABASE_URL` in your `.env.test`.
```bash
# Run all tests
uv run python -m pytest

# Run only unit tests (fast, no DB required)
uv run python -m pytest tests/unit

# Run only integration tests
uv run python -m pytest tests/integration

# Verbose output
uv run python -m pytest -v
```

### Adding a Platform

To add support for a platform like Threads:

1.  Create a new module in `src/smdt/standardizers/threads/` that maps raw data to the normalized models.
    
2.  Update `src/smdt/standardizers/__init__.py` to import and expose the new standardizer.
    

## Citation

```bibtex
@article{najafi2025smdt,
  title   = {Social Media Data Toolkit: Standardization and Pseudonymization of Social Network Datasets},
  author  = {Najafi, Ali and Iannucci, Stefano and Kivel\"a, Mikko and Varol, Onur},
  journal = {To appear},
  year    = {2025}
}
```

## License

MIT License. See [`LICENSE`](LICENSE) for details.
