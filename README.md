
# SMDT — Social Media Data Toolkit

![diagram](assets/toolkit_diagram.png?raw=true)

**SMDT** is a lightweight toolkit designed for ingesting, normalizing, enriching, and analyzing social-media data. It prioritizes streaming-friendly processing for large datasets, providing builders, utilities, and NLP hooks to transform raw exports (JSONL/CSV) into edge lists and NetworkX graphs.

The goal is to provide a flexible, consistent data model to enable reproducible data analysis across different social platforms.

## Table of Contents
- [Features](#features)
- [Prerequisites & Database Setup](#prerequisites--database-setup)
- [Installation & Quickstart](#installation--quickstart)
- [Usage](#usage)
    - [1. Standardize Raw Exports](#1-standardize-raw-exports)
    - [2. Inspect Data Quality](#2-inspect-data-quality)
    - [3. Build Networks](#3-build-networks)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Development & Testing](#development--testing)

---

## Features

* **Ingest & Standardize:** Convert raw platform exports (Twitter/X, Bluesky, TruthSocial) into normalized SQL tables (`Communities`, `Accounts`, `Posts`,  `Actions`, `Entities`).
* **Anonymize & Redact:** Remove or pseudonymize sensitive fields using policy-driven helpers before sharing datasets.
* **Enrich & Label:** Apply computed features (language detection, toxicity scores, embeddings) via a local or server-backed enrichment framework.
* **Build Networks:** Generate edge lists (User–User, Entity–Cooccurrence) and bipartite graphs compatible with NetworkX and Gephi.
* **Scale:** Designed for streaming; handles datasets that do not fit in memory using incremental builders and Parquet exports.

---

## Prerequisites & Database Setup

**System Requirements:**
* Python 3.11+
* PostgreSQL 14.19+
* TimescaleDB Extension

### Database Installation
SMDT requires a PostgreSQL database with the TimescaleDB extension enabled.

<details>
<summary><strong>Click to expand detailed PostgreSQL & TimescaleDB Installation Guide</strong></summary>

### 1. Install PostgreSQL 14.19

**Windows**
1.  Download version 14.19 from the [EDB PostgreSQL Archive](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).
2.  Run the installer (Default port: `5432`). Set a password for the `postgres` user.
3.  Add `C:\Program Files\PostgreSQL\14\bin` to your System `Path` environment variable.

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
2.  Extract, run PowerShell as Administrator, navigate to the folder, and run `.\setup.exe`.
3.  Restart the PostgreSQL service via `services.msc`.

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

### 3. Initialize Database
Run the following SQL commands to create the database and enable the extension:

```bash
# Connect to Postgres
psql -U postgres
```

```sql
CREATE DATABASE project_db; 
CREATE USER project_user WITH ENCRYPTED PASSWORD 'your_password_here'; 
GRANT ALL PRIVILEGES ON DATABASE project_db TO project_user; 
\c project_db 
CREATE EXTENSION IF NOT EXISTS timescaledb; 
-- Verify installation 
\dx timescaledb
```
</details>
 
## Installation & Quickstart

This project uses `uv` for dependency management.

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
    ```env	
    DEFAULT_DB_NAME=postgres
    DB_USER=username
    DB_OWNER=username
    DB_PASSWORD=password
    DB_HOST=localhost
    DB_PORT=PORT
    ```

---

## Usage

### 1. Standardize Raw Exports
Convert raw JSONL data into normalized objects (Posts, Accounts, Entities, Actions).

```python
from smdt.standardizers.twitter.twitter_v2 import TwitterV2Standardizer
from smdt.io.readers.jsonl import JSONLReader

standardizer = TwitterV2Standardizer()

# Stream through a JSONL export
for record in JSONLReader("data/tweets.jsonl"):
    for model in standardizer.standardize(record):
        # model is an instance of Posts, Accounts, Entities, or Actions
        print(model)
```

### 2. Inspect Data Quality
Check the completeness and schema distributions of your normalized tables.

```python
from smdt.config import DBConfig
from smdt.store.standard_db import StandardDB
from smdt.inspector.inspector import Inspector, report_schemas

cfg = DBConfig() # reads DB_* env vars
db = StandardDB(db_name=cfg.default_dbname or 'mydb', cfg=cfg)
ins = Inspector(db, schema=getattr(cfg, 'owner', 'public'))

report_schemas([ins], only_tables=['posts', 'actions', 'accounts'])
```

### 3. Build Networks
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
## Project Structure

```text
SMDT/
├── src/smdt/                  # Main package
│   ├── anonymizer/            # Redaction and pseudonymization utilities
│   ├── config.py              # Configuration (DB, anonymization)
│   ├── enrichers/             # Text enrichment framework (local + server adapters)
│   ├── ingest/                # Ingestion pipelines and deduplication logic
│   ├── inspector/             # Data quality inspection utilities
│   ├── io/                    # Streaming readers (JSONL, CSV, ZIP)
│   ├── networks/              # Network builders and streaming helpers
│   ├── standardizers/         # Platform-specific normalizers (Twitter, Bluesky, etc.)
│   └── store/                 # DB models and StandardDB abstraction
├── tests/
│   ├── unit/                  # Fast unit tests (no external deps)
│   └── integration/           # DB integration tests (requires Postgres)
├── prompt.yml                 # Prompt templates for enrichers
└── pyproject.toml             # Project metadata and dependencies
``` 
 
## Data Model

SMDT normalizes data into four primary tables. If creating a new standardizer, ensure your output maps to these fields:

| Table | Key Fields |
| :--- | :--- |
| **Communities** | `community_id`, `community_type`(CHANNEL/GROUP), `community_username`, `community_name`, `bio`, `is_public`, `member_count`, `post_count`, `profile_image_url`, `owner_account_id`, `created_at`, `retrieved_at`  |
| **Accounts** | `account_id`, `username`, `profile_name`, `bio`, `location`, `post_count`,`friend_count`, `follower_count`, `is_verified`, `profile_image_url`,  `created_at`, `retrieved_at`|
| **Posts** | `post_id`, `account_id`, `conversation_id`, `community_id`, `body`, `like_count`, `dislike_count`, `view_count`, `share_count`, `comment_count`, `quote_count`, `bookmark_count`|
| **Entities** | `account_id`, `community_id`, `post_id` , `body`, `entity_type` (e.g HASHTAG), `created_at`, `retrieved_at` |
| **Actions** | `originator_account_id`, `originator_post_id`, `target_account_id`, `target_post_id`, `originator_community_id`, `target_community_id`, `action_type` (e.g SHARE), `created_at`, `retrieved_at` |

--- 

## Development & Testing

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

## License
[License Information Here]
