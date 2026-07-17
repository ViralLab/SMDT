---
description: Set up SMDT, connect to a database, run your first standardizer, and verify the result. The quickest path from raw JSONL to normalized data.
---

# Getting Started

This guide walks you through the initial steps of using SMDT: connecting to a database, running your first standardizer, and verifying the result.

## Quickstart

The shortest path from a raw JSONL file to a database:

```python
from smdt.io.readers import discover
from smdt.ingest.plan import plan_directories
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.store.standard_db import StandardDB
from smdt.standardizers import TwitterV2Standardizer
from smdt.store.models import Accounts, Posts, Entities

discover()
plan = plan_directories(roots=["/path/to/your/data"], include=("*.jsonl",))
db = StandardDB("my_first_db", initialize=True)
run_pipeline(plan, db, TwitterV2Standardizer(), config=PipelineConfig(
    on_conflict={Accounts: "DO NOTHING", Posts: "DO NOTHING", Entities: "DO NOTHING"},
))
```

That is the core pattern. The rest of this guide explains each step in detail.

## 1. Environment Verification

Ensure that you have installed the package and that your Python environment is active.

```bash
uv run python -c "import smdt; print('SMDT installed successfully')"
```

## 2. Basic Configuration

Make sure your `.env` file is set up correctly in the root of your project:

```bash
DEFAULT_DB_NAME=smdt_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

## 3. Using a Standardizer

SMDT provides standardizers to normalize data from various social media platforms. Each standardizer takes a raw record (a Python `dict`) and returns normalized model objects ready for insertion.

```python
from smdt.standardizers import TwitterV2Standardizer
from smdt.standardizers.base import SourceInfo

standardizer = TwitterV2Standardizer()

raw_tweet = {
    "data": {
        "id": "1234567890",
        "text": "Hello world! #SMDT @VarolLab",
        "author_id": "987654321",
        "created_at": "2023-10-27T10:00:00Z",
        "public_metrics": {"retweet_count": 5, "reply_count": 2,
                           "like_count": 42, "quote_count": 1}
    },
    "includes": {
        "users": [{"id": "987654321", "name": "Varol Lab",
                   "username": "VarolLab"}]
    }
}

models = standardizer.standardize((raw_tweet, SourceInfo(path="")))
for m in models:
    print(type(m).__name__, m.insert_values())
```

This prints the generated Accounts, Posts, Entities, and Actions rows ready to be written to the database.

## 4. Connecting to the Database

You can connect to your TimescaleDB-enabled PostgreSQL database using `StandardDB`.

```python
from smdt.store.standard_db import StandardDB
import os

db_name = os.getenv("DEFAULT_DB_NAME", "smdt_db")
db = StandardDB(db_name, initialize=False)

with db.connect() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print(f"Connected to database '{db_name}' successfully")
```

## Next Steps

Now that you can connect to the database and run a standardizer, the next step is scaling that up to thousands of files with [Using Ingestion Pipelines](./using-pipelines.md).

That is the first of several steps after ingestion: enrichment, privacy, and analysis. See the [Recipes Overview](./index.md) for the full path.
