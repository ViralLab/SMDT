# Standardizing Twitter API v2 Data

This recipe walks through ingesting Twitter API v2 data (JSONL format) into SMDT. The same pattern applies to any platform: point at your files, create a plan, and run the pipeline.

## Quickstart

```python
from smdt.io.readers import discover
from smdt.ingest.plan import plan_directories, print_plan
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.store.standard_db import StandardDB
from smdt.standardizers import TwitterV2Standardizer
from smdt.store.models import Accounts, Posts, Entities

discover()
plan = plan_directories(roots=["/path/to/your/twitter/data"], include=("*.jsonl",))
print_plan(plan)

db = StandardDB("twitter_v2_db", initialize=True)

run_pipeline(
    plan, db, TwitterV2Standardizer(),
    config=PipelineConfig(
        batch_size=1000,
        on_conflict={Accounts: "DO NOTHING", Posts: "DO NOTHING", Entities: "DO NOTHING"},
    ),
)
```

## Step by Step

### 1. Point at Your Data

`plan_directories` scans a directory for files matching the `include` pattern. It resolves the correct reader for each file automatically. Run `print_plan` to preview what will be ingested before committing.

```python
plan = plan_directories(roots=["/data/twitter/"], include=("*.jsonl",))
print_plan(plan)
# This shows a colorized table of files and readers. It asks for confirmation.
```

### 2. Initialize the Database

```python
db = StandardDB("twitter_v2_db", initialize=True)
```

`initialize=True` creates the schema if it does not already exist. For a second run on the same database, use `initialize=False`.

### 3. Run the Pipeline

The pipeline reads files, standardizes each record, deduplicates within each batch, and flushes to the database. The `on_conflict` parameter uses `DO NOTHING` so repeated ingestion of the same files is safe.

```python
run_pipeline(
    plan, db, TwitterV2Standardizer(),
    config=PipelineConfig(
        batch_size=1_000,
        chunk_size=1_000,
        on_conflict={Accounts: "DO NOTHING", Posts: "DO NOTHING", Entities: "DO NOTHING"},
    ),
)
```

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `roots` | required | List of directories to scan |
| `include` | `("*",)` | Glob pattern for files to include |
| `exclude` | `()` | Glob pattern for files to skip |
| `batch_size` | `100_000` | Rows to buffer before flushing to DB |
| `chunk_size` | `100_000` | Rows per database INSERT chunk |
| `on_conflict` | `None` | Per-model conflict resolution (e.g., `DO NOTHING`) |

::: tip Tuning hypertables for your data volume
`initialize=True` applies a default hypertable configuration sized for large Twitter-scale datasets. For smaller datasets, smaller chunk intervals reduce the number of chunks each query must scan:

```python
from dataclasses import replace
from datetime import timedelta
from smdt.store.schema_config import SchemaConfig

custom = SchemaConfig(tables={
    **SchemaConfig().tables,
    "posts": replace(SchemaConfig().tables["posts"], chunk_time_interval=timedelta(days=1)),
})
db = StandardDB("twitter_v2_db", initialize=True, hypertable_config=custom)
```
:::

## Expected Output

A successful run prints progress bars and flush summaries:

```
Ingestion plan:
  FILE  [✓] twitter_data.jsonl  → jsonl

Pipeline files: 100%|████████████████████| 1/1 [00:01<00:00]
[Progress] flush: {'model': 'Accounts', 'count': 152}
[Progress] flush: {'model': 'Posts', 'count': 500}
[Progress] flush: {'model': 'Entities', 'count': 1207}
[Progress] done: {'files': 1, 'records': 500, 'record_errors': 0, 'row_failures': 0}
```

## Prerequisites

- Database configured (see [Getting Started](./getting-started.md))
- Twitter API v2 JSONL files (one JSON object per line, with `data` and `includes` keys)

::: details Don't have Twitter data? Generate a sample
Create a small test file to verify your setup:

```python
import json, random
from datetime import datetime, timezone

with open("sample_twitter_v2.jsonl", "w") as f:
    for i in range(1, 6):
        tweet = {
            "data": {
                "id": str(1000 + i),
                "text": f"Sample tweet #{i} about #SMDT",
                "author_id": str(500 + (i % 2)),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "conversation_id": str(1000 + i),
                "public_metrics": {"retweet_count": random.randint(0, 100),
                                   "reply_count": random.randint(0, 50),
                                   "like_count": random.randint(0, 500),
                                   "quote_count": random.randint(0, 20)}
            },
            "includes": {
                "users": [{"id": str(500 + (i % 2)),
                           "name": f"User {500 + (i % 2)}",
                           "username": f"user_{500 + (i % 2)}"}]
            }
        }
        f.write(json.dumps(tweet) + "\n")
print("Generated sample_twitter_v2.jsonl with 5 records")
```
:::

## Next Steps

- Verify the ingested data with [Using the Database Inspector](./analysis/inspector.md)
- Apply [Pseudonymization](./pseudonymization.md) before sharing the dataset
- [Build networks](./networks/construction.md) from the ingested data
