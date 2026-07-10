# Using Ingestion Pipelines

The SMDT ingestion pipeline is a robust system designed to process large volumes of data files efficiently. It handles:

- **File Discovery**: Recursively finding files matching specific patterns.
- **Batch Processing**: Grouping records for efficient database insertion.
- **Error Handling**: Managing failures at the record or file level without crashing the entire process.
- **Deduplication**: Automatically checking for duplicate records based on model definitions.
- **Checkpointing**: Resuming from where you left off if a long-running job is interrupted.

## Basic Usage

The pipeline requires three main components: a **Database Connection**, a **Standardizer**, and an **Ingestion Plan**.

### 1. The Script

Here is a complete example of a script that ingests JSONL files using a hypothetical `MyStandardizer`.

```python
import logging
from pathlib import Path
from smdt.store.standard_db import StandardDB
from smdt.ingest.plan import plan_directories
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.standardizers import TwitterV2Standardizer

# Configure logging to see progress and errors
logging.basicConfig(level=logging.INFO)

def main():
    # Initialize database wrapper
    # Note: Replace 'smdt_db' with your actual database name
    db = StandardDB("smdt_db")
    
    # 2. Initialize your standardizer
    standardizer = TwitterV2Standardizer()
    
    # 3. Create an ingestion plan
    # Scans 'data/raw' for all files ending in .jsonl (recursively)
    plan = plan_directories(
        roots=["data/raw"],
        include=["*.jsonl"]
    )
    
    print(f"Found {len(plan.files)} files to process.")
    
    # 4. Configure pipeline settings (optional)
    config = PipelineConfig(
        batch_size=1000,       # Records per batch
        chunk_size=50000,      # DB insert chunk size
        checkpoint_file=".pipeline_checkpoint" # Save progress here
    )
    
    # 5. Run the pipeline
    run_pipeline(
        plan=plan,
        db=db,
        standardizer=standardizer,
        config=config
    )

if __name__ == "__main__":
    main()
```

## Configuration Options

The `PipelineConfig` class allows you to tune performance and behavior.

### Processing Settings

| Option | Default | Description |
| :--- | :--- | :--- |
| `batch_size` | `1000` | Number of records to accumulate in memory before flushing to the database. Larger values increase memory usage but may speed up insertion. |
| `chunk_size` | `100000` | Maximum number of values (parameters) in a single SQL `INSERT` statement. Adjust this if you encounter DB limits. |
| `num_workers` | `1` | Number of files to process concurrently via a process pool. `1` (the default) is the original single-process behavior. |

### Parallel Ingestion

If your dataset is split across many files, you can process several of them concurrently:

```python
config = PipelineConfig(
    num_workers=8  # process up to 8 files at once
)
```

Each worker reads, standardizes, and flushes its own file independently, with its own database connection — this is where the speedup comes from if your standardizer's per-record work is CPU-bound. `num_workers=1` is unchanged from single-process behavior, so this is purely opt-in.

::: warning Deduplication is scoped per file in parallel mode
Deduplication only happens within a single flush's buffer. In single-process mode that buffer can span many files (up to `batch_size` records); in parallel mode each worker's buffer is scoped to just its own file. A duplicate record spanning two different files will produce two rows instead of one, unless the model's table has a database-level unique constraint for `on_conflict` to catch it (true for `Accounts`/`Communities`/the `*Enrichments` tables by default, not for `Posts`/`Entities`/`Actions`).
:::

Progress reporting is also coarser in parallel mode: `flush` events are reported once per model per file, rather than streamed live at every intra-file buffer flush the way they are with `num_workers=1`.

### Checkpointing

Long-running ingestion jobs can be interrupted by network issues or system reboots. Checkpointing allows you to resume without re-processing completed files.

```python
config = PipelineConfig(
    checkpoint_file="ingest_progress.txt",
    reset_checkpoint=False  # Set to True to force processing all files again
)
```

### Conflict Resolution

By default, SMDT attempts to ignore duplicates using `ON CONFLICT DO NOTHING`. You can customize this behavior for specific models using the `on_conflict` dictionary.

```python
from smdt.store.models import Accounts

config = PipelineConfig(
    on_conflict={
        Accounts: "UPDATE"  # Update account info if it already exists
    }
)
```

::: warning Note
The "UPDATE" strategy requires that your database schema and standardizer support upserts for the given model.
:::

### File Formats & Readers

The pipeline automatically selects a reader based on file extension (e.g., `.json`, `.csv`, `.tsv`). You can pass extra arguments to the underlying pandas/standard reader via `reader_kwargs`.

```python
config = PipelineConfig(
    reader_kwargs={
        "csv": {"sep": ";", "encoding": "latin1"} # for semicolon-separated files
    }
)
```

## Advanced: Archive Support

The `plan_directories` function automatically detects and plans for files inside archives like `.zip` or `.tar.gz` if you enable it.

```python
plan = plan_directories(
    roots=["data/archives"],
    include=["**/*.zip", "**/*.tar.gz"],
    # The pipeline handles inspecting contents inside these archives
)
```
