---
description: Sanity-check your ingested data with SMDT's database inspector: row counts, per-column completeness, and enum distributions before relying on the data.
---

# Using the Database Inspector

SMDT includes a built-in `Inspector` tool to help you visualize the contents and quality of your database. It provides a terminal-friendly report showing:

- **Completeness**: How many rows have non-null values for each column.
- **Enumerations**: Distributions of values for categorical fields (like `action_type`).
- **Row Counts**: Estimated or exact row counts for each table.
- **Structured Output**: Every report is also saved as a JSON snapshot by default, for later comparison.

This is particularly useful for verifying data ingestion pipelines or exploring a new dataset.

## Basic Usage

The inspector is typically used via a script or an interactive Python session.

### Running a Report

Here's how to generate a report for your default database:

```python
import os
from smdt.store.standard_db import StandardDB
from smdt.inspector import Inspector, report_schemas

def main():
    # 1. Connect to your database
    db_name = os.getenv("DEFAULT_DB_NAME", "smdt_db")
    db = StandardDB(db_name, initialize=False)

    # 2. Create an Inspector instance
    # You can inspect the 'public' schema or any other schema you use
    inspector = Inspector(db, schema="public")

    # 3. Generate the report
    # report_schemas prints to stdout AND writes a JSON snapshot to
    # inspector_snapshots/ by default -- see "Saving Structured Output" below
    print(f"Inspecting database: {db_name} (public schema)\n")
    
    # Pass a list of inspectors to compare multiple DBs or schemas if needed
    report_schemas([inspector])

if __name__ == "__main__":
    main()
```

## Comparisons

The `report_schemas` function accepts a list of inspectors, allowing you to compare two databases side-by-side. This is excellent for checking if a migration or backup was successful.

```python
# Compare Prod vs Dev
prod_db = StandardDB("production_db", initialize=False)
dev_db = StandardDB("development_db", initialize=False)

insp_prod = Inspector(prod_db, schema="public")
insp_dev = Inspector(dev_db, schema="public")

# The report will show columns for both databases side-by-side
report_schemas([insp_prod, insp_dev])
```

### Comparing the Same Time Period Across Databases

Combine `time_window` with the multi-inspector comparison above to compare
the *same* time slice across several databases -- e.g. how January 2023
looked across different platform datasets:

```python
from datetime import datetime, timezone

jan_2023 = (datetime(2023, 1, 1, tzinfo=timezone.utc), datetime(2023, 2, 1, tzinfo=timezone.utc))

insp_twitter = Inspector(twitter_db, schema="public", time_window=jan_2023)
insp_bluesky = Inspector(bluesky_db, schema="public", time_window=jan_2023)

report_schemas([insp_twitter, insp_bluesky], only_tables=["posts"])
```

Each inspector filters to `created_at >= start AND created_at < end` for
any table that has a `created_at` column (tables without one are
unaffected). This stays cheap even on a huge table: TimescaleDB's chunk
exclusion means a narrow window only touches the chunks that overlap it,
regardless of total table size. The comparison header shows the window
next to each database's label so it's clear what's being compared.

## Filtering

If you only want to inspect specific tables, you can filter the output to reduce noise.

```python
# Only inspect the 'posts' and 'accounts' tables
report_schemas(
    [inspector], 
    only_tables=["posts", "accounts"]
)
```

## Large Tables: Sampled Mode

By default, `Inspector` computes exact completeness/enum stats with a full
table scan -- correct, but slow on a large hypertable (measured: 18+ seconds
for a single 64M-row table). For a quick look at a large dataset, sample
instead:

```python
inspector = Inspector(db, schema="public", sample_pct=1.0)  # sample ~1% of rows
```

This uses `TABLESAMPLE SYSTEM` for completeness/enum queries and the
database's own row-count estimate instead of an exact `COUNT(*)` --
percentages stay statistically reliable, but the table is now marked
`(sampled estimate)` in the report so it's clear the absolute counts are
extrapolated, not exact. Leave `sample_pct` unset (the default) when you
need exact numbers, e.g. verifying a migration.

## Saving Structured Output

`report_schemas` doesn't just print to the terminal -- by default it also
writes one JSON snapshot file per inspector to `inspector_snapshots/`
(created automatically), named after that inspector's db/schema (and its
`time_window`/`sample_pct` if set, so different comparisons don't collide
on the same file):

```python
report_schemas([insp_prod, insp_dev])
# -> inspector_snapshots/production_db__public.json
# -> inspector_snapshots/development_db__public.json
```

Load one back later -- the reconstructed data is usable exactly like a live
snapshot (same `TableStat`/`ColStat` objects, `enum_counts` included):

```python
from smdt.inspector import load_snapshot

metadata, snap = load_snapshot("inspector_snapshots/production_db__public.json")
print(metadata["timestamp"], metadata["sample_pct"], metadata["time_window"])
print(snap["posts"].est_rows, snap["posts"].columns["body"].completeness)
```

`metadata` records the settings the snapshot was taken with (`db_name`,
`schema`, `sample_pct`, `time_window`) so a loaded snapshot is never
ambiguous about what it actually measured.

To change where files go, or skip saving entirely:

```python
report_schemas([inspector], save_dir="my_snapshots")  # different directory
report_schemas([inspector], save=False)               # print only, no files
```

If you want a snapshot without going through `report_schemas` at all (e.g.
in a script with no printed report), `Inspector.snapshot_and_save(path)` or
the module-level `save_snapshot(inspector, snap, path)` save to an exact
path of your choosing; `snapshot()` on its own still returns an in-memory
result with no file I/O.

## Understanding the Output

The output uses color-coding to highlight data quality:

- **Green**: High completeness (>80%).
- **Yellow**: Medium completeness (20-80%).
- **Red**: Low completeness (<20%).

For `ENUM` columns (like `action_type` or `community_type`), the report will automatically show a breakdown of the top values and their percentages.

### Example Output

```text
Table: actions (~1,250 rows)
--------------------------------------------------
column / type        |          public          |
--------------------------------------------------
action_id : uuid     | 1,250/1,250 (100.0%)     |
action_type : text   | 1,250/1,250 (100.0%)     |
  ↳ RETWEET          |    850 (68.0%)           |
  ↳ REPLY            |    300 (24.0%)           |
  ↳ QUOTE            |    100 ( 8.0%)           |
created_at : timest..| 1,250/1,250 (100.0%)     |
body : text          |    400/1,250 ( 32.0%)    |
...
```
