# Using the Database Inspector

SMDT includes a built-in `Inspector` tool to help you visualize the contents and quality of your database. It provides a terminal-friendly report showing:

- **Completeness**: How many rows have non-null values for each column.
- **Enumerations**: Distributions of values for categorical fields (like `action_type`).
- **Row Counts**: Estimated or exact row counts for each table.

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

    # 3. Generate and print the report
    # The report_schemas function handles formatting and printing to stdout
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

## Filtering

If you only want to inspect specific tables, you can filter the output to reduce noise.

```python
# Only inspect the 'posts' and 'accounts' tables
report_schemas(
    [inspector], 
    only_tables=["posts", "accounts"]
)
```

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
