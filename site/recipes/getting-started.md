# Getting Started

This guide walks you through the initial steps of using the SMDT package.

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

## 3. Using Standardizers

SMDT provides standardizers to clean and normalize data from various social media platforms. Here is an example using the `TwitterUSCStandardizer`.

```python
from smdt.standardizers import TwitterUSCStandardizer

# Initialize the standardizer
standardizer = TwitterUSCStandardizer()

# Example raw data (this will vary based on the platform)
raw_tweet = {
    "text": "Hello world! #SMDT @ViralLab",
    "created_at": "2023-10-27T10:00:00Z",
    "id": "1234567890"
}

# Standardize the data
# (Note: Specific usage depends on the standardizer implementation)
# standardized_data = standardizer.standardize(raw_tweet)
# print(standardized_data)
```

## 4. Connecting to the Database

You can connect to your TimescaleDB-enabled PostgreSQL database using `StandardDB`.

```python
from smdt.store.standard_db import StandardDB
import os

# Initialize database connection
# Note: Ensure you pass the correct database name
db_name = os.getenv("DEFAULT_DB_NAME", "smdt_db")
# set initialize=False to skip auto initialization if you already initialized the database
db = StandardDB(db_name, initialize=False) 

try:
    # Check connection
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            print(f"Connected to database '{db_name}' successfully!")
except Exception as e:
    print(f"Database connection failed: {e}")
```

## Next Steps

Now that you can connect to the database and run a standardizer, learn how to scale up to thousands of files:

- **[Using Ingestion Pipelines](./using-pipelines.md)**  
  Process entire directories of JSON/JSONL files in batches with checkpointing.
- **[Standardizing Twitter API v2 Data](./standardizing-twitter-v2.md)**  
  A complete end-to-end example from data generation to database insertion.
