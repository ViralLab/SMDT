# Building a Custom Standardizer

This recipe guides you through creating a custom standardizer to ingest data from a new source format (e.g., a custom CSV or JSON) into the SMDT database.

## Overview

All standardizers in SMDT inherit from the base `Standardizer` class. Your job is to implement the `standardize` method, which takes a raw record and converts it into a list of database models (like `Accounts`, `Posts`, `Actions`, etc.).

Once your standardizer is defined, you can plug it directly into the standard SMDT ingestion pipeline, and it will handle the reading, batching, and database insertion automatically.

## End-to-End Example

Below is a complete, runnable script. It defines a custom standardizer for a hypothetical CSV format, generates a dummy CSV file, and then runs the full SMDT pipeline to ingest the data into a database.

Create a file named `custom_ingestion.py` and run it:

```python
import os
import csv
from datetime import datetime, timezone
from typing import Any, Iterable, Tuple

from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models import Accounts, Posts
from smdt.store.standard_db import StandardDB
from smdt.ingest.plan import plan_directories
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.io.readers import discover

# ---------------------------------------------------------
# 1. Define the Custom Standardizer
# ---------------------------------------------------------
class MyCustomCSVStandardizer(Standardizer):
    """
    A custom standardizer that maps our specific CSV columns 
    (user_id, username, tweet_text, timestamp, likes) to SMDT models.
    """
    name = "my_custom_csv_standardizer"

    def standardize(
        self, input_record: Tuple[dict, SourceInfo]
    ) -> Iterable[Any]:
        
        record, source_info = input_record
        output_models = []

        # Extract data from the CSV row (which is passed as a dictionary)
        user_id = record.get("user_id")
        username = record.get("username")
        body = record.get("tweet_text")
        
        # We generate a unique post_id since our CSV doesn't have one
        post_id = f"{user_id}_{record.get('timestamp')}" 

        # Parse the timestamp into a timezone-aware UTC datetime
        try:
            dt_str = record.get("timestamp")
            created_at = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            created_at = created_at.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            # Skip records with invalid dates
            return []

        # Create the Account model
        account = Accounts(
            account_id=user_id,
            username=username,
            created_at=created_at, 
            profile_name=username, 
            retrieved_at=datetime.now(timezone.utc)
        )
        output_models.append(account)

        # Create the Post model
        post = Posts(
            post_id=post_id,
            account_id=user_id,
            body=body,
            created_at=created_at,
            like_count=int(record.get("likes", 0)),
            retrieved_at=datetime.now(timezone.utc)
        )
        output_models.append(post)

        return output_models

# ---------------------------------------------------------
# 2. Setup and Run the Pipeline
# ---------------------------------------------------------
def main():
    # Ensure SMDT knows how to read various file formats (like CSV)
    discover()

    # Create a dummy CSV file for this example
    csv_filename = "custom_data.csv"
    with open(csv_filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "username", "tweet_text", "timestamp", "likes"])
        writer.writerow(["u123", "alice", "Hello Custom World!", "2023-10-27 10:00:00", "5"])
        writer.writerow(["u456", "bob", "SMDT is flexible", "2023-10-27 11:30:00", "10"])

    print(f"Generated sample data: {csv_filename}")

    # Initialize the Database
    # initialize=True creates the necessary tables if they don't exist
    db = StandardDB("custom_smdt_db", initialize=True)

    # Initialize our Custom Standardizer
    standardizer = MyCustomCSVStandardizer()

    # Create an ingestion plan targeting our CSV file
    current_dir = os.path.dirname(os.path.abspath(__file__)) or "."
    plan = plan_directories(
        roots=[current_dir],
        include=[csv_filename]
    )

    print(f"Found {len(plan.files)} file(s) to process. Starting ingestion...")

    # Run the pipeline
    run_pipeline(
        plan=plan,
        db=db,
        standardizer=standardizer,
        config=PipelineConfig(
            batch_size=100,
            # We use DO NOTHING to prevent errors if we run the script multiple times
            on_conflict={
                Accounts: "DO NOTHING",
                Posts: "DO NOTHING"
            }
        )
    )

    print("Ingestion complete! Check your database.")

if __name__ == "__main__":
    main()
```

## How It Works

1. **`standardize()` Method**: The pipeline reads the file (e.g., via the built-in CSV reader) and passes each row to your standardizer as a dictionary. 
2. **Data Extraction**: You extract the fields, parse strings into proper `datetime` objects, and handle missing data.
3. **Model Instantiation**: You instantiate SMDT models (`Accounts`, `Posts`) and return them in a list.
4. **Pipeline Execution**: The `run_pipeline` function takes care of batching your returned models, deduplicating them, and safely inserting them into the PostgreSQL database using `StandardDB`.
