# Building a Custom Standardizer

This recipe guides you through creating a custom standardizer to ingest data from a new source format (e.g., a custom CSV or JSON) into the SMDT database.

## Overview

All standardizers in SMDT inherit from the base `Standardizer` class. Your job is to implement the `standardize` method, which takes a raw record and converts it into a list of database models (like `Accounts`, `Posts`, `Actions`, etc.).

## 1. Create the Custom Standardizer Class

Let's imagine we have a CSV file with the following columns:
`user_id, username, tweet_text, timestamp, likes`

We want to map this to our `Accounts` and `Posts` tables.

Create a file named `my_custom_standardizer.py`:

```python
from datetime import datetime, timezone
from typing import Any, List, Tuple
from smdt.standardizers.base import Standardizer, SourceInfo
from smdt.store.models import Accounts, Posts

class MyCustomStandardizer(Standardizer):
    """
    A custom standardizer for my specific CSV format.
    """
    name = "my_custom_standardizer"

    def standardize(
        self, input_record: Tuple[dict, SourceInfo]
    ) -> List[Any]:
        """
        Transforms a raw dictionary (from CSV row) into SMDT models.
        """
        record, source_info = input_record
        
        output_models = []

        # 1. Extract and Clean Data
        user_id = record.get("user_id")
        username = record.get("username")
        body = record.get("tweet_text")
        # Generate a post_id if your data doesn't have one (e.g., using hash or if provided)
        # For this example, let's assume we don't have a post_id, so we skip creating the Post
        # or we could generate one. Let's assume we generate a dummy one for the example.
        post_id = f"{user_id}_{record.get('timestamp')}" 

        # 2. Parse Timestamps
        # Ensure dates are timezone-aware (UTC)
        try:
            # Assuming format "2023-10-27 10:00:00"
            dt_str = record.get("timestamp")
            created_at = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            created_at = created_at.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            # If date is invalid, we might skip this record or use a default
            return []

        # 3. Create Account Model
        # We assume the record contains up-to-date user info
        account = Accounts(
            account_id=user_id,
            username=username,
            created_at=created_at, # Using post time as proxy if user creation time isn't available
            profile_name=username, # Fallback
            retrieved_at=datetime.now(timezone.utc)
        )
        output_models.append(account)

        # 4. Create Post Model
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
```

## 2. Use the Standardizer in a Script

Now you can use this standardizer in a script to process your CSV file.

Create a file named `run_custom_import.py`:

```python
import csv
from smdt.store.standard_db import StandardDB
from smdt.standardizers.base import SourceInfo
from my_custom_standardizer import MyCustomStandardizer

# 1. Initialize Database
db = StandardDB("my_custom_db", initialize=True)

# 2. Initialize Standardizer
std = MyCustomStandardizer()

# 3. Process File
csv_file = "data.csv"

# Create a dummy CSV for this recipe
with open(csv_file, "w") as f:
    f.write("user_id,username,tweet_text,timestamp,likes\n")
    f.write("u123,alice,Hello World!,2023-10-27 10:00:00,5\n")
    f.write("u456,bob,SMDT is cool,2023-10-27 11:30:00,10\n")

print("Importing data...")

with open(csv_file, "r") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        source_info = SourceInfo(path=csv_file, line_number=i+1)
        
        # Standardize
        models = std.standardize((row, source_info))
        
        # Insert into Database
        # Note: In a real pipeline, you'd batch these insertions using db.copy_records()
        # for better performance. Here we just print them.
        for model in models:
            print(f"Generated: {type(model).__name__} -> {model}")

print("Done!")
```

## 3. Advanced: Using with the Pipeline

To use your custom standardizer with the robust `smdt.ingest.pipeline` system, you just need to pass an instance of it to `run_pipeline`.

```python
from smdt.ingest.plan import plan_directories
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.store.standard_db import StandardDB
from my_custom_standardizer import MyCustomStandardizer

# ... set up plan ...
# plan = plan_directories(...)

# ... set up db ...
# db = StandardDB(...)

# ... run pipeline ...
# run_pipeline(
#     plan,
#     db,
#     MyCustomStandardizer(), # <--- Your custom standardizer here
#     config=PipelineConfig(...)
# )
```
