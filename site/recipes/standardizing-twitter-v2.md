# Standardizing Twitter API v2 Data

This recipe demonstrates how to generate a sample Twitter API v2 dataset and standardize it using SMDT.

## 1. Generate Sample Data

First, let's create a Python script to generate a sample dataset in JSONL format. This simulates the output you might get from the Twitter API.

Create a file named `generate_twitter_data.py`:

```python
import json
import random
from datetime import datetime, timezone

def generate_sample_tweet(tweet_id, author_id, conversation_id=None):
    now = datetime.now(timezone.utc).isoformat()
    
    return {
        "data": {
            "id": str(tweet_id),
            "text": f"This is sample tweet #{tweet_id} about #SMDT",
            "author_id": str(author_id),
            "created_at": now,
            "conversation_id": str(conversation_id or tweet_id),
            "public_metrics": {
                "retweet_count": random.randint(0, 100),
                "reply_count": random.randint(0, 50),
                "like_count": random.randint(0, 500),
                "quote_count": random.randint(0, 20),
                "impression_count": random.randint(100, 10000)
            },
            "lang": "en"
        },
        "includes": {
            "users": [
                {
                    "id": str(author_id),
                    "name": f"User {author_id}",
                    "username": f"user_{author_id}",
                    "created_at": "2020-01-01T00:00:00Z",
                    "public_metrics": {
                        "followers_count": random.randint(100, 1000),
                        "following_count": random.randint(100, 500),
                        "tweet_count": random.randint(50, 2000),
                        "listed_count": random.randint(0, 10)
                    }
                }
            ]
        }
    }

def main():
    with open("sample_twitter_v2.jsonl", "w") as f:
        # Generate 5 sample tweets
        for i in range(1, 6):
            tweet = generate_sample_tweet(
                tweet_id=1000 + i, 
                author_id=500 + (i % 2) # Toggle between two authors
            )
            f.write(json.dumps(tweet) + "\n")
            
    print("Generated sample_twitter_v2.jsonl with 5 records.")

if __name__ == "__main__":
    main()
```

Run the script to generate the data:

```bash
python generate_twitter_data.py
```

## 2. Standardize the Data

SMDT uses a pipeline architecture to handle large-scale data ingestion efficiently. Instead of manually reading files, we define a specialized ingestion plan and run it through a pipeline.

Create a file named `run_standardization.py`:

```python
from smdt.io.readers import discover
from smdt.ingest.plan import plan_directories, print_plan
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.store.standard_db import StandardDB
from smdt.standardizers import TwitterV2Standardizer
from smdt.store.models import (
    Accounts,
    Posts,
    Entities,
    AccountEnrichments,
    PostEnrichments,
)
import os

# 1. Register readers (JSONL, CSV, etc.)
discover()

def main():
    # Define the directory where our sample data lives
    # In this example, it's the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # 2. Create an ingestion plan
    # This scans the directory for files matching the pattern
    plan = plan_directories(
        [current_dir],
        include=("sample_twitter_v2.jsonl",),
    )

    print_plan(plan)

    # 3. Initialize Database Connection
    # The 'initialize=True' flag ensures schemas are created if they don't exist
    db = StandardDB("twitter_v2_sample", initialize=True)

    # 4. Initialize Standardizer
    standardizer = TwitterV2Standardizer()

    # 5. Configure the Pipeline
    # Defines how to handle conflicts (e.g., if a post already exists)
    # and batch processing sizes.
    pipeline_cfg = PipelineConfig(
        batch_size=1000,
        chunk_size=1000,
        on_conflict={
            Accounts: "DO NOTHING",
            Posts: "DO NOTHING",
            Entities: "DO NOTHING",
            AccountEnrichments: "DO NOTHING",
            PostEnrichments: "DO NOTHING",
        },
        progress=lambda event, info: print(f"[Progress] {event}: {info}")
    )

    # 6. Run the Pipeline
    print("\nStarting Pipeline...")
    run_pipeline(
        plan,
        db,
        standardizer,
        config=pipeline_cfg,
        hints={"dataset": "sample_dataset"},
    )
    print("\nPipeline Finished!")

if __name__ == "__main__":
    main()
```

## 3. Run the Standardization

Execute the standardization script:

```bash
python run_standardization.py
```

You should see output indicating the plan is created and the pipeline is processing the file:

```text
Ingestion plan:
  FILE  [✓] /cta/users/anajafi/SMDT/sample_twitter_v2.jsonl  [2,690B  2026-02-17T20:03:59]  → jsonl

By reader:
  jsonl        : 1
Should I start ingestion? (y/n): y


Starting Pipeline...
Pipeline files:   0%|                                                                                                                                                           | 0/1 [00:00<?, ?it/s][Progress] 
file_start: {'path': 'current_dir/sample_twitter_v2.jsonl'}
                                   [Progress] file_end: {'path': '/cta/users/anajafi/SMDT/sample_twitter_v2.jsonl', 'records': 5, 'models': 20, 'record_errors': 0, 'row_failures': 0, 'elapsed': 0.4030951801687479}                    
Pipeline files: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00,  2.48it/s]
[Progress] flush: {'model': 'Accounts', 'count': 2, 'elapsed': 0.12162525579333305}
[Progress] flush: {'model': 'Posts', 'count': 5, 'elapsed': 0.16321173310279846}
[Progress] flush: {'model': 'Entities', 'count': 10, 'elapsed': 0.1657374557107687}
[Progress] done: {'files': 1, 'records': 5, 'models': 20, 'record_errors': 0, 'row_failures': 0, 'failed_models_total': 0, 'failed_models_by_class': {}, 'elapsed': 0.8682915344834328}

Pipeline Finished!
```

This approach is scalable and can handle thousands of files by simply adjusting the `plan_directories` path and patterns.
