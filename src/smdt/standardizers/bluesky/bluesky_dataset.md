This standardizer is for the data which was provided in this article:



[I’M IN THE BLUESKY TONIGHT”: INSIGHTS FROM A YEAR WORTH OF SOCIAL DATA](https://arxiv.org/abs/2404.18984)


- Data Strucutre: <br>
feed_bookmarks.csv  
feed_posts.tar.gz  
feed_posts_likes.tar.gz  
followers.csv.gz  
graphs.tar.gz 
interactions.csv.gz  
posts.tar.gz


```python
# main.py
from __future__ import annotations
import logging
from smdt.ingest.plan import plan_directories, print_plan
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.store.standard_db import StandardDB
from smdt.standardizers.bluesky import BlueSky  # or your TwitterV2Standardizer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("smdt.main")

def main():
    # 1) Build a plan with explicit order for files and members
    plan = plan_directories(
        roots=[
            "/path/to/bluesky/archives",              # folders or files
        ],
        include=["*.zip", "*.tar*", "*.jsonl*", "*.csv*", "*.tsv*"],
        exclude=["*.DS_Store", "*readme*"],
        # Process these archives first (left-most highest priority)
        order=[
            "*feed_posts.tar.gz",
            "*posts.tar.gz",
            "*interactions.csv.gz",
            "*feed_posts_likes.tar.gz",
        ],
        # Inside each archive, process members in this order
        member_order=[
            # Bluesky-like names (adjust to your dataset)
            "*feed_posts/*", "*posts/*",
            "*interactions*.csv*", "*likes*.csv*",
            "*.jsonl*", "*.csv*", "*.tsv*",
        ],
    )

    print_plan(plan)

    # 2) DB + standardizer
    db = StandardDB(db_name="smdt_bluesky")
    std = BlueSky()

    # 3) Reader kwargs (passed to the reader chosen for each file/member)
    # If your TSVs are handled by the pandas CSV reader, set sep here.
    cfg = PipelineConfig(
        batch_size=20_000,
        chunk_size=5_000,
        reader_kwargs={
            # applies to csv/tsv files handled by the pandas-backed reader
            "sep": "\t",
            "engine": "python",        # more forgiving for dirty CSV/TSV
            "on_bad_lines": "skip",    # skip malformed rows
            # other pandas options are fine here (quotechar, escapechar, etc.)
        },
        on_conflict={
            # keep only newest snapshot or do-nothing on dup keys (your schema/keys decide)
            # Example (adjust to your constraints):
            # Accounts: ("account_id","created_at","retrieved_at") unique
            # Posts:    ("post_id","created_at") unique
            # Entities: ("post_id","body","created_at","retrieved_at") unique
            # Actions:  composite unique you defined
            # Values below are the literal ON CONFLICT clauses:
            # Accounts: do nothing on exact duplicates
            # type: ignore[arg-type] – keys are classes in your code
        },
        progress=lambda ev, info: log.info("%s %s", ev, info),
        flush_per_file=True,
    )

    # 4) Run
    run_pipeline(plan, db, std, config=cfg, hints=None)

if __name__ == "__main__":
    main()
```