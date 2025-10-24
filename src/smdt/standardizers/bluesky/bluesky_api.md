# BlueSky API

This standardizer is developed for firehose API of Bluesky.


## Sample data

```json
{
  "seq": 6505277627,
  "collected_at": 1741996800.001658,
  "collected_at_str": "2025-03-15T00:00:00.001658Z",
  "commit_time": 1741985999.559,
  "commit_time_str": "2025-03-14T23:59:59.559Z",
  "action": "create",
  "type": "app.bsky.feed.like",
  "uri": "at://did:plc:cr52pmlz7st3srrc72t6y3gw/app.bsky.feed.like/3lkesyaqdof2y",
  "author": "did:plc:cr52pmlz7st3srrc72t6y3gw",
  "cid": "bafyreigz3lvwv2f3f2c36fuaawotw73k2gs7tlkqg2kl4zq5nm5qepfemi",
  "createdAt": "2025-03-14T23:59:59.023Z",
  "subject": {
    "cid": "bafyreieh3xuxaj3rwtm3tbjimff3cu7cgazie3x55snrqxxr7rrrma4hju",
    "uri": "at://did:plc:bfozp6ld257xnyyyuazjb5bu/app.bsky.feed.post/3lkcfevihvc2r",
    "$type": "com.atproto.repo.strongRef"
  },
  "$type": "app.bsky.feed.like"
}
```


<!-- "/scratch/anajafi/BlueSkyDATA/2025-03-14.json" -->



```python
import csv
from smdt.io.readers import discover

from smdt.ingest.plan import plan_directories, print_plan
from smdt.ingest.pipeline import run_pipeline, PipelineConfig
from smdt.store.standard_db import StandardDB
from smdt.store.models import (
    Accounts,
    Posts,
    Entities,
    AccountEnrichments,
    PostEnrichments,
)

from smdt.standardizers import TwitterV1Standardizer
from smdt.standardizers import TwitterV2Standardizer
from smdt.standardizers import TruthSocialStandardizer

from smdt.standardizers import BlueSkyAPIStandardizer


# 1) enable built-in readers (and plugin readers via entry points)
discover()

# 2) plan
plan = plan_directories(
    ["/cta/bscratch/anajafi/chistera/BlueSkyDATA/bsky_collected_firehose/"],
    include=(
        "*2025-03-14_users.jsonl",
        "*2025-03-14.jsonl",
    ),
    order=[
        "*2025-03-14_users.jsonl",
        "*2025-03-14.jsonl",
    ],
)

print_plan(plan)

# 3) db
db = StandardDB("bsky_api", initialize=True)
 
# 4) standardizer
standardizer = BlueSkyAPIStandardizer()


# 5) optional progress callback
def progress(event: str, info: dict):
    if event in ("file_start", "file_end", "flush", "done"):
        print(event, info)


pipeline_cfg = PipelineConfig(
    batch_size=20_000,
    chunk_size=20_000,
    reader_kwargs={
        "csv": {"sep": ","},
        "tsv": {
            "engine": "python",  # critical: tolerant parser
            "sep": "\t",
            "on_bad_lines": "skip",
            "quoting": csv.QUOTE_NONE,
        },  # auto-applied also for *.tsv.gz, *.tab.bz2, etc.
        "jsonl": {},
    },
    on_conflict={
        Accounts: "DO NOTHING",
        Posts: "DO NOTHING",
        Entities: "DO NOTHING",
        AccountEnrichments: "DO NOTHING",
        PostEnrichments: "DO NOTHING",
    },
    progress=progress,
)

# 6) run
run_pipeline(
    plan, db, standardizer, config=pipeline_cfg, hints={"dataset": "twitter_v2"}
)

```