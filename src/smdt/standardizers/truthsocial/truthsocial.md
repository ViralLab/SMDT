# TruthSocial Data Fromat Sample

The Zip file contained:

and order matters:

```python
"*users.tsv",   
"*hashtags.tsv",  
"*media.tsv",   
"*external_urls.tsv",  
"*replies.tsv",  
"*truths.tsv",  
"*quotes.tsv",   
"*truth_hashtag_edges.tsv",  
"*truth_media_edges.tsv",  
"*truth_external_url_edges.tsv",  
"*follows.tsv", 
```
<br>


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
 
from smdt.standardizers import TruthSocialStandardizer


# 1) enable built-in readers (and plugin readers via entry points)
discover()

# 2) plan
plan = plan_directories(
    ["/cta/users/anajafi/SocialMediaDataToolkit/data/zipfiles/"],
    include=("*truth_social.zip",),
    member_order=[
        "*users.tsv",    # emit accounts
        "*replies.tsv",  # build reply-user map
        "*truths.tsv",   # emit posts, COMMENTs, build external_id→id
        "*quotes.tsv",   # use external_id map
        "*follows.tsv",  # emit FOLLOWs
    ],
    member_exclude=[
        "*/*.DS_Store",
        "*/*readme.txt",
        "*truth_hashtag_edges.tsv",        
        "*media.tsv",                  
        "*truth_media_edges.tsv",  
        "*truth_external_url_edges.tsv",  
        "*truth_user_tag_edges.tsv",
        "*external_urls.tsv",   
        "*hashtags.tsv",   
    ],  
)
 
print_plan(plan)

# 3) db
db = StandardDB("truth_social", initialize=True)

# 4) standardizer
standardizer = TruthSocialStandardizer()


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
            "engine": "python", 
            "sep": "\t",
            "on_bad_lines": "skip",
            "quoting": csv.QUOTE_NONE,
        }, 
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