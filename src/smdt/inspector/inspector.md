```python

### INSPECTOR 
from smdt.store.standard_db import StandardDB
from smdt.inspector import Inspector, report_schemas

db1 = StandardDB("truth_social")
ins1 = Inspector(db1, schema="anajafi")

db2 = StandardDB("bsky_api")
ins2 = Inspector(db2, schema="anajafi")


db3 = StandardDB("smdt_twitter_v1_election")
ins3 = Inspector(db3, schema="anajafi")

db4 = StandardDB("smdt_twitter_v2_election")
ins4 = Inspector(db4, schema="anajafi")


insp1 = Inspector(db1, schema="public")
insp2 = Inspector(db2, schema="public")


report_schemas(
    [ins1, ins2, ins3, ins4], only_tables=["accounts", "posts", "actions", "entities"]
)
```