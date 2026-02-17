# Network Construction

SMDT provides powerful tools to construct various types of networks from your social media data. These functions generate networks over a specified period.

Supported network types include:
- **User Interaction**: Direct interactions between users (e.g., Retweets, Mentions).
- **Entity Co-occurrence**: Entities (e.g., Hashtags) that appear together in the same post.
- **Bipartite Networks**: Connections between two different node types (e.g., Accounts and Hashtags).

## Prerequisites

Ensure you have a database connection ready.

```python
from smdt.store.standard_db import StandardDB
from smdt import networks
from datetime import datetime
import os

# Connect to your database
# initialize=False is recommended when just reading data
db_name = os.getenv("DEFAULT_DB_NAME", "smdt_db")
db = StandardDB(db_name, initialize=False)
```

## Entity Co-occurrence Networks

This type of network connects entities that appear together in the same post. A common use case is a **Hashtag Co-occurrence Network**, where edges represent hashtags used in the same context.

```python
# Generate a Hashtag Co-occurrence Network
# for a specific 24-hour window
ht_net = networks.entity_cooccurrence(
    db,
    entity_type="HASHTAG",
    start_time=datetime(2023, 5, 14),
    end_time=datetime(2023, 5, 15),
    min_weight=5,  # Only include edges with at least 5 co-occurrences
)

print(ht_net.meta)
print(ht_net.edges)
```

### Parameters
- `entity_type`: The type of entity to analyze (e.g., `'HASHTAG'`, `'USER_TAG'`, `'LINK'`).
- `start_time` / `end_time`: Filter data to this time range.
- `min_weight`: Filter out edges with fewer than N co-occurrences to reduce noise.

## Bipartite Networks

Bipartite networks connect two different types of nodes. For example, an **Account-Hashtag Network** connects users to the hashtags they have used.

```python
# Build account–hashtag bipartite network
user_ht = networks.bipartite(
    db,
    left="account",
    right="hashtag",
    start_time=datetime(2023, 5, 14),
    end_time=datetime(2023, 5, 15),
    weighting="count",
)

print(user_ht.meta)
print(user_ht.edges)
```

### Parameters
- `left`: Must be `'account'` or `'post'`.
- `right`: Any valid entity type (e.g., `'HASHTAG'`, `'LINK'`).
- `weighting`: `'count'` (number of connections) or `'binary'` (1 if connected).

## User Interaction Networks

These networks represent direct social interactions. For example, a **Retweet Network** connects users who retweeted others.

```python
# Build a Retweet Network (Who retweeted whom?)
rt_net = networks.user_interaction(
    db,
    interaction="QUOTE", # or "SHARE", "COMMENT", "ReTweet" depends on platform mapping
    start_time=datetime(2023, 5, 14),
    end_time=datetime(2023, 5, 15),
    min_weight=2
)

print(f"Interaction edges: {len(rt_net.edges)}")
```

## Working with Results

All network functions return a `NetworkResult` object containing pandas DataFrames.

```python
# The edges are a standard DataFrame
df = ht_net.edges
# Columns: ['source', 'target', 'weight', ...]

# You can easily export to CSV
df.to_csv("hashtag_network.csv", index=False)

# Or convert to NetworkX for analysis
import networkx as nx
G = networks.to_networkx(ht_net)
print(nx.info(G))
```
