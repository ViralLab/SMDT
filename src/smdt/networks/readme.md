# 🕸️ Networks Layer

The **`networks`** module provides a unified interface for constructing and streaming large-scale social network graphs from standardized social media datasets stored in PostgreSQL.  

It supports multiple network types — including **user–user interaction**, **entity co-occurrence**, and **bipartite** graphs — with both in-memory and streaming modes.

---

## 📦 Overview

### Supported network families

| Network Type | Example | Directed | Description |
|---------------|----------|-----------|--------------|
| **User–User Interaction** | `QUOTE`, `REPLY`, `FOLLOW` | ✅ | Connects users based on actions performed on each other. |
| **Entity Co-occurrence** | `HASHTAG`, `MENTION`, `LINK` | ❌ | Connects entities that appear together within the same post. |
| **Bipartite** | `account–hashtag`, `account–link` | ✅ | Connects heterogeneous node types (e.g., users ↔ hashtags). |

Each network builder automatically generates:
- an **edge list** (`src`, `dst`, `weight`, `edge_type`)
- a **node table**
- and **metadata** (counts, filters, weighting, etc.)

---

## 🚀 Usage Examples

### 1. User–User Interaction Network

```python
from datetime import datetime
from smdt.store.standard_db import StandardDB
from smdt import networks

db = StandardDB("smdt_twitter_v2_election")

# Build a directed QUOTE network between users
quote_net = networks.user_interaction(
    db,
    interaction="QUOTE",
    start_time=datetime(2023, 5, 14),
    end_time=datetime(2023, 5, 15),
    weighting="count",
    min_weight=3,
)

print(quote_net.meta)
print(quote_net.edges.head())
```

---

### 2. Entity Co-occurrence Network

```python
# Build an undirected hashtag co-occurrence network
ht_net = networks.entity_cooccurrence(
    db,
    entity_type="HASHTAG",
    start_time=datetime(2023, 5, 14),
    end_time=datetime(2023, 5, 15),
    min_weight=5,
)

print(ht_net.meta)
print(ht_net.edges.sample(5))
```

---

### 3. Bipartite Network

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

print(user_ht.edges.head())
```

---

## ⚙️ Streaming Mode

For large datasets, you can stream edges directly from PostgreSQL in fixed-size chunks, keeping memory usage constant.

### Stream user–user QUOTE edges

```python
chunks = networks.iter_user_interaction_edges(
    db,
    interaction="QUOTE",
    start_time=datetime(2023, 5, 14),
    end_time=datetime(2023, 5, 15),
    chunksize=200_000,
)

for i, chunk in enumerate(chunks, start=1):
    print(f"Chunk {i}: {len(chunk):,} edges")
    process_edges(chunk)  # user-defined function
    del chunk
```

---

### Stream bipartite edges and save to Parquet

```python
from smdt.networks.io_utils import write_edges_to_parquet_from_chunks

chunks = networks.iter_bipartite_edges(
    db,
    left="account",
    right="hashtag",
    start_time=datetime(2023, 5, 14),
    end_time=datetime(2023, 5, 15),
    chunksize=250_000,
)

write_edges_to_parquet_from_chunks(chunks, "user_hashtag_edges.parquet")
```

---

## 🔄 Convert to NetworkX Graphs

### a) From a full network result

```python
from smdt.networks.converters import to_networkx

G = to_networkx(quote_net)
print(G.number_of_nodes(), G.number_of_edges())
```

### b) From sampled edges (for visualization)

```python
from smdt.networks.converters import to_networkx_sample

G_small = to_networkx_sample(ht_net.edges, directed=False, n=10_000)
```

---

## 🧠 End-to-End Example

```python
from smdt.store.standard_db import StandardDB
from smdt import networks
from smdt.networks.io_utils import write_edges_to_parquet_from_chunks
from smdt.networks.converters import to_networkx
import pandas as pd
import networkx.algorithms.community as nx_comm

db = StandardDB("smdt_twitter_v2_election")

# Step 1: Stream account–hashtag bipartite edges and export
chunks = networks.iter_bipartite_edges(
    db, left="account", right="hashtag", chunksize=250_000
)
write_edges_to_parquet_from_chunks(chunks, "user_hashtag_edges.parquet")

# Step 2: Load back for analysis
edges = pd.read_parquet("user_hashtag_edges.parquet")

# Step 3: Build a sample graph
G = networks.to_networkx_sample(edges, directed=False, n=50_000)

# Step 4: Community detection
communities = nx_comm.louvain_communities(G)
print(f"Found {len(communities)} communities")
```

---

## 🧩 Design Notes

- All builders are **read-only** — no writes to the database.
- Compatible with PostgreSQL through `psycopg` connections.
- Streaming mode (`iter_*_edges`) ensures memory-stable operation even for billions of edges.
- Nodes and edges follow consistent schema conventions across all network types.

---

## 📚 Available Builders

| Builder Class | Function | Description |
|----------------|-----------|--------------|
| `UserInteractionNetworkBuilder` | `networks.user_interaction()` | Builds directed user–user interaction graphs. |
| `EntityCooccurrenceNetworkBuilder` | `networks.entity_cooccurrence()` | Builds undirected entity–entity co-occurrence graphs. |
| `BipartiteNetworkBuilder` | `networks.bipartite()` | Builds heterogeneous bipartite graphs. |

---

## 🧮 Output Schema

### Edges
| Column | Description |
|---------|-------------|
| `src` | Source node ID |
| `dst` | Target node ID |
| `weight` | Edge weight (count or binary) |
| `edge_type` | Action or relationship type |

### Nodes
| Column | Description |
|---------|-------------|
| `node_id` | Node identifier |
| `label` / `type` | Optional metadata for labeling or node type |

---

## ✅ Summary

- Read-only, scalable, and memory-safe graph extraction layer.  
- Unified API for **interaction**, **co-occurrence**, and **bipartite** networks.  
- Integrates cleanly with pandas, Parquet, and NetworkX.  
- Designed for large-scale computational social science pipelines.
