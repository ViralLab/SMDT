---
description: Generate networks over sliding time windows to analyze how community structures evolve. Build weekly or daily interaction graphs with configurable weights and thresholds.
---

# Temporal Networks

This recipe shows how to generate networks over sliding time windows, letting you analyze how community structures evolve day by day or week by week.

## Quickstart

```python
from smdt.store.standard_db import StandardDB
from smdt import networks
from datetime import datetime, timedelta
import pickle, os

db = StandardDB("my_dataset", initialize=False)

windows = networks.user_interaction_over_time(
    db,
    interaction="SHARE",
    start_time=datetime(2024, 1, 1),
    end_time=datetime(2024, 1, 14),
    step=timedelta(days=7),
    weighting="count",
    min_weight=3,
)

os.makedirs("output", exist_ok=True)
for result in windows:
    ws = result["window_start"].strftime("%Y%m%d")
    we = result["window_end"].strftime("%Y%m%d")
    with open(f"output/network_{ws}_{we}.pkl", "wb") as f:
        pickle.dump(result, f)
    print(f"{ws}-{we}: {result['network'].meta.get('edge_count', 0)} edges")
```

## How It Works

`user_interaction_over_time` queries the `actions` table and slices the results into time windows. For each window it builds a graph where nodes are accounts and weighted edges represent the number of interactions between them. The `min_weight` parameter filters out weak connections, reducing noise in the output.

The function returns a generator so you can process one window at a time without holding all graphs in memory.

## Parameters

| Parameter | Description |
|---|---|
| `db` | A `StandardDB` instance connected to your database |
| `interaction` | Action type to build edges from: `"SHARE"`, `"COMMENT"`, `"QUOTE"`, `"MENTION"` |
| `start_time` / `end_time` | Overall time range to cover |
| `step` | Width of each sliding window as a `timedelta` |
| `weighting` | `"count"` (number of interactions) or `"binary"` (present/absent) |
| `min_weight` | Drop edges with fewer than N interactions in a window |

## Output Format

Each yielded result is a dictionary:

```python
{
    "window_start": datetime(...),
    "window_end": datetime(...),
    "network": <igraph.Graph>,  # the graph for this window
}
```

The `igraph` graph object can be analyzed directly or converted to NetworkX:

```python
from smdt.networks.converters import to_networkx
G_nx = to_networkx(result["network"])
```

## Prerequisites

```bash
uv pip install igraph tqdm
```

## Next Steps

For single-snapshot network construction, see [Network Construction](./construction.md).
