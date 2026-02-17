# Temporal Networks

This recipe demonstrates how to generate **temporal networks** of user interactions (e.g., RETWEET/SHARE) over a specific time range. This is useful for analyzing how community structures evolve over time.

## Overview

Unlike standard network construction which creates a single aggregate graph, this approach slices the data into time windows (e.g., weekly or daily).

The script connects to a database, specifies a time window and step size, and iteratively builds networks for each window. The resulting networks are saved as pickle files.


Create a file named `generate_temporal_networks.py`:

```python
from smdt.store.standard_db import StandardDB
from smdt import networks

from datetime import datetime, timedelta
import os
import pickle
from tqdm import tqdm

# 1. Configuration
# Choose your database name
DB_NAME = "truthsocial_usc"  
INTERACTION_TYPE = "SHARE"   # e.g., "SHARE", "REPLY", "QUOTE", "MENTION"
OUTPUT_BASE_DIR = "./output_networks"

# Define the analysis period
# Adjust dates according to your dataset

START_TIME = datetime(2024, 1, 1)
END_TIME = datetime(2025, 1, 1)

TIME_STEP = timedelta(days=7) # Window size / sliding step

# 2. Initialize Database Connection
# initialize=False because we are reading, not creating tables
db = StandardDB(db_name=DB_NAME, initialize=False)

print(f"Connected to database: {DB_NAME}")
print(f"Generating {INTERACTION_TYPE} networks from {START_TIME} to {END_TIME} with step {TIME_STEP}")

# 3. Generate Networks Generator
# user_interaction_over_time returns a generator that yields networks lazily
windows = networks.user_interaction_over_time(
    db,
    interaction=INTERACTION_TYPE,
    start_time=START_TIME,
    end_time=END_TIME,
    step=TIME_STEP,
    weighting="count", # Edge weight based on number of interactions
    min_weight=3,      # Filter out weak ties (less than 3 interactions)
)

# 4. Prepare Output Directory
# Normalize output directory name based on DB

output_dir = os.path.join(
    OUTPUT_BASE_DIR, 
    f"{DB_NAME}_enrichments", 
    "temporal_networks", 
    f"{INTERACTION_TYPE}_interaction"
)
os.makedirs(output_dir, exist_ok=True)
print(f"Saving networks to: {output_dir}")

# 5. Iterate and Save
for network_result in tqdm(windows, desc="Processing Windows"):
    ws = network_result["window_start"]
    we = network_result["window_end"]
    net = network_result["network"] # This is an igraph object

    # Print stats for the current window
    edge_count = net.meta.get("edge_count", "N/A")
    # print(f"Window: {ws} - {we} | Edges: {edge_count}")

    # Construct filename
    filename = f"{INTERACTION_TYPE}_interaction_{ws.strftime('%Y%m%d')}_{we.strftime('%Y%m%d')}.pkl"
    filepath = os.path.join(output_dir, filename)
    
    # Save the result dictionary (containing window info and the network)
    with open(filepath, "wb") as f:
        pickle.dump(network_result, f)

print("Done!")
```

## How it Works

1.  **`StandardDB`**: Connects to your configured PostgreSQL database.
2.  **`networks.user_interaction_over_time`**: This is the core function. It queries the `actions` table in the database.
    *   It filters for actions where `action_type` matches your request (e.g., 'SHARE').
    *   It groups interactions by the specified time window (e.g., weekly).
    *   It constructs a graph where nodes are users (`account_id`) and edges represent the interaction.
    *   `weighting="count"` sets the edge weight to the number of times User A interacted with User B in that window.
    *   `min_weight=3` is a threshold to reduce noise; edges with fewer than 3 interactions are discarded.
3.  **Output**: The script saves a pickle file for each time window. Each file contains a dictionary with:
    *   `window_start`: Datetime of window start.
    *   `window_end`: Datetime of window end.
    *   `network`: An `igraph` Graph object representing the interaction network.

## Prerequisites

Ensure you have the `igraph` library installed, as SMDT uses it for network representation.

```bash
uv pip install igraph tqdm
```
