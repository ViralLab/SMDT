from smdt.store.standard_db import StandardDB
from smdt import networks

from datetime import datetime, timedelta
import os

import pickle
from tqdm import tqdm

# Initialize DB connection
# db_name = "turkish_election2023"
db_name = "twitter_usc2"
# db_name = "truthsocial_usc"
db = StandardDB(db_name=db_name, initialize=False)
entity_type = "HASHTAG"


# Build hourly QUOTE networks over a 1-day period
windows = networks.entity_cooccurrence_over_time(
    db,
    entity_type=entity_type,
    start_time=datetime(2023, 1, 1) if "turkish" in db_name else datetime(2024, 1, 1),
    end_time=datetime(2024, 1, 1) if "turkish" in db_name else datetime(2025, 1, 1),
    step=timedelta(days=7),
    weighting="count",
    min_weight=3,
)

if db_name == "twitter_usc2":
    db_name = "usc_twitter"
elif db_name == "truthsocial_usc":
    db_name = "usc_truthsocial"
output_dir = f"/chistera/CaseStudyOutputs/{db_name}_enrichments/temporal_networks/{entity_type}_cooccurrence/"

os.makedirs(output_dir, exist_ok=True)
for network_result in tqdm(windows):
    ws = network_result["window_start"]
    we = network_result["window_end"]
    net = network_result["network"]

    print(ws, we, net.meta["edge_count"])

    filename = f"{entity_type}_cooccurrence_{ws.strftime('%Y%m%d')}_{we.strftime('%Y%m%d')}.pkl"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "wb") as f:
        pickle.dump(network_result, f)
    print(f"Saved to {filepath}")
