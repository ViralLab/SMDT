"""
Starter script for running the Botometer bot-detection model on a database.

The enricher pulls account features from the `accounts`, `posts`, `actions`,
and `entities` tables, scores each account with a pre-trained sklearn model,
and writes results to `account_enrichments` (or a JSONL file if do_save_to_db=False).

JSONB payload stored per account:  {"bot_score": float}   (0 = human, 1 = bot)
model_id in account_enrichments:   "bot_detection"
"""

from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.bot_detection import BotometerEnricher, BotometerConfig

# ---------------------------------------------------------------------------
# 1. Connect to your database
# ---------------------------------------------------------------------------
DB_NAME = "twitter_v2_election_tr"   # <-- change this
db = StandardDB(db_name=DB_NAME)

# ---------------------------------------------------------------------------
# 2. Configure the enricher
#    Set do_save_to_db=False + output_dir if you want JSONL output instead.
# ---------------------------------------------------------------------------
config = BotometerConfig(
    # Skip accounts that already have a botometer_v1 enrichment row
    only_missing=True,

    # Write scores to account_enrichments table.
    # Set to False to write JSONL files to output_dir instead.
    do_save_to_db=False,
    output_dir="./bot_scores_output",          # e.g. "./bot_scores_output" when do_save_to_db=False

    # Optional: path to a custom model file (pkl.gz).
    # Defaults to the bundled model shipped with the package.
    model_path=None,

    # Cache keeps track of already-processed account IDs across runs.
    reset_cache=False,
    cache_dir=None,           # defaults to ~/.smdt_enricher_cache
)

# ---------------------------------------------------------------------------
# 3. Run
# ---------------------------------------------------------------------------
DB_BATCH_SIZE = 5  # accounts fetched from DB per iteration

print(f"Running bot detection on '{DB_NAME}'...")
run_enricher(BotometerEnricher, db=db, config=config, db_batch_size=DB_BATCH_SIZE)
print("Done.")
