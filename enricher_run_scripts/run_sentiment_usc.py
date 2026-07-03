from smdt.store.standard_db import StandardDB
from smdt.enrichers.sentence_classifier import (
    SentenceClassifierConfig,
    SentenceClassifierEnricher,
)

DB_NAME = "bsky_EFE_2"
OUTPUT_DIR = "~/bsky_EFE_2_enrichments/sentiment/"
DB_BATCH_SIZE = 2**2
MODEL_BATCH_SIZE = 2**2

db = StandardDB(db_name=DB_NAME, initialize=False)

config = SentenceClassifierConfig(
    hf_model_id="finiteautomata/bertweet-base-sentiment-analysis",
    model_batch_size=MODEL_BATCH_SIZE,
    do_save_to_db=False,
    output_dir=OUTPUT_DIR,
    model_name="bertweet-base-sentiment",
    only_missing=False,
    reset_cache=False,
    max_seq_len=128,
)

# Initialize and Run
enricher = SentenceClassifierEnricher(db, config=config)
enricher.run(db_batch_size=DB_BATCH_SIZE)
