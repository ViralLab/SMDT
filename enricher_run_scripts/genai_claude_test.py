import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationEnricher, TextGenerationConfig

db_name = "finland_sample"
db = StandardDB(db_name=db_name)

config = TextGenerationConfig.for_anthropic(
    model="claude-sonnet-4-20250514",
    api_key=os.environ["ANTHROPIC_API_KEY"],
    # Required: Model identifiers
    model_id_postfix="v1_sentiment_claude",  # distinct suffix for claude results
    # The Prompt
    system_prompt="You are a helpful assistant.",
    user_template="Analyze the sentiment of this post: {body}",
    # Settings
    # temperature=0.0,
    only_missing=True,  # Skip already processed posts
    batch_size=10,
    reset_cache=True,
    max_tokens=1000,
)

# Run
print("Starting Text Generation Enricher (Claude)...")
run_enricher(TextGenerationEnricher, db=db, config=config)
print("Done.")
