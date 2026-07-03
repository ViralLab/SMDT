import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.llm_textgen import TextGenEnricher, TextGenConfig

db_name = "finland_sample"
db = StandardDB(db_name=db_name)

config = TextGenConfig(
    # Required: Model identifiers
    model_id_postfix="v1_sentiment_claude",  # distinct suffix for claude results
    chat_model_id="claude-sonnet-4-20250514",
    # Claude provides an OpenAI-compatible endpoint
    base_url="https://api.anthropic.com/v1/messages",
    api_key=os.environ["ANTHROPIC_API_KEY"],
    provider_kind="anthropic",
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
print("Starting TextGen Enricher (Claude)...")
run_enricher(TextGenEnricher, db=db, config=config)
print("Done.")
