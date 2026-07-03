import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.llm_textgen import TextGenEnricher, TextGenConfig

db_name = "finland_sample"
db = StandardDB(db_name=db_name)

config = TextGenConfig(
    # Required: Model identifiers
    model_id_postfix="v1_sentiment",  # Resulting ID: textgen_v1_sentiment
    chat_model_id="gpt-4o-mini",  # Provider's model name
    base_url="https://api.openai.com/v1",
    api_key=os.environ["OPENAI_API_KEY"],
    # Provider Type (openai, anthropic, hf-text, ollama, gemini)
    provider_kind="openai",
    # The Prompt
    system_prompt="You are a helpful assistant.",
    user_template="Analyze the sentiment of this post: {body}",
    # Settings
    temperature=0.0,
    only_missing=True,  # Skip already processed posts
    batch_size=10,
    reset_cache=True,
)

# Run
print("Starting TextGen Enricher...")
run_enricher(TextGenEnricher, db=db, config=config)
print("Done.")
