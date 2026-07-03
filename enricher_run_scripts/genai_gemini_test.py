import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.llm_textgen import TextGenEnricher, TextGenConfig

db_name = "finland_sample"
db = StandardDB(db_name=db_name)

config = TextGenConfig(
    # Required: Model identifiers
    model_id_postfix="v1_sentiment_gemini",  # distinct suffix for gemini results
    chat_model_id="gemini-3-pro-preview",
    # Gemini provides an OpenAI-compatible endpoint
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    api_key=os.environ["GEMINI_API_KEY"],
    provider_kind="gemini",
    # The Prompt
    system_prompt="You are a helpful assistant.",
    user_template="Analyze the sentiment of this post: {body}",
    # Settings
    temperature=0.0,
    only_missing=True,  # Skip already processed posts
    batch_size=10,
    reset_cache=True,
    max_tokens=None,
)

# Run
print("Starting TextGen Enricher (Gemini)...")
run_enricher(TextGenEnricher, db=db, config=config)
print("Done.")
