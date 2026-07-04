import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationEnricher, TextGenerationConfig

db_name = "finland_sample"
db = StandardDB(db_name=db_name)

config = TextGenerationConfig.for_gemini(
    model="gemini-3-pro-preview",
    api_key=os.environ["GEMINI_API_KEY"],
    # Required: Model identifiers
    model_id_postfix="v1_sentiment_gemini",  # distinct suffix for gemini results
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
print("Starting Text Generation Enricher (Gemini)...")
run_enricher(TextGenerationEnricher, db=db, config=config)
print("Done.")
