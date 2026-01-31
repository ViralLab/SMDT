### Sentiment Analyses using `gpt-4o-mini`:

```python 
import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher

db_name = "finland_sample"
db = StandardDB(db_name=db_name)

api_key = "API_KEY"
config = {
    # Required: Model identifiers
    "model_id_postfix": "v1_sentiment",  # Resulting ID: textgen_v1_sentiment
    "chat_model_id": "gpt-4o-mini",      # Provider's model name
    "base_url": "https://api.openai.com/v1",
    "api_key": api_key,

    # Provider Type (openai, anthropic, hf-text, ollama)
    "provider_kind": "openai",

    # The Prompt
    "system_prompt": "You are a helpful assistant.",
    "user_template": "Analyze the sentiment of this post: {body}",

    # Settings
    "temperature": 0.0,
    "only_missing": True,   # Skip already processed posts
    "batch_size": 10,
    "reset_cache": True,
}

# 3. Run
run_enricher("textgen", db=db, **config)
print("Done.")


```