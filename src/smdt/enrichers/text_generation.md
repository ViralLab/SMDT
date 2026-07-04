### Sentiment Analyses using `gpt-4o-mini`:

```python 
import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationEnricher, TextGenerationConfig

db_name = "finland_sample"
db = StandardDB(db_name=db_name)

config = TextGenerationConfig.for_openai(
    model="gpt-4o-mini",
    api_key=os.environ["OPENAI_API_KEY"],

    # Required: Model identifiers
    model_id_postfix="v1_sentiment",  # Resulting ID: text_generation_v1_sentiment

    # The Prompt
    system_prompt="You are a helpful assistant.",
    user_template="Analyze the sentiment of this post: {body}",

    # Settings
    temperature=0.0,
    only_missing=True,   # Skip already processed posts
    batch_size=10,
    reset_cache=True,
)

# Run
run_enricher(TextGenerationEnricher, db=db, config=config)
print("Done.")
```

`for_openai` pre-fills `base_url`/`provider_kind` for OpenAI; use `for_anthropic`, `for_gemini`, or `for_ollama` for the other built-in providers (see the [NLP Enrichment recipe](/recipes/enrichment/nlp) for one example per provider), or construct `TextGenerationConfig(...)` directly for `hf-text` or any other OpenAI-compatible endpoint.