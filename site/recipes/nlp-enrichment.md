# NLP Enrichment with LLMs

This recipe demonstrates how to enrich posts in your database using Large Language Models (LLMs). SMDT supports various providers including **OpenAI**, **Anthropic**, **HuggingFace**, and **Ollama** via the `textgen` enricher.

## Overview

The `TextGenEnricher` allows you to run any LLM prompt against the `body` of posts in your database and save the structured response or text output into the `post_enrichments` table.

Common use cases include:
- Sentiment Analysis
- Topic Classification
- Toxicity Detection
- Summarization
- Entity Extraction

## Prerequisites

Ensure you have the necessary API keys or a local LLM server running (e.g., Ollama).

## Configuration Reference

The `TextGenEnricher` is highly configurable. You can pass these parameters in a dictionary to `run_enricher`.

### Required Parameters

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`model_id_postfix`** | `str` | A unique suffix for this enrichment run. The final `model_id` in the database will be `textgen_{model_id_postfix}` (e.g., `textgen_v1_sentiment`). |
| **`chat_model_id`** | `str` | The specific model name to request from the provider (e.g., `gpt-4o`, `llama3`, `claude-3-opus`). |
| **`base_url`** | `str` | The API endpoint URL. For OpenAI, use `https://api.openai.com/v1`. For local Ollama, typically `http://localhost:11434/v1`. |

### Provider Settings

| Parameter | Default | Description |
| :--- | :--- | :--- |
| **`provider_kind`** | `"openai"` | The type of provider interface. Options: `"openai"`, `"anthropic"`, `"hf-text"`, `"ollama"`. |
| **`api_key`** | `""` | The API key for authentication. Not needed for local Ollama. |

### Prompting

| Parameter | Default | Description |
| :--- | :--- | :--- |
| **`user_template`** | *Summary prompt* | The prompt sent to the model. Use `{body}` as a placeholder for the post content. |
| **`system_prompt`** | *"You are a helpful assistant."* | The system instruction that sets the behavior context for the model. |
| **`temperature`** | `0.2` | Controls randomness (0.0=deterministic, 1.0=creative). Lower values are better for classification. |
| **`max_tokens`** | `None` | Max number of tokens to generate. |
| **`prompt_path`** | `None` | Path to a YAML/JSON file containing prompts (advanced usage). |

### Execution & Performance

| Parameter | Default | Description |
| :--- | :--- | :--- |
| **`batch_size`** | `32` | Number of posts to fetch from the database at once. Lower this for large local models. |
| **`requests_per_minute`** | `120` | Client-side rate limiting to avoid hitting API caps. |
| **`only_missing`** | `True` | If `True`, skips posts that already have an enrichment entry for this `model_id`. Set to `False` to re-process everything. |
| **`reset_cache`** | `False` | If `True`, clears internal caches before starting. |

### Output

| Parameter | Default | Description |
| :--- | :--- | :--- |
| **`do_save_to_db`** | `True` | Whether to save results to the `post_enrichments` table. |
| **`output_dir`** | `None` | If `do_save_to_db=False`, results are written to JSONL files in this directory. |

## Examples

### 1. OpenAI Sentiment Analysis

This example uses OpenAI's GPT-4o-mini to analyze the sentiment of posts.

```python
import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
# Explicit import to ensure registration
from smdt.enrichers.nlp.server.textgen.textgen import TextGenEnricher

# 1. Initialize Database
db_name = "my_local_db"
db = StandardDB(db_name=db_name)

# 2. Configuration
api_key = os.getenv("OPENAI_API_KEY") 
if not api_key:
    # Set this in your environment or replace with your key
    api_key = "your_openai_api_key"

config = {
    # --- Identifiers ---
    "model_id_postfix": "v1_sentiment",  # Saved as: textgen_v1_sentiment
    
    # --- Provider Settings ---
    "provider_kind": "openai",
    "base_url": "https://api.openai.com/v1",
    "chat_model_id": "gpt-4o-mini",
    "api_key": api_key,
    
    # --- Prompting ---
    "system_prompt": "You are an expert sentiment analyst.",
    "user_template": (
        "Classify the sentiment of the following social media post as "
        "POSITIVE, NEGATIVE, or NEUTRAL. Return only the class label.\n\n"
        "Post: {body}"
    ),
    "temperature": 0.0,
    
    # --- Execution Settings ---
    "batch_size": 20,          # Number of posts to fetch/process at a time
    "requests_per_minute": 60, # Rate limiting
    "only_missing": True,      # Skip posts that already have this enrichment
}

# 3. Run the Enricher
print(f"Starting TextGen Enricher on {db_name}...")

# run_enricher looks up "textgen" in the registry
run_enricher("textgen", db=db, **config)

print("Enrichment complete.")
```

### 2. Local LLM with Ollama

You can use a locally running Ollama instance to save costs and keep data private.

```python
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.nlp.server.textgen.textgen import TextGenEnricher

db = StandardDB(db_name="my_local_db")

config = {
    "model_id_postfix": "llama3_topic",
    
    # --- Ollama Settings ---
    "provider_kind": "ollama",
    "base_url": "http://localhost:11434/v1", # Default Ollama API port
    "chat_model_id": "llama3",
    "api_key": "ollama", # Placeholder key
    
    # --- Prompting ---
    "system_prompt": "You are a helpful classifier.",
    "user_template": "Identify the main topic of this text: {body}",
    
    "batch_size": 5, # Lower batch size for local inference
    "only_missing": True,
}

print("Starting Local Enrichment...")
run_enricher("textgen", db=db, **config)
print("Done.")
```

### 3. Using HuggingFace Inference Endpoints

Connect to a hosted HuggingFace model.

```python
config = {
    "model_id_postfix": "hf_classification",
    "provider_kind": "hf-text",
    "base_url": "https://api-inference.huggingface.co/models/meta-llama/Llama-2-7b-chat-hf",
    "chat_model_id": "meta-llama/Llama-2-7b-chat-hf",
    "api_key": "hf_...", 
    # ...
}
```

## Viewing Results

The method for retrieving results depends on your `do_save_to_db` setting.

### Option A: Database Storage (`do_save_to_db=True`)

This is the default. Results are stored in the `post_enrichments` table. You can query them using SQL:

```sql
SELECT 
    p.body AS original_text, 
    pe.body ->> 'text' AS llm_response 
FROM posts p
JOIN post_enrichments pe ON p.post_id = pe.post_id
WHERE pe.model_id = 'textgen_v1_sentiment'
LIMIT 10;
```

Or using Python:

```python
from smdt.store.models import PostEnrichments

db = StandardDB("my_local_db", initialize=False)
conn = db.connect()
try:
    with conn.cursor() as cur:
        # Example using raw SQL
        cur.execute(
            """
            SELECT body ->> 'text' 
            FROM post_enrichments 
            WHERE model_id = %s 
            LIMIT 5
            """,
            ("textgen_v1_sentiment",)
        )
        for row in cur.fetchall():
            print(f"Result: {row[0]}")
finally:
    conn.close()
```

### Option B: Local File Storage (`do_save_to_db=False`)

If you set `do_save_to_db=False`, the enricher writes results to JSONL files in the directory specified by `output_dir`.

The files are named following the pattern `{model_id}_{date}.jsonl`.

```python
import json
import glob
import os

# The directory you specified in your config
output_dir_path = "./my_llm_outputs" 

# Find all JSONL files
files = glob.glob(os.path.join(output_dir_path, "*.jsonl"))

for filepath in files:
    print(f"Reading {filepath}...")
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            # The structure matches the database model
            post_id = record["post_id"]
            
            # The actual LLM output is inside the 'body' dictionary
            llm_text = record["body"]["text"] 
            
            print(f"[{post_id}] {llm_text}")
```
