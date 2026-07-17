---
description: Run NLP enrichment on your data with local (Ollama, Hugging Face) or hosted (OpenAI, Anthropic, Gemini) LLMs. Built-in privacy layer redacts PII before external API calls.
---

# NLP Enrichment with LLMs

This recipe demonstrates how to enrich posts in your database using Large Language Models (LLMs). SMDT supports various providers including **OpenAI**, **Anthropic**, **Gemini**, **Ollama**, and **Hugging Face** via the `text_generation` enricher.

## Overview

The `TextGenerationEnricher` allows you to run any LLM prompt against the `body` of posts in your database and save the structured response into the `post_enrichments` table.

Common use cases include:
- Sentiment Analysis
- Topic Classification
- Toxicity Detection
- Summarization
- Entity Extraction

## Prerequisites

Ensure you have the necessary API keys or a local LLM server running (e.g., Ollama). If you plan to use the built-in [privacy layer](#privacy-layer-optional) with `pii_policy` (rather than just `privacy_fields` on their own), install the `pii` extra: `pip install 'smdt[pii]'`.

## Configuration Reference

`TextGenerationConfig` is highly configurable, but the common case (point at one of the built-in providers) only needs a `model` and an `api_key`, via a provider factory:

| Factory | Provider | Pre-filled `base_url` |
| :--- | :--- | :--- |
| `TextGenerationConfig.for_openai(model, api_key, **kwargs)` | OpenAI | `https://api.openai.com/v1` |
| `TextGenerationConfig.for_anthropic(model, api_key, **kwargs)` | Anthropic | `https://api.anthropic.com/v1/messages` |
| `TextGenerationConfig.for_gemini(model, api_key, **kwargs)` | Gemini | `https://generativelanguage.googleapis.com/v1beta/openai/` |
| `TextGenerationConfig.for_ollama(model, base_url="http://localhost:11434/v1", **kwargs)` | Ollama (local) | as given, defaults to local |

Every other field is passed through `**kwargs` and is identical to constructing `TextGenerationConfig(...)` directly. There is no factory for Hugging Face Inference Endpoints (`provider_kind="hf-text"`) since each one has its own endpoint URL rather than one shared host. Construct `TextGenerationConfig(...)` directly for that case (see the [example below](#5-hugging-face-inference-endpoints)).

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

### Privacy layer and preprocessing

| Parameter | Default | Description |
| :--- | :--- | :--- |
| **`privacy_fields`** | `[]` | Row fields to redact/hash before they're sent anywhere, e.g. `["body"]`. Empty means the privacy layer is off. |
| **`pii_policy`** | `None` | Optional `PiiPolicy` for Presidio-based PII detection (phone numbers, emails, names, ...) on `privacy_fields`. Without one, `privacy_fields` still get baseline mention/email/URL redaction. |
| **`pepper`** | `None` | Secret pepper for the hasher. Required once `privacy_fields` is non-empty. |
| **`preprocessors`** | `[]` | Your own list of row-transform functions, applied after the privacy layer and before the LLM sees the row (e.g. to clean up artifacts the privacy layer left behind). |

See [Privacy Layer](#privacy-layer-optional) below for a full example.

## Provider Examples

### 1. OpenAI

```python
import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationConfig

db = StandardDB(db_name="my_local_db")

config = TextGenerationConfig.for_openai(
    model="gpt-4o-mini",
    api_key=os.environ["OPENAI_API_KEY"],
    model_id_postfix="v1_sentiment",  # Saved as: text_generation_v1_sentiment
    system_prompt="You are an expert sentiment analyst.",
    user_template=(
        "Classify the sentiment of the following social media post as "
        "POSITIVE, NEGATIVE, or NEUTRAL. Return only the class label.\n\n"
        "Post: {body}"
    ),
    temperature=0.0,
    batch_size=20,
    requests_per_minute=60,
    only_missing=True,
)

print("Starting Text Generation Enricher (OpenAI)...")
run_enricher("text_generation", db=db, config=config)
print("Enrichment complete.")
```

### 2. Anthropic (Claude)

```python
import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationConfig

db = StandardDB(db_name="my_local_db")

config = TextGenerationConfig.for_anthropic(
    model="claude-3-5-sonnet-20241022",
    api_key=os.environ["ANTHROPIC_API_KEY"],
    model_id_postfix="v1_sentiment_claude",
    system_prompt="You are a helpful assistant.",
    user_template="Analyze the sentiment of this post: {body}",
    only_missing=True,
    batch_size=10,
    reset_cache=True,
    max_tokens=1000,
)

print("Starting Text Generation Enricher (Anthropic)...")
run_enricher("text_generation", db=db, config=config)
print("Enrichment complete.")
```

### 3. Gemini

```python
import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationConfig

db = StandardDB(db_name="my_local_db")

config = TextGenerationConfig.for_gemini(
    model="gemini-1.5-pro",
    api_key=os.environ["GEMINI_API_KEY"],
    model_id_postfix="v1_sentiment_gemini",
    system_prompt="You are a helpful assistant.",
    user_template="Analyze the sentiment of this post: {body}",
    only_missing=True,
    batch_size=10,
)

print("Starting Text Generation Enricher (Gemini)...")
run_enricher("text_generation", db=db, config=config)
print("Enrichment complete.")
```

### 4. Local LLM with Ollama

You can use a locally running Ollama instance to save costs and keep data private (no `api_key` needed).

```python
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationConfig

db = StandardDB(db_name="my_local_db")

config = TextGenerationConfig.for_ollama(
    model="llama3",
    model_id_postfix="llama3_topic",
    system_prompt="You are a helpful classifier.",
    user_template="Identify the main topic of this text: {body}",
    batch_size=5,  # Lower batch size for local inference
    only_missing=True,
)

print("Starting Local Enrichment (Ollama)...")
run_enricher("text_generation", db=db, config=config)
print("Done.")
```

### 5. Hugging Face Inference Endpoints

Each Hugging Face Inference Endpoint has its own URL, so there is no `for_hf_text` factory. Construct `TextGenerationConfig` directly with `provider_kind="hf-text"`.

```python
import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationConfig

db = StandardDB(db_name="my_local_db")

config = TextGenerationConfig(
    provider_kind="hf-text",
    chat_model_id="meta-llama/Llama-2-7b-chat-hf",
    base_url="https://api-inference.huggingface.co/models/meta-llama/Llama-2-7b-chat-hf",
    api_key=os.environ["HF_API_KEY"],
    model_id_postfix="hf_classification",
    system_prompt="You are a helpful classifier.",
    user_template="Identify the main topic of this text: {body}",
    only_missing=True,
)

print("Starting Text Generation Enricher (Hugging Face)...")
run_enricher("text_generation", db=db, config=config)
print("Enrichment complete.")
```

## Privacy layer (optional)

Server-backed enrichers like `text_generation` send `body` to a third-party API by default. `privacy_fields`/`pii_policy` enable a built-in redaction/hashing layer (reusing `smdt.pseudonymizer`) that runs before anything leaves the machine:

```python
import os
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.text_generation import TextGenerationConfig
from smdt.pseudonymizer.pii_policy import DEFAULT_PII_POLICY

db = StandardDB(db_name="my_local_db")

config = TextGenerationConfig.for_anthropic(
    model="claude-3-5-sonnet-20241022",
    api_key=os.environ["ANTHROPIC_API_KEY"],
    model_id_postfix="v1_sentiment_claude",
    system_prompt="You are a helpful assistant.",
    user_template="Analyze the sentiment of this post: {body}",
    only_missing=True,
    batch_size=10,
    # Privacy layer: mentions, emails, phone numbers, and names in
    # "body" are redacted or hashed before the row leaves the machine.
    privacy_fields=["body"],
    pii_policy=DEFAULT_PII_POLICY,
    pepper=os.environ["PSEUDONYMIZATION_PEPPER"].encode(),
)

run_enricher("text_generation", db=db, config=config)
```

If `base_url` points at a known commercial API host (OpenAI, Anthropic, Gemini, or the Hugging Face Inference API) and `privacy_fields` is left empty, SMDT logs a warning so this isn't a silent default. Without a `pii_policy`, `privacy_fields` still get baseline mention/email/URL redaction via the dependency-free `Redactor`; `pii_policy` (e.g. `DEFAULT_PII_POLICY`) upgrades this to Presidio-based detection covering phone numbers, credit cards, and person names.

Need to clean something up afterward, like leftover formatting artifacts the redaction pass introduces? Add your own `preprocessors=[...]`. It runs after the privacy layer and before the LLM sees the row.

## Viewing Results

The method for retrieving results depends on your `do_save_to_db` setting.

### Option A: Database Storage (`do_save_to_db=True`)

This is the default. Results are stored in the `post_enrichments` table, with the LLM's response under the `text` key. You can query them using SQL:

```sql
SELECT 
    p.body AS original_text, 
    pe.body ->> 'text' AS llm_response 
FROM posts p
JOIN post_enrichments pe ON p.post_id = pe.post_id
WHERE pe.model_id = 'text_generation_v1_sentiment'
LIMIT 10;
```

Or using Python:

```python
from smdt.store.standard_db import StandardDB

db = StandardDB("my_local_db", initialize=False)
conn = db.connect()
try:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT body ->> 'text' 
            FROM post_enrichments 
            WHERE model_id = %s 
            LIMIT 5
            """,
            ("text_generation_v1_sentiment",)
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
            post_id = record["post_id"]

            # The actual LLM output is inside the 'body' dictionary
            llm_text = record["body"]["text"] 
            
            print(f"[{post_id}] {llm_text}")
```
