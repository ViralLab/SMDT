# Sentence Classification Enricher

A flexible, production-ready sentence classification tool built on top of the `smdt` framework. This enricher allows you to run any HuggingFace BERT-family model (e.g., Sentiment Analysis, Toxicity Detection, Emotion Classification) over database records.

## Features

* **Generic Mode**: Automatically switches between **Sigmoid** (binary) and **Softmax** (multi-class) activations based on the model's architecture.
* **Logit Preservation**: Stores both the final probabilities (`scores`) and raw `logits` for advanced analysis.
* **Dynamic Labeling**: Automatically extracts class names (e.g., "positive", "neutral", "negative") from the HuggingFace model configuration.
* **Batch Processing**: Highly optimized for GPU inference using configurable batch sizes.
* **Persistence**: Supports direct injection into a PostgreSQL database or export to `.jsonl` files.

## Installation

Ensure you have the required dependencies installed:

```bash
pip install torch transformers smdt
```



```python
from smdt.store.standard_db import StandardDB
from your_module import SentenceClfEnricher, ModelConfig

# Initialize DB connection
db = StandardDB(db_name="your_db")

# Setup Config
config = {
    "hf_model_id": "cardiffnlp/twitter-roberta-base-sentiment",
    "model_batch_size": 16,
    "do_save_to_db": True
}

# Initialize and Run
enricher = SentenceClfEnricher(db, config=config)
enricher.run()
```