```python
from smdt.enrichers.embeddings import EmbeddingConfig, EmbeddingEnricher
from smdt.store.standard_db import StandardDB

db = StandardDB(db_name="smdt_twitter")

cfg = EmbeddingConfig(
    embedding_model_id="intfloat/e5-large",   # served by vLLM (or any OpenAI-compatible embeddings endpoint)
    base_url="http://localhost:8010/v1",
    api_key="NO_KEY_NEEDED",
    model_id_postfix="e5-large",              # optional; leave unset for plain "embeddings"

    batch_size=64,
)

enricher = EmbeddingEnricher(db, config=cfg)
enricher.run()
```
