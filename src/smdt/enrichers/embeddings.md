### Self-hosted (e.g. vLLM)

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

### OpenAI

```python
import os
from smdt.enrichers.embeddings import EmbeddingConfig, EmbeddingEnricher
from smdt.store.standard_db import StandardDB

db = StandardDB(db_name="smdt_twitter")

cfg = EmbeddingConfig.for_openai(
    model="text-embedding-3-small",
    api_key=os.environ["OPENAI_API_KEY"],
    model_id_postfix="openai_small",
    batch_size=64,
)

enricher = EmbeddingEnricher(db, config=cfg)
enricher.run()
```
