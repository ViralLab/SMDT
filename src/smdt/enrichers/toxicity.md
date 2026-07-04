# Sample Code

```python
from smdt.store.standard_db import StandardDB
from smdt.enrichers.toxicity import ToxicityConfig, ToxicityEnricher

db = StandardDB(db_name="your_db")

config = ToxicityConfig(
    model_name="multilingual",
    model_batch_size=16,
    do_save_to_db=False,
    output_dir="/somewhere/output/",
)

# Initialize and Run
enricher = ToxicityEnricher(db, config=config)
enricher.run()
```