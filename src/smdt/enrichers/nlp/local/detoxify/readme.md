# Sample Code

```python
from smdt.store.standard_db import StandardDB
from smdt.enrichers.nlp.local.detoxify.detoxify import DetoxifyConfig, DetoxifyToxicityEnricher

config = DetoxifyConfig(
    model_name="multilingual",
    model_batch_size=16,
    do_save_to_db=False,
    output_dir="/somewhere/output/",

)

# Initialize and Run
enricher = DetoxifyToxicityEnricher(db, config=config)
enricher.run()

```