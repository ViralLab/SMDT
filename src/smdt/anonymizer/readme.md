```python

from smdt.anonymizer import Anonymizer, AnonymizeConfig, Algorithm, DEFAULT_POLICY
from smdt.config import AnonymizationVariables


anon_vars = AnonymizationVariables()
cfg = AnonymizeConfig(
    src_db_name="truth_social",
    dst_db_name="truth_social_anon",
    pepper=anon_vars.pepper,
    algorithm=Algorithm.SHA256,
    output_hex_len=64,
    replace_numbers=False,
    ask_reinit=True,
    chunk_rows=5_000,
)

az = Anonymizer(cfg, DEFAULT_POLICY)
az.run()
```
