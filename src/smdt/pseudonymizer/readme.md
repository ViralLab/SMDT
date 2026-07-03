```python

from smdt.pseudonymizer import Pseudonymizer, PseudonymizeConfig, Algorithm, DEFAULT_POLICY
from smdt.config import PseudonymizationVariables


pseudo_vars = PseudonymizationVariables()
cfg = PseudonymizeConfig(
    src_db_name="db",
    dst_db_name="db_pseudo",
    pepper=pseudo_vars.pepper,
    algorithm=Algorithm.SHA256,
    ask_reinit=True,
    chunk_rows=5_000,
)

pz = Pseudonymizer(cfg, DEFAULT_POLICY)
pz.run()
```
