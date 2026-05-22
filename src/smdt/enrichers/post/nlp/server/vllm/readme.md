```python
from smdt.enrichers.vllm.textgen_vllm import VLLMTextGenConfig, VLLMTextGenEnricher
from smdt.store.standard_db import StandardDB

db = StandardDB(db_name="smdt_twitter")  # your existing helper

cfg = VLLMTextGenConfig(
    model_id_postfix="qwen25-14b_summarize_tr",
    chat_model_id="qwen2.5-14b-instruct",     # served by vLLM
    base_url="http://localhost:8010/v1",
    api_key="NO_KEY_NEEDED",

    # prompt plumbing
    prompt_path="prompts.yml",
    prompt_id="summarize_tr",
    extra_vars={"lang": "tr"},

    # adapter selection (vLLM/OpenAI-compatible)
    provider_kind="openai",
    provider_model="qwen2.5-14b-instruct",

    batch_size=64,
    max_tokens=128,
)

enricher = VLLMTextGenEnricher(db, config=cfg)
# use your existing runner or call enricher via your pipeline
```