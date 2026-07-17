# Pseudonymization

SMDT provides a built-in `Pseudonymizer` to process a source database into a destination database, applying hashing to identifiers and redaction to free-text fields. The same real identifier always maps to the same pseudonym, so network graphs and cross-table joins survive the transformation intact.

## Quickstart

```python
from smdt.pseudonymizer import Pseudonymizer, PseudonymizeConfig, Algorithm, DEFAULT_POLICY
from smdt.config import PseudonymizationVariables

pseudo_vars = PseudonymizationVariables()

cfg = PseudonymizeConfig(
    src_db_name="my_dataset",
    dst_db_name="my_dataset_pseudo",
    pepper=pseudo_vars.pepper,
    algorithm=Algorithm.SHA256,
    ask_reinit=True,
    chunk_rows=5_000,
)

Pseudonymizer(cfg, DEFAULT_POLICY).run()
```

That is all you need. The default policy handles the standard SMDT schema: it hashes all identifier columns, redacts `body` and `bio` columns, blanks profile image URLs, and copies everything else unchanged.

## How It Works

The pseudonymizer reads from your source database and writes to a new destination database with the same schema. Each column in each table gets one of five treatments:

| Action | Effect | Example |
|---|---|---|
| **Hash** | Replaced with a pepper-keyed SHA-256 hash | `account_id`, `username`, `post_id` |
| **Redact** | Mentions, emails, and URLs are scrubbed | `body`, `bio` |
| **Blank** | Set to NULL | `profile_image_url` |
| **Drop** | Column removed entirely | Not used by default |
| **Keep** | Copied unchanged (default) | `created_at`, `platform`, counts |

## Customizing the Policy

If you need different per-column rules, create a custom `PseudonymPolicy`:

```python
from smdt.pseudonymizer import PseudonymPolicy, DEFAULT_POLICY

custom_policy = PseudonymPolicy(
    hash_cols={
        "accounts": {"account_id", "username"},
        "posts": {"account_id", "post_id", "conversation_id"},
    },
    redact_cols={
        "posts": {"body"},
        "accounts": {"bio"},
    },
    blank_cols={
        "accounts": {"profile_image_url", "location"},
    },
    drop_cols={},
)

Pseudonymizer(cfg, custom_policy).run()
```

When a column appears in multiple action sets, priority is: **Drop > Hash > Redact > Blank > Keep**.

::: details Optional: PII Detection with Presidio
By default, free-text redaction uses a dependency-free regex engine that catches `@mentions`, emails, and URLs. Installing the `pii` extra enables [Presidio](https://github.com/data-privacy-stack/presidio)-based detection of broader PII: phone numbers, credit cards, and personal names. This is opt-in and requires an NLP model.

```bash
pip install 'smdt[pii]'
python -m spacy download en_core_web_lg
```

```python
from smdt.pseudonymizer import PiiPolicy, PiiAction, PiiRule, DEFAULT_PII_POLICY

pii_policy = DEFAULT_PII_POLICY  # covers accounts.bio, posts.body, communities.bio

cfg = PseudonymizeConfig(
    src_db_name="my_dataset",
    dst_db_name="my_dataset_pseudo",
    pepper=pseudo_vars.pepper,
    pii_policy=pii_policy,
    nlp_configuration={
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    },
)

Pseudonymizer(cfg, DEFAULT_POLICY).run()
```

Each entity type gets its own action: mentions are hashed for consistency with identifiers, phone numbers and credit cards become `[PHONE_NUMBER]` and `[CREDIT_CARD]`, URLs are reduced to their domain. See the `PiiPolicy` documentation for custom rules.
:::

::: details Optional: Parallel Processing
For large tables with substantial free-text content, the CPU-bound hashing and redaction can be distributed across worker processes. Database reads and writes stay in the main process.

```python
cfg = PseudonymizeConfig(
    src_db_name="my_dataset",
    dst_db_name="my_dataset_pseudo",
    pepper=pseudo_vars.pepper,
    num_workers=8,
    transform_chunk_size=500,
)
```
:::

::: details Optional: GDPR Erasure
`Eraser` handles deletion requests by recomputing the pseudonym for a given real identity and locating their data. It supports hard deletion or in-place scrubbing across the source database, the pseudonymized database, or both.

```python
from smdt.pseudonymizer import Eraser, ErasureTarget, ErasureMode

eraser = Eraser(
    targets=[
        ErasureTarget(db_name="my_dataset", mode=ErasureMode.DELETE,
                      is_pseudonymized=False),
        ErasureTarget(db_name="my_dataset_pseudo", mode=ErasureMode.SCRUB,
                      is_pseudonymized=True),
    ],
    pepper=pseudo_vars.pepper,
)

report = eraser.erase("real_account_id_123", identity_column="account_id")
```

Scrubbing nulls personal columns but keeps posts in place so other users' replies and interactions are not orphaned. There is no reverse mapping table; erasure works by forward recomputation only.
:::

## Prerequisites

- A database ingested with SMDT (see [Standardizing Twitter v2](./standardizing-twitter-v2.md))
- A `PEPPER` environment variable set (the secret used for keyed hashing)
- `PiiPolicy` users also need `pip install 'smdt[pii]'` and a spaCy model
