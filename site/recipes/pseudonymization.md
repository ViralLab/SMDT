# Pseudonymization

SMDT provides a built-in `Pseudonymizer` to process a source database into a destination database, applying hashing to identifiers and redaction to text fields. This is crucial for sharing datasets while preserving privacy and maintaining data linkability for analysis.

## Features

- **Pseudonymization**: Hashes user IDs, usernames, and other identifiers using a configurable pepper and algorithm (SHA256, etc.). This ensures the same user always maps to the same hash, allowing for network graph construction.
- **Redaction**: Detects and replaces sensitive entities (like @mentions) in text fields.
- **PII Detection**: Optional [Presidio](https://github.com/data-privacy-stack/presidio)-based scanning of free text for broader identifier-grade PII (phone numbers, emails, credit cards, person names, ...), on top of platform-aware `@mention`/`#hashtag` handling.
- **Configurable Policy**: Define per-table rules for what to hash, redact, drop, or blank out.
- **Batched Processing**: memory-efficient processing of large tables.

## Basic Usage

The process uses the `Pseudonymizer` class, which requires a configuration (`PseudonymizeConfig`) and a policy (`PseudonymPolicy`).

### 1. Configuration

Setup the database connection details and hashing parameters.

```python
from smdt.pseudonymizer import PseudonymizeConfig, Algorithm
from smdt.config import PseudonymizationVariables

# Load secrets (like the pepper) from environment variables or .env file
pseudo_vars = PseudonymizationVariables()

cfg = PseudonymizeConfig(
    src_db_name="source_db",       # The database to read from
    dst_db_name="target_db_pseudo",  # The database to write to (will be created/overwritten)
    pepper=pseudo_vars.pepper,       # Secret pepper for hashing
    algorithm=Algorithm.SHA256,    # Hashing algorithm
    ask_reinit=True,               # Ask before wiping the destination DB
    chunk_rows=5_000,              # Process rows in batches
)
```

### 2. Policy

The `PseudonymPolicy` dictates how each column in each table should be handled. You can use the `DEFAULT_POLICY` which covers standard SMDT schema fields (like `users.id`, `tweets.author_id`, etc.), or define your own.

```python
from smdt.pseudonymizer import PseudonymPolicy, DEFAULT_POLICY

# You can use the default policy directly
policy = DEFAULT_POLICY

# Or create a custom one
custom_policy = PseudonymPolicy(
    hash_cols={
        "users": {"id", "username"},
        "tweets": {"author_id", "in_reply_to_user_id"}
    },
    redact_cols={
        "tweets": {"text"}
    },
    drop_cols={
        "users": {"email", "phone"}
    },
    blank_cols={
        "users": {"location"} # Keeps column but sets values to NULL
    }
)
```

### 3. PII Detection (optional)

By default, `bio`/`body` columns are redacted with a dependency-free, regex-based pass (mentions, emails, URLs only). Installing the `pii` extra (`pip install 'smdt[pii]'`) and providing a `PiiPolicy` upgrades this to a [Presidio](https://github.com/data-privacy-stack/presidio)-based engine that additionally detects phone numbers, credit cards, person names, and more — configurable per table/column/entity-type, with three possible actions per entity type:

- `HASH` — pepper-keyed via the same `Hasher` used for identifiers, so e.g. a `@mention` in a post body hashes identically to the mentioned account's own `username`.
- `REPLACE` — a fixed placeholder (`"[PHONE_NUMBER]"`) or a callable transform (e.g. reducing a URL to just its domain).
- `DROP` — remove the matched span entirely.

`@mention`/`#hashtag` detection is platform-aware: it's selected using each row's `platform` column (e.g. Weibo's `#topic#` double-wrapped hashtags vs. Twitter's single leading `#`), and you can register your own recognizers for additional platform quirks or organization-specific patterns (e.g. internal ticket IDs).

This is strictly opt-in — without a `pii_policy`, nothing changes from the dependency-free default.

```python
from smdt.pseudonymizer import PiiPolicy, PiiAction, PiiRule, DEFAULT_PII_POLICY

# DEFAULT_PII_POLICY already covers MENTION/HASHTAG/URL/PHONE_NUMBER/
# EMAIL_ADDRESS/CREDIT_CARD/PERSON on accounts.bio, communities.bio, posts.body.
pii_policy = DEFAULT_PII_POLICY

# Or define your own, per (table, column, entity_type):
custom_pii_policy = PiiPolicy(
    rules={
        "posts": {
            "body": {
                "MENTION": PiiRule(PiiAction.HASH),
                "PHONE_NUMBER": PiiRule(PiiAction.DROP),
                "EMPLOYEE_ID": PiiRule(PiiAction.REPLACE, replacement="[EMPLOYEE_ID]"),
            }
        }
    }
)

cfg = PseudonymizeConfig(
    src_db_name="source_db",
    dst_db_name="target_db_pseudo",
    pepper=pseudo_vars.pepper,
    pii_policy=pii_policy,
    # No automatic language detection -- you choose the NLP model(s).
    # Pattern-based entities (PHONE_NUMBER, EMAIL_ADDRESS, CREDIT_CARD, ...)
    # work regardless of language; NER-based entities (PERSON) need a model
    # for that language, or they won't be detected.
    nlp_configuration={
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    },
)
```

### 4. Running the Process

Combine the config and policy to run the pseudonymization process.

```python
from smdt.pseudonymizer import Pseudonymizer

# Initialize
pz = Pseudonymizer(cfg, policy)

# Run the process
pz.run()
```

## Complete Example

Here is a complete script to pseudonymize a dataset.

```python
import logging
from smdt.pseudonymizer import Pseudonymizer, PseudonymizeConfig, Algorithm, DEFAULT_POLICY
from smdt.config import PseudonymizationVariables

# Setup logging
logging.basicConfig(level=logging.INFO)

def main():
    # Load pseudonymization variables (requires .env file with SMDT_PEPPER set)
    pseudo_vars = PseudonymizationVariables()

    # Configuration
    cfg = PseudonymizeConfig(
        src_db_name="social_media_raw",
        dst_db_name="social_media_public",
        pepper=pseudo_vars.pepper,
        algorithm=Algorithm.SHA256,
        ask_reinit=True,
        chunk_rows=10_000,
    )

    # Initialize Pseudonymizer with default policy
    # The default policy automatically handles:
    # - Hashing user IDs, usernames, and foreign keys (author_id, etc.)
    # - Redacting text content in standard tables
    pz = Pseudonymizer(cfg, DEFAULT_POLICY)

    # Execute
    pz.run()

if __name__ == "__main__":
    main()
```
