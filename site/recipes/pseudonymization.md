# Pseudonymization

SMDT provides a built-in `Anonymizer` (which primarily performs pseudonymization) to process a source database into a destination database, applying hashing to identifiers and redaction to text fields. This is crucial for sharing datasets while preserving privacy and maintaining data linkability for analysis.

## Features

- **Pseudonymization**: Hashes user IDs, usernames, and other identifiers using a configurable pepper and algorithm (SHA256, etc.). This ensures the same user always maps to the same hash, allowing for network graph construction.
- **Redaction**: Detects and replaces sensitive entities (like @mentions) in text fields.
- **Configurable Policy**: Define per-table rules for what to hash, redact, drop, or blank out.
- **Batched Processing**: memory-efficient processing of large tables.

## Basic Usage

The process uses the `Anonymizer` class, which requires a configuration (`AnonymizeConfig`) and a policy (`AnonPolicy`).

### 1. Configuration

Setup the database connection details and hashing parameters.

```python
from smdt.anonymizer import AnonymizeConfig, Algorithm
from smdt.config import AnonymizationVariables

# Load secrets (like the pepper) from environment variables or .env file
anon_vars = AnonymizationVariables()

cfg = AnonymizeConfig(
    src_db_name="source_db",       # The database to read from
    dst_db_name="target_db_anon",  # The database to write to (will be created/overwritten)
    pepper=anon_vars.pepper,       # Secret pepper for hashing
    algorithm=Algorithm.SHA256,    # Hashing algorithm
    output_hex_len=64,             # Length of the resulting hash
    ask_reinit=True,               # Ask before wiping the destination DB
    chunk_rows=5_000,              # Process rows in batches
)
```

### 2. Policy

The `AnonPolicy` dictates how each column in each table should be handled. You can use the `DEFAULT_POLICY` which covers standard SMDT schema fields (like `users.id`, `tweets.author_id`, etc.), or define your own.

```python
from smdt.anonymizer import AnonPolicy, DEFAULT_POLICY

# You can use the default policy directly
policy = DEFAULT_POLICY

# Or create a custom one
custom_policy = AnonPolicy(
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

### 3. Running the Process

Combine the config and policy to run the pseudonymization process.

```python
from smdt.anonymizer import Anonymizer

# Initialize
az = Anonymizer(cfg, policy)

# Run the process
az.run()
```

## Complete Example

Here is a complete script to pseudonymize a dataset.

```python
import logging
from smdt.anonymizer import Anonymizer, AnonymizeConfig, Algorithm, DEFAULT_POLICY
from smdt.config import AnonymizationVariables

# Setup logging
logging.basicConfig(level=logging.INFO)

def main():
    # Load anonymization variables (requires .env file with SMDT_PEPPER set)
    anon_vars = AnonymizationVariables()

    # Configuration
    cfg = AnonymizeConfig(
        src_db_name="social_media_raw",
        dst_db_name="social_media_public",
        pepper=anon_vars.pepper,
        algorithm=Algorithm.SHA256,
        output_hex_len=64,
        ask_reinit=True,
        chunk_rows=10_000,
    )

    # Initialize Anonymizer with default policy
    # The default policy automatically handles:
    # - Hashing user IDs, usernames, and foreign keys (author_id, etc.)
    # - Redacting text content in standard tables
    az = Anonymizer(cfg, DEFAULT_POLICY)

    # Execute
    az.run()

if __name__ == "__main__":
    main()
```
