# Recipes

Welcome to the SMDT recipes collection. Every dataset goes through roughly the same journey: **ingest** raw platform exports into a standardized database, optionally **enrich** and **protect** that data, then **analyze** it. The recipes below are ordered to match that journey — read them in order the first time through, or jump straight to whichever step you need.

## Start Here

New to SMDT? Start with [Getting Started](./getting-started.md) — it verifies your installation, configures your database connection, and runs your first standardizer. Every other recipe assumes you've done this.

## 1. Ingest & Verify Your Data

Bring raw platform exports into SMDT's standardized schema, then check the result before building on top of it.

- **[Using Ingestion Pipelines](./using-pipelines.md)** — the core pipeline system: discovering files, batching database inserts, and handling errors at scale.
- **[Standardizing Twitter API v2 Data](./standardizing-twitter-v2.md)** — a complete, concrete walkthrough using `TwitterV2Standardizer` end to end.
- **[Using the Database Inspector](./analysis/inspector.md)** — sanity-check what you just ingested: row counts, per-column completeness, and enum distributions, before you rely on the data for anything else.

## 2. Enrich Your Data

Add computed features to posts you've already ingested — sentiment, toxicity, language, embeddings, or any custom signal.

- **[NLP Enrichment with LLMs](./enrichment/nlp.md)** — configure local (Ollama, Hugging Face) or hosted (OpenAI, Anthropic, Gemini) models for tasks like sentiment analysis, toxicity detection, and topic classification, including the built-in privacy layer for hosted providers.

## 3. Protect & Share Your Data

Before sharing a dataset outside your team, pseudonymize identifiers and redact free text.

- **[Pseudonymization](./pseudonymization.md)** — hash identifiers and redact sensitive text with configurable policies, detect broader PII with the optional Presidio-based engine, and handle GDPR erasure requests.

## 4. Analyze Your Data

Turn standardized (and optionally enriched/protected) data into networks and cross-dataset insight.

- **[Network Construction](./networks/construction.md)** — build entity co-occurrence, bipartite, and user-interaction graphs over a time window.
- **[Temporal Networks](./networks/temporal.md)** — extract how interactions evolve over time (e.g. weekly retweet graphs) for tools like Gephi or NetworkX.
- **[Cross-Platform Analysis (MultiStore)](./analysis/multistore.md)** — attach multiple per-dataset databases into one DuckDB connection and join/union across them with plain SQL.

## Advanced & Reference

Read these only if you need them — they extend SMDT rather than continue the core journey above.

- **[Building a Custom Standardizer](./building-custom-standardizer.md)** — map a new, unsupported data source into SMDT's schema.
- **[Building a Custom Enricher](./enrichment/building-custom-enricher.md)** — write your own enricher from scratch, step by step.
