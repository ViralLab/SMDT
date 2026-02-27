# Recipes

Welcome to the SMDT recipes collection. This section provides step-by-step guides and patterns for common tasks, from setting up your first project to advanced data analysis.

## Essentials

Start here to understand the core concepts of SMDT.

- **[Getting Started](./getting-started.md)**  
  Learn how to verify your installation, configure your database connection, and run your first standardizer.

## Ingestion & Standardization

Learn how to bring data into the SMDT ecosystem.

- **[Using Ingestion Pipelines](./using-pipelines.md)**  
  A guide to the core pipeline system. Learn how to discover files, manage bulk database insertions, and handle errors efficiently.

- **[Standardizing Twitter API v2 Data](./standardizing-twitter-v2.md)**  
  A complete guide to processing raw Twitter v2 JSON data. Covers generating sample data and using the `TwitterV2Standardizer` within an ingestion pipeline.

- **[Building a Custom Standardizer](./building-custom-standardizer.md)**  
  Need to import data from a new source? This guide walks you through creating a custom standardizer class to map any data format to the SMDT schema.

## Enrichment

Unlock insights from your social media data.

- **[NLP Enrichment with LLMs](./enrichment/nlp.md)**  
  Enhance your text data using Large Language Models (LLMs). Learn how to configure local (Ollama, HF) or remote (OpenAI) models to perform tasks like sentiment analysis, toxicity detection, and topic classification.

## Data Privacy

Ensure your data is safe to share.

- **[Pseudonymization](./pseudonymization.md)**  
  Learn how to pseudonymize identifiers and redact sensitive text using the built-in Anonymizer and configurable policies.

## Analysis

Verify your data quality and schema health.

- **[Using the Database Inspector](./analysis/inspector.md)**  
  Learn how to generate reports on table row counts, column completeness, and enum distributions.

## Network Analysis

Construct and analyze networks from your data.

- **[Network Construction](./networks/construction.md)**  
  Create networks such as Entity Co-occurrence (Hashtags), Bipartite (User-Hashtag), or basic User Interaction graphs over a specific time window.

- **[Temporal Networks](./networks/temporal.md)**  
  Analyze how interactions evolve over time. This recipe shows how to extract temporal networks (e.g., weekly retweet graphs) from your database for use with tools like Gephi or NetworkX.

