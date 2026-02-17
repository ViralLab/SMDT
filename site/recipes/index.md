# Recipes

Welcome to the SMDT recipes collection. This section provides step-by-step guides and patterns for common tasks, from setting up your first project to advanced data analysis.

## Essentials

Start here to understand the core concepts of SMDT.

- **[Getting Started](./getting-started.md)**  
  Learn how to verify your installation, configure your database connection, and run your first standardizer.

## Ingestion & Standardization

Learn how to bring data into the SMDT ecosystem.

- **[Standardizing Twitter API v2 Data](./standardizing-twitter-v2.md)**  
  A complete guide to processing raw Twitter v2 JSON data. Covers generating sample data and using the `TwitterV2Standardizer` within an ingestion pipeline.

- **[Building a Custom Standardizer](./building-custom-standardizer.md)**  
  Need to import data from a new source? This guide walks you through creating a custom standardizer class to map any data format to the SMDT schema.

## Enrichment & Analysis

Unlock insights from your social media data.

- **[NLP Enrichment with LLMs](./nlp-enrichment.md)**  
  Enhance your text data using Large Language Models (LLMs). Learn how to configure local (Ollama, HF) or remote (OpenAI) models to perform tasks like sentiment analysis, toxicity detection, and topic classification.

- **[Generating Temporal Interaction Networks](./temporal-networks.md)**  
  Analyze how interactions evolve over time. This recipe shows how to extract temporal networks (e.g., weekly retweet graphs) from your database for use with tools like Gephi or NetworkX.

