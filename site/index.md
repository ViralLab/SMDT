---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: '<span id="hero-typewriter" style="opacity:0">Social Media Data Toolkit</span><span class="cursor">|</span>'
  text: 'SMDT'
  actions:
    - theme: brand
      text: API Reference
      link: /api/
    - theme: alt
      text: 📄 Paper
      link: https://example.com/paper
      target: _blank
    - theme: alt
      text: 📜 arXiv
      link: https://arxiv.org/abs/xxxx.xxxxx
      target: _blank

features:
  - title: Data Standardization
    details: Unified schemas for Twitter, Reddit, Bluesky, Telegram, and more.
  - title: Content Enrichment
    details: Powerful pipelines for NLP, toxicity detection, and entity extraction.
  - title: Network Analysis
    details: Built-in tools for constructing and analyzing social graphs.
---

## What is SMDT?

**SMDT (Social Media Data Toolkit)** is a comprehensive Python library designed to streamline the ingestion, standardization, and analysis of social media data. It provides a unified interface for handling data from diverse platforms, enabling researchers to focus on analysis rather than data wrangling.

## Why use SMDT?

- **Unified Schema**: Convert messy JSON dumps from various platforms into a consistent, queryable format.
- **Modular Design**: Easily plug in new data readers, enrichers, or analysis modules.
- **Research Ready**: Built specifically for computational social science workflows, supporting reproducibility and scalability.

## Citation

If you use SMDT in your research, please cite the following paper:

```bibtex
@article{smdt2024,
  title={SMDT: A Social Media Data Toolkit for Computational Social Science},
  author={Viral Lab},
  journal={arXiv preprint arXiv:xxxx.xxxxx},
  year={2026}
}
```
