---
description: Build a custom enricher from scratch in SMDT. Step-by-step tutorial covering fetch, preprocess, process, and save, with a complete runnable example.
---

# Building a Custom Enricher

This recipe walks through writing your own enricher from scratch: a `WordCountEnricher` that writes `{"word_count": int, "char_count": int}` for every post. It builds the class one method at a time, then shows the complete file, how to register and run it, and how to test it.

## The Contract

Every enricher subclasses `BaseEnricher` and implements four abstract methods:

| Method | Responsibility |
| :--- | :--- |
| `total_count()` | How many rows are left to process (or `None` if unknown). |
| `fetch_batch(offset, limit)` | Read one batch of rows to enrich. |
| `process_batch(rows)` | Do the actual enrichment; return result dicts. |
| `save_results(results)` | Persist results (DB or JSONL). |

`setup()`, `load_model()`, and `teardown()` are optional lifecycle hooks (no-ops by default) for opening connections, loading a model, or cleanup.

In exchange, `BaseEnricher.run()` gives you, for free, without writing any code for them:

- Batched iteration with a progress bar
- The `only_missing`/`reset_cache` resumable-run cache
- The [privacy layer](/recipes/enrichment/nlp#privacy-layer-optional) (`privacy_fields`/`pii_policy`) and the `preprocessors` pipeline, applied to every batch before `process_batch` ever sees it
- `do_save_to_db=False` → JSONL file output, handled the same way for every enricher

You never call any of this directly -- just implement the four methods and configure behavior through your config class.

## Step 1: Define Your Config

Every enricher's config subclasses `EnricherRunConfig`, which already provides `only_missing`, `reset_cache`, `cache_dir`, `do_save_to_db`, `output_dir`, `privacy_fields`, `pii_policy`, `pepper`, and `preprocessors`. Add only what's specific to your enricher:

```python
from dataclasses import dataclass
from typing import Optional
from smdt.enrichers.base import EnricherRunConfig


@dataclass
class WordCountConfig(EnricherRunConfig):
    """Configuration for WordCountEnricher.

    Attributes:
        model_id_postfix: Optional suffix appended to form the
            ``post_enrichments.model_id`` key (``"word_count_<postfix>"``).
    """
    model_id_postfix: Optional[str] = None
```

## Step 2: `__init__`

Every enricher's `__init__` follows the same four-line pattern: call the parent constructor, coerce the config, build `model_id`, and initialize the cache.

```python
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from smdt.enrichers.base import BaseEnricher
from smdt.store.standard_db import StandardDB


class WordCountEnricher(BaseEnricher):
    def __init__(self, db: StandardDB, *, config: Optional[Dict[str, Any]] = None):
        super().__init__(db)
        self.cfg = self._coerce_config(config, WordCountConfig)
        self.model_id = self._make_model_id(self.cfg.model_id_postfix)
        self.applied_datetime = datetime.now(timezone.utc)
        self._init_cache()
```

`_coerce_config` accepts a ready `WordCountConfig` instance, a plain dict, or `None`. `_make_model_id` builds the value that ends up in `post_enrichments.model_id` -- just `"word_count"` here, or `"word_count_<postfix>"` if you set `model_id_postfix`.

## Step 3: `total_count` and `fetch_batch`

These read from the database. Respect `self.cfg.only_missing` so re-running the enricher skips posts it already scored:

```python
from typing import Any, Dict, List


def total_count(self) -> int:
    where = ["p.body IS NOT NULL", "p.body <> ''"]
    params: List[Any] = []
    if self.cfg.only_missing:
        where.append(
            "NOT EXISTS (SELECT 1 FROM post_enrichments pe "
            "WHERE pe.post_id::text = p.post_id::text AND pe.model_id = %s)"
        )
        params.append(self.model_id)
    q = f"SELECT COUNT(*) FROM posts p WHERE {' AND '.join(where)}"
    conn = self.db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(q, params)
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()


def fetch_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
    where = ["p.body IS NOT NULL", "p.body <> ''"]
    params: List[Any] = []
    if self.cfg.only_missing:
        where.append(
            "NOT EXISTS (SELECT 1 FROM post_enrichments pe "
            "WHERE pe.post_id::text = p.post_id::text AND pe.model_id = %s)"
        )
        params.append(self.model_id)
    q = (
        f"SELECT p.post_id, p.body, created_at, retrieved_at FROM posts p "
        f"WHERE {' AND '.join(where)} ORDER BY p.id OFFSET %s LIMIT %s"
    )
    params.extend([offset, limit])
    conn = self.db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
```

`fetch_batch` returns plain dicts, keyed by column name -- this is the shape `process_batch`, the privacy layer, and `preprocessors` all operate on.

## Step 4: `process_batch`

This is the only method with logic specific to *this* enricher. It receives rows that have already been through the privacy layer and any configured `preprocessors`:

```python
def process_batch(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        body = r.get("body") or ""
        out.append(
            {
                "created_at": r.get("created_at"),
                "post_id": r["post_id"],
                "model_id": self.model_id,
                "body": {"word_count": len(body.split()), "char_count": len(body)},
            }
        )
    return out
```

The returned `"body"` key is what becomes the JSONB payload in `post_enrichments.body` -- it can be any JSON-serializable dict, shaped however your enricher needs.

## Step 5: `save_results`

Support both output modes so your enricher works whether or not the caller wants database writes:

```python
import json
from pathlib import Path
from smdt.store.models import PostEnrichments


def save_results(self, results: List[Dict[str, Any]]) -> None:
    if not results:
        return
    if self.cfg.do_save_to_db:
        objs = [
            PostEnrichments(
                created_at=r["created_at"],
                retrieved_at=self.applied_datetime,
                post_id=r["post_id"],
                model_id=r["model_id"],
                body=r["body"],
            )
            for r in results
        ]
        self.db.insert_with_fallbacks(objs)
    else:
        output_base = Path(self.cfg.output_dir)
        output_base.mkdir(parents=True, exist_ok=True)
        outp = output_base / f"{self.model_id}.jsonl"
        with outp.open("a", encoding="utf-8") as f:
            for r in results:
                f.write(
                    json.dumps(
                        {"post_id": r["post_id"], "model_id": r["model_id"], "body": r["body"]},
                        default=str,
                    )
                    + "\n"
                )
```

## Putting It All Together

```python
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from smdt.enrichers.base import BaseEnricher, EnricherRunConfig
from smdt.enrichers.registry import register
from smdt.store.models import PostEnrichments
from smdt.store.standard_db import StandardDB


@dataclass
class WordCountConfig(EnricherRunConfig):
    """Configuration for WordCountEnricher.

    Attributes:
        model_id_postfix: Optional suffix appended to form the
            ``post_enrichments.model_id`` key (``"word_count_<postfix>"``).
    """
    model_id_postfix: Optional[str] = None


@register(
    "word_count",
    target="posts",
    description="Counts words and characters in post bodies",
)
class WordCountEnricher(BaseEnricher):
    """Writes ``{"word_count": int, "char_count": int}`` per post."""

    def __init__(self, db: StandardDB, *, config: Optional[Dict[str, Any]] = None):
        super().__init__(db)
        self.cfg = self._coerce_config(config, WordCountConfig)
        self.model_id = self._make_model_id(self.cfg.model_id_postfix)
        self.applied_datetime = datetime.now(timezone.utc)
        self._init_cache()

    def total_count(self) -> int:
        where = ["p.body IS NOT NULL", "p.body <> ''"]
        params: List[Any] = []
        if self.cfg.only_missing:
            where.append(
                "NOT EXISTS (SELECT 1 FROM post_enrichments pe "
                "WHERE pe.post_id::text = p.post_id::text AND pe.model_id = %s)"
            )
            params.append(self.model_id)
        q = f"SELECT COUNT(*) FROM posts p WHERE {' AND '.join(where)}"
        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(q, params)
                row = cur.fetchone()
                return int(row[0]) if row else 0
        finally:
            conn.close()

    def fetch_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        where = ["p.body IS NOT NULL", "p.body <> ''"]
        params: List[Any] = []
        if self.cfg.only_missing:
            where.append(
                "NOT EXISTS (SELECT 1 FROM post_enrichments pe "
                "WHERE pe.post_id::text = p.post_id::text AND pe.model_id = %s)"
            )
            params.append(self.model_id)
        q = (
            f"SELECT p.post_id, p.body, created_at, retrieved_at FROM posts p "
            f"WHERE {' AND '.join(where)} ORDER BY p.id OFFSET %s LIMIT %s"
        )
        params.extend([offset, limit])
        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(q, params)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            conn.close()

    def process_batch(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for r in rows:
            body = r.get("body") or ""
            out.append(
                {
                    "created_at": r.get("created_at"),
                    "post_id": r["post_id"],
                    "model_id": self.model_id,
                    "body": {"word_count": len(body.split()), "char_count": len(body)},
                }
            )
        return out

    def save_results(self, results: List[Dict[str, Any]]) -> None:
        if not results:
            return
        if self.cfg.do_save_to_db:
            objs = [
                PostEnrichments(
                    created_at=r["created_at"],
                    retrieved_at=self.applied_datetime,
                    post_id=r["post_id"],
                    model_id=r["model_id"],
                    body=r["body"],
                )
                for r in results
            ]
            self.db.insert_with_fallbacks(objs)
        else:
            output_base = Path(self.cfg.output_dir)
            output_base.mkdir(parents=True, exist_ok=True)
            outp = output_base / f"{self.model_id}.jsonl"
            with outp.open("a", encoding="utf-8") as f:
                for r in results:
                    f.write(
                        json.dumps(
                            {"post_id": r["post_id"], "model_id": r["model_id"], "body": r["body"]},
                            default=str,
                        )
                        + "\n"
                    )
```

Save this as `word_count_enricher.py`. The `@register(...)` decorator runs at import time, so importing this module is enough to make `"word_count"` available to `run_enricher`.

## Step 6: Register and Run

```python
from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
import word_count_enricher  # noqa: F401 -- import triggers @register

db = StandardDB(db_name="my_local_db")
run_enricher("word_count", db=db)
```

`run_enricher` also accepts the class directly (`run_enricher(word_count_enricher.WordCountEnricher, db=db)`), which works even for an enricher you haven't registered.

Because `WordCountConfig` inherits from `EnricherRunConfig`, the privacy layer and `preprocessors` work immediately, with no extra code in `WordCountEnricher` itself:

```python
run_enricher(
    "word_count",
    db=db,
    config=WordCountConfig(
        privacy_fields=["body"],
        pepper=b"...",
    ),
)
```

## Step 7: Test It

`fetch_batch`/`total_count`/`save_results` all just need a `MagicMock` database; `process_batch` needs nothing at all, since it's pure logic on plain dicts:

```python
from unittest.mock import MagicMock
from word_count_enricher import WordCountConfig, WordCountEnricher


def test_process_batch_counts_words_and_chars():
    db = MagicMock()
    e = WordCountEnricher(db, config=WordCountConfig())
    rows = [{"post_id": "p1", "body": "hello world", "created_at": None}]
    results = e.process_batch(rows)
    assert results == [
        {
            "created_at": None,
            "post_id": "p1",
            "model_id": "word_count",
            "body": {"word_count": 2, "char_count": 11},
        }
    ]


def test_model_id_includes_postfix():
    db = MagicMock()
    e = WordCountEnricher(db, config=WordCountConfig(model_id_postfix="v1"))
    assert e.model_id == "word_count_v1"
```

## Bonus: Enriching Accounts Instead of Posts

Everything above targets `posts`. To write an account-level enricher instead:

- Pass `target="accounts"` to `@register(...)`.
- Read from the `accounts` table instead of `posts` in `fetch_batch`/`total_count`.
- Use `AccountEnrichments` (with `account_id` instead of `post_id`) in `save_results`.

`bot_detection.py` (`BotDetectionEnricher`) is a real, complete example of an account-level enricher if you want to see the full pattern.
