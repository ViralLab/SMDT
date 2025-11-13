from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import os

from smdt.enrichers import BaseEnricher, register
from smdt.store.models import PostEnrichments
from smdt.store.standard_db import StandardDB


@dataclass
class VLLMEmbeddingConfig:
    # required
    embedding_model_id: str
    model_id_postfix: str
    base_url: str
    do_save_to_db: bool

    # destination
    output_dir: Optional[str] = None  # required if do_save_to_db == False

    # optional
    api_key: str = ""
    batch_size: int = 1024  # texts per embeddings API call

    def __post_init__(self) -> None:
        self.model_id_postfix = (self.model_id_postfix or "").strip()
        self.embedding_model_id = (self.embedding_model_id or "").strip()
        self.base_url = (self.base_url or "").strip()
        self.api_key = (self.api_key or "").strip()

        if not self.model_id_postfix:
            raise ValueError("model_id_postfix is required.")
        if not self.embedding_model_id:
            raise ValueError("embedding_model_id is required.")
        if not self.base_url:
            raise ValueError("base_url is required.")
        if not isinstance(self.batch_size, int) or self.batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        if self.do_save_to_db is None:
            raise ValueError("do_save_to_db is required and must be a boolean.")

        if not isinstance(self.do_save_to_db, bool):
            raise ValueError("do_save_to_db must be a boolean.")

        if self.do_save_to_db == False:
            if not self.output_dir:
                raise ValueError("output_dir is required when do_save_to_db=False.")
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)


@register(
    "vllm_client",
    target="posts",
    description="VLLM embeddings via OpenAI-compatible client",
    requires=["openai"],
)
class VLLMClientEnricher(BaseEnricher):
    """
    Enricher that batches post bodies and requests embeddings from a vLLM-compatible
    OpenAI API server (using openai>=1 client with custom base_url).
    """

    TARGET = "posts"
    ENRICHER_ID = "vllm_embedding"  # we'll append a postfix from config

    def __init__(self, db: StandardDB, *, config: Optional[Dict[str, Any]] = None):
        super().__init__(db, config=config)

        if isinstance(config, VLLMEmbeddingConfig):
            self.cfg = config
        else:
            try:
                self.cfg = VLLMEmbeddingConfig(**(config or {}))
            except TypeError as e:
                raise ValueError(
                    f"Invalid config keys: {e}. "
                    "Expected: model_id_postfix, embedding_model_id, base_url, "
                    "api_key(optional), batch_size(optional), "
                    "do_save_to_db(optional), output_dir(if saving to file)."
                ) from e

        # model_id used in post_enrichments.model_id
        self.MODEL_ID = f"{self.ENRICHER_ID}_{self.cfg.model_id_postfix}"
        self.applied_datetime = datetime.now(timezone.utc)
        self.client = None  # set in load_model()

    # ---------------- lifecycle / model ----------------
    def load_model(self) -> None:
        try:
            from openai import OpenAI
        except ImportError as e:
            raise ImportError(
                "The 'openai' package is required. "
                "Install with 'pip install smdt[vllm_client]' or 'pip install openai'."
            ) from e

        self.client = OpenAI(base_url=self.cfg.base_url, api_key=self.cfg.api_key)

    # ---------------- DB selection ----------------
    def total_count(self) -> Optional[int]:
        q = """
            SELECT COUNT(*)
            FROM posts p
            WHERE p.body IS NOT NULL
              AND p.body <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM post_enrichments pe
                  WHERE pe.post_id = p.id AND pe.model_id = %s
              )
        """
        conn = self.db.connect(self.db.db_name)
        try:
            with conn.cursor() as cur:
                cur.execute(q, (self.MODEL_ID,))
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else 0
        finally:
            conn.close()

    def fetch_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        q = """
            SELECT p.id AS post_id, p.body AS body
            FROM posts p
            WHERE p.body IS NOT NULL
              AND p.body <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM post_enrichments pe
                  WHERE pe.post_id = p.id AND pe.model_id = %s
              )
            ORDER BY p.id
            OFFSET %s
            LIMIT %s
        """
        conn = self.db.connect(self.db.db_name)
        try:
            with conn.cursor() as cur:
                cur.execute(q, (self.MODEL_ID, offset, limit))
                cols = [d.name if hasattr(d, "name") else d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            conn.close()

    # ---------------- main inference step ----------------
    def process_batch(self, rows: List[Dict[str, Any]]) -> List[PostEnrichments]:
        if not rows:
            return []
        if self.client is None:
            self.load_model()

        post_ids = [r["post_id"] for r in rows]
        texts = [(r.get("body") or "") for r in rows]

        resp = self.client.embeddings.create(
            model=self.cfg.embedding_model_id,
            input=texts,
        )

        out: List[PostEnrichments] = []
        for pid, item in zip(post_ids, resp.data):
            vec = getattr(item, "embedding", None)
            if vec is None:
                continue
            payload = {
                "embedding": vec,  # list[float]
                "vendor": "vllm",
                "embedding_model_id": self.cfg.embedding_model_id,
                "dim": len(vec),
            }
            out.append(
                PostEnrichments(
                    created_at=self.applied_datetime,
                    retrieved_at=self.applied_datetime,
                    post_id=pid,
                    model_id=self.MODEL_ID,
                    content=payload,  # JSONB
                )
            )
        return out

    def model_batch_size(self) -> int:
        return self.cfg.batch_size

    def save_results(self, results: List[PostEnrichments]) -> None:
        if not results:
            return

        if self.cfg.do_save_to_db:
            try:
                # Uses your resilient COPY → bulk → row-by-row fallback
                results = [
                    PostEnrichments(
                        created_at=self.applied_datetime,
                        retrieved_at=self.applied_datetime,
                        post_id=r["post_id"],
                        model_id=r["model_id"],
                        content=r["content"],  # JSONB
                    )
                    for r in results
                ]
                self.db.insert_with_fallbacks(results)
            except Exception as e:
                print(f"[ERROR] Failed to insert results: {e}")
        else:
            # Write JSONL file under output_dir
            path = Path(self.cfg.output_dir) / f"{self.MODEL_ID}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                for r in results:
                    rec = {
                        "created_at": self.applied_datetime.isoformat(),
                        "retrieved_at": self.applied_datetime.isoformat(),
                        "post_id": r.post_id,
                        "model_id": r.model_id,
                        "content": r.content,
                    }
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
