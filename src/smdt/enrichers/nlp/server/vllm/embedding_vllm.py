from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import os

from smdt.enrichers.base import BaseEnricher
from smdt.enrichers.registry import register

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

    reset_cache: bool = False
    cache_dir: Optional[str] = None  # optional

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
        self.MODEL_ID = f"{self._ENRICHER_ID}_{self.cfg.model_id_postfix}"
        self.applied_datetime = datetime.now(timezone.utc)
        self.client = None  # set in load_model()

        # load the cached IDs
        if self.cfg.reset_cache:
            self.cached_ids = set()
            self.reset_cache()
        else:
            self.cached_ids = set(self.load_cached_output_ids_from_file())
            self.setup_cache_table()

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

    def total_count(self) -> int:
        where_clauses = ["p.body IS NOT NULL", "p.body <> ''"]
        params = []

        if self.cfg.only_missing:
            where_clauses.append(
                "NOT EXISTS (SELECT 1 FROM post_enrichments pe WHERE pe.post_id::text = p.post_id::text AND pe.model_id = %s)"
            )
            params.append(self.MODEL_ID)

        # Use the real table for exclusion
        if not self.cfg.reset_cache and self.cached_ids:
            where_clauses.append(
                f"NOT EXISTS (SELECT 1 FROM {self.generated_temp_table_name} c WHERE c.post_id = p.post_id::text)"
            )

        q = f"SELECT COUNT(*) FROM posts p WHERE {' AND '.join(where_clauses)}"

        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(q, params)
                row = cur.fetchone()
                return int(row[0]) if row else 0
        finally:
            conn.close()

    def fetch_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        where_clauses = ["p.body IS NOT NULL", "p.body <> ''"]
        params = []

        if self.cfg.only_missing:
            where_clauses.append(
                "NOT EXISTS (SELECT 1 FROM post_enrichments pe WHERE pe.post_id::text = p.post_id::text AND pe.model_id = %s)"
            )
            params.append(self.MODEL_ID)

        if not self.cfg.reset_cache and self.cached_ids:
            where_clauses.append(
                f"NOT EXISTS (SELECT 1 FROM {self.generated_temp_table_name} c WHERE c.post_id = p.post_id::text)"
            )

        q = f"SELECT p.post_id, p.body, created_at, retrieved_at FROM posts p WHERE {' AND '.join(where_clauses)} ORDER BY p.id OFFSET %s LIMIT %s"
        params.extend([offset, limit])

        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(q, params)
                cols = [d[0] for d in cur.description]
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
        created_ats = [r.get("created_at") for r in rows]

        resp = self.client.embeddings.create(
            model=self.cfg.embedding_model_id,
            input=texts,
        )

        out: List[PostEnrichments] = []
        for pid, created_at, item in zip(post_ids, created_ats, resp.data):
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
                {
                    "created_at": created_at,
                    "retrieved_at": self.applied_datetime,
                    "post_id": pid,
                    "model_id": self.MODEL_ID,
                    "content": payload,  # JSONB
                }
            )
        return out

    def model_batch_size(self) -> int:
        return self.cfg.batch_size

    # ---- persistence ----
    def save_results(self, results: List[Dict[str, Any]]) -> None:
        if not results:
            return
        if self.cfg.do_save_to_db:
            try:
                results = [
                    PostEnrichments(
                        created_at=r["created_at"],
                        retrieved_at=self.applied_datetime,
                        post_id=r["post_id"],
                        model_id=r["model_id"],
                        body=r["content"],  # JSONB
                    )
                    for r in results
                ]

                self.db.insert_with_fallbacks(results)

                self.write_current_cache_ids_to_file([r.post_id for r in results])
            except Exception as e:
                print(f"[ERROR] Failed to insert results: {e}")
            print(f"[INFO] Saved {len(results)} rows to post_enrichments.")
        else:
            # Grouping results by the created_at day and storing in separate files
            output_base = Path(self.cfg.output_dir or ".")
            output_base.mkdir(parents=True, exist_ok=True)

            # Use a dict to keep track of open files to avoid repeated opening/closing
            open_files = {}

            try:
                for r in results:
                    # 1. Extract the date for the filename (Default to 'unknown' if missing)
                    c_at = r.get("created_at")
                    date_str = c_at.strftime("%Y-%m-%d") if c_at else "unknown"

                    # 2. Construct filename: e.g., model_name_2023-10-27.jsonl
                    safe_model_id = self.MODEL_ID.replace("/", "_")
                    outp = output_base / f"{safe_model_id}_{date_str}.jsonl"

                    # 3. Get or create the file handle
                    if date_str not in open_files:
                        open_files[date_str] = outp.open("a", encoding="utf-8")

                    # 4. Prepare and write the record
                    rec = {
                        "created_at": c_at.isoformat() if c_at else None,
                        "retrieved_at": (
                            r["retrieved_at"].isoformat()
                            if r.get("retrieved_at")
                            else self.applied_datetime.isoformat()
                        ),
                        "post_id": r["post_id"],
                        "model_id": r["model_id"],
                        "body": r["body"],
                    }
                    open_files[date_str].write(
                        json.dumps(rec, ensure_ascii=False) + "\n"
                    )

                print(
                    f"[INFO] Processed {len(results)} rows into {len(open_files)} daily files."
                )

            finally:
                # Always close all open file handles
                for f in open_files.values():
                    f.close()
