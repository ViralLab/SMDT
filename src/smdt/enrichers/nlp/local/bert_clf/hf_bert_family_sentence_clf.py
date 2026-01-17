from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import re
import math
from tqdm.auto import trange

from smdt.enrichers import BaseEnricher, register
from smdt.store.models import PostEnrichments
from smdt.store.standard_db import StandardDB

try:
    import torch
    import torch.nn.functional as F
except ImportError as e:
    raise ImportError(
        "The 'torch' package is required. Install with 'pip install torch'."
    ) from e


# ----------------------- Config -----------------------
@dataclass
class BERTSentenceClfConfig:
    # Inference knobs
    model_batch_size: int = 8
    max_seq_len: int = 256
    device: Optional[str] = None

    # Runner/selector knobs
    only_missing: bool = True

    # Persistence
    do_save_to_db: bool = True
    output_dir: Optional[str] = None

    # HF Model overrides
    hf_model_id: Optional[str] = None
    hf_tokenizer_id: Optional[str] = None
    model_name: str = "generic_classifier"
    is_multilabel: bool = False

    reset_cache: bool = False
    cache_dir: Optional[str] = None

    def __post_init__(self) -> None:
        if self.model_batch_size <= 0:
            raise ValueError("model_batch_size must be > 0")
        if not self.do_save_to_db:
            if not self.output_dir:
                raise ValueError("output_dir is required when do_save_to_db=False")
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)


# ----------------------- Enricher -----------------------


@register(
    "sentence_clf",
    target="posts",
    description="A generic sentence-level classification model (Binary or Multi-class).",
    requires=["torch", "transformers"],
)
class BERTSentenceClfEnricher(BaseEnricher):
    TARGET = "posts"

    def __init__(self, db: StandardDB, *, config: Optional[Dict[str, Any]] = None):
        super().__init__(db, config=config)

        if isinstance(config, BERTSentenceClfConfig):
            self.cfg = config
        else:
            self.cfg = BERTSentenceClfConfig(**(config or {}))
        self.device = (
            self.cfg.device
            if self.cfg.device is not None
            else ("cuda" if torch.cuda.is_available() else "cpu")
        )

        self.MODEL_ID = self.cfg.hf_model_id
        self.applied_datetime = datetime.now(timezone.utc)

        self.model = None
        self.tokenizer = None
        self.class_names: List[str] = []

        # load the cached IDs
        if self.cfg.reset_cache:
            self.cached_ids = set()
            self.reset_cache()
        else:
            self.cached_ids = set(self.load_cached_output_ids_from_file())
            self.setup_cache_table()

    def load_model(self) -> None:
        """
        Loads classifier and tokenizer. Automatically detects class labels
        from model config and handles multi-GPU setups.
        """
        try:
            from transformers import (
                AutoConfig,
                AutoModelForSequenceClassification,
                AutoTokenizer,
            )
        except ImportError as e:
            raise ImportError("The 'transformers' package is required.") from e

        if not self.cfg.hf_model_id:
            raise ValueError("hf_model_id must be provided in config.")

        # Load config to extract labels
        config = AutoConfig.from_pretrained(self.cfg.hf_model_id)

        # Determine class names: use id2label if available, else generic names
        id2label = getattr(config, "id2label", None)
        if id2label:
            # Sort by keys to ensure correct order
            self.class_names = [id2label[i] for i in sorted(id2label.keys())]
        else:
            num_labels = getattr(config, "num_labels", 1)
            self.class_names = [f"label_{i}" for i in range(num_labels)]

        tok_id = self.cfg.hf_tokenizer_id or self.cfg.hf_model_id
        self.tokenizer = AutoTokenizer.from_pretrained(tok_id, use_fast=True)
        model = AutoModelForSequenceClassification.from_pretrained(self.cfg.hf_model_id)

        if (
            torch.cuda.is_available()
            and torch.cuda.device_count() > 1
            and self.device == "cuda"
        ):
            model = torch.nn.DataParallel(model)

        self.model = model.to(self.device)
        self.model.eval()

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

    # --------------- inference ----------------
    _MENTION_RE = re.compile(r"@\w+", flags=re.UNICODE)

    def _clean(self, text: str) -> str:
        return self._MENTION_RE.sub("@user", text or "")

    def _predict_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        if self.model is None or self.tokenizer is None:
            self.load_model()

        enc = self.tokenizer(
            texts,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=self.cfg.max_seq_len,
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**enc)
            logits = outputs.logits if hasattr(outputs, "logits") else outputs[0]

            # if multilabel -> Sigmoid
            # If num_labels > 1 -> Softmax (probabilities sum to 1 across labels)
            # If num_labels == 1 -> Sigmoid (independent probability)
            num_labels = logits.shape[-1]
            if self.cfg.is_multilabel:
                probs = torch.sigmoid(logits)
            elif num_labels > 1:
                probs = F.softmax(logits, dim=-1)
            else:
                probs = torch.sigmoid(logits)

            probs_np = probs.cpu().numpy()
            logits_np = logits.cpu().numpy()

        results: List[Dict[str, Any]] = []
        for i in range(len(texts)):
            row_probs = probs_np[i]
            row_logits = logits_np[i]

            results.append(
                {
                    "scores": {
                        name: float(p) for name, p in zip(self.class_names, row_probs)
                    },
                    "logits": {
                        name: float(l) for name, l in zip(self.class_names, row_logits)
                    },
                }
            )
        return results

    def process_batch(self, rows: List[Dict[str, Any]]) -> List[PostEnrichments]:
        if not rows:
            return []

        post_ids = [r["post_id"] for r in rows if (r.get("body") or "").strip()]
        texts = [self._clean(r["body"]) for r in rows if (r.get("body") or "").strip()]
        post_created_ats = [
            r.get("created_at") or self.applied_datetime
            for r in rows
            if (r.get("body") or "").strip()
        ]
        if not texts:
            return []

        out: List[PostEnrichments] = []
        mb = self.cfg.model_batch_size

        for i in trange(0, len(texts), mb):
            chunk_ids = post_ids[i : i + mb]
            chunk_txt = texts[i : i + mb]
            chunk_created_ats = post_created_ats[i : i + mb]
            batch_results = self._predict_batch(chunk_txt)

            for pid, created_at, res in zip(
                chunk_ids, chunk_created_ats, batch_results
            ):
                payload = {
                    "vendor": "huggingface",
                    "model_name": self.cfg.hf_model_id,
                    "scores": res["scores"],
                    "logits": res["logits"],
                }
                out.append(
                    {
                        "created_at": created_at,
                        "retrieved_at": self.applied_datetime,
                        "post_id": pid,
                        "model_id": self.MODEL_ID,
                        "body": payload,
                    }
                )
        return out

    # --------------- persistence ----------------
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
