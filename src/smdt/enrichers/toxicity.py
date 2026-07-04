from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import re
import math


from smdt.enrichers.base import BaseEnricher, EnricherRunConfig, RowPreprocessor
from smdt.enrichers.registry import register

from smdt.store.models import PostEnrichments
from smdt.store.standard_db import StandardDB


try:
    import torch
except ImportError as e:
    raise ImportError(
        "The 'torch' package is required. " "Install with 'pip install torch'."
    ) from e


_MENTION_RE = re.compile(r"@\w+", flags=re.UNICODE)


def default_mention_preprocessor(row: Dict[str, Any]) -> Dict[str, Any]:
    """Collapse any @mention-shaped token in `body` to the generic `@user`.

    Detoxify-style checkpoints were pretrained expecting a single generic
    mention token rather than distinct handles. This is the default entry in
    `ToxicityConfig.preprocessors` -- pass your own `preprocessors` list to
    replace it (e.g. to keep the privacy layer's `@u_<hash>` tokens intact).
    """
    body = row.get("body")
    if not body:
        return row
    row = dict(row)
    row["body"] = _MENTION_RE.sub("@user", body)
    return row


# ----------------------- Config -----------------------
@dataclass
class ToxicityConfig(EnricherRunConfig):
    """Configuration for ToxicityEnricher.

    Attributes:
        model_name: Detoxify variant. One of: ``original``, ``unbiased``,
            ``multilingual``, ``original-small``, ``unbiased-small``.
        model_batch_size: Number of texts per forward pass.
        max_seq_len: Tokenizer truncation length.
        device: Inference device — ``"cuda"``, ``"cpu"``, or ``None`` (auto-detect).
        hf_model_id: Override the Hugging Face model checkpoint (advanced).
        hf_tokenizer_id: Override the Hugging Face tokenizer (advanced).
    """
    model_name: str = "multilingual"
    model_batch_size: int = 8
    max_seq_len: int = 256
    device: Optional[str] = None
    hf_model_id: Optional[str] = None
    hf_tokenizer_id: Optional[str] = None
    preprocessors: List[RowPreprocessor] = field(
        default_factory=lambda: [default_mention_preprocessor]
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.model_batch_size <= 0:
            raise ValueError("model_batch_size must be > 0")

        if self.model_name not in {
            "original",
            "unbiased",
            "multilingual",
            "original-small",
            "unbiased-small",
        }:
            raise ValueError(
                f"model_name '{self.model_name}' is not one of the supported Detoxify variants."
            )


# ----------------------- Enricher -----------------------


@register(
    "toxicity",
    target="posts",
    description="Multilingual toxicity scoring (Detoxify) via Transformers",
    requires=["torch", "transformers"],  # soft dep check
)
class ToxicityEnricher(BaseEnricher):
    """Scores post text for toxicity using a Detoxify transformer checkpoint.

    - ``model_id`` format: ``"toxicity_<model_name>"``
    - JSONB payload: ``{"scores": {label: float, ...}, "logits": {label: float, ...}, "vendor": "detoxify", "model_name": str}``
    """

    # Canonical Detoxify class sets (after renaming severe_toxic->severe_toxicity, identity_hate->identity_attack)
    _CLASS_NAMES_DEFAULT = [
        "toxicity",
        "severe_toxicity",
        "obscene",
        "threat",
        "insult",
        "identity_attack",
        "sexual_explicit",
    ]

    def __init__(self, db: StandardDB, *, config: Optional[Dict[str, Any]] = None):
        super().__init__(db)
        self.cfg = self._coerce_config(config, ToxicityConfig)

        self.device = (
            self.cfg.device
            if self.cfg.device is not None
            else ("cuda" if torch.cuda.is_available() else "cpu")
        )

        self.model_id = self._make_model_id(self.cfg.model_name)
        self.applied_datetime = datetime.now(timezone.utc)

        self.model = None
        self.tokenizer = None
        self.class_names: List[str] = self._CLASS_NAMES_DEFAULT

        self._init_cache()

    # --------------- lifecycle ----------------
    def load_model(self) -> None:
        """
        Loads a transformers classifier & tokenizer compatible with Detoxify checkpoints.
        Tries to auto-derive class names; falls back to defaults if not available.
        """

        try:
            from transformers import (
                AutoConfig,
                AutoModelForSequenceClassification,
                AutoTokenizer,
            )
        except ImportError as e:
            raise ImportError(
                "The 'transformers' package is required. "
                "Install with 'pip install transformers'."
            ) from e

        # Choose HF model ids. You can map your preferred checkpoints here.
        # These are common choices compatible with Detoxify label space.
        model_map = {
            "original": "unitary/toxic-bert",
            "unbiased": "unitary/unbiased-toxic-roberta",
            "multilingual": "unitary/multilingual-toxic-xlm-roberta",
            "original-small": "unitary/toxic-bert",  # fallback to nearest
            "unbiased-small": "unitary/unbiased-toxic-roberta",
        }

        model_id = self.cfg.hf_model_id or model_map.get(self.cfg.model_name)
        tok_id = self.cfg.hf_tokenizer_id or model_id
        if not model_id:
            raise ValueError(
                f"Unknown model_name '{self.cfg.model_name}' and no hf_model_id provided"
            )

        config = AutoConfig.from_pretrained(
            model_id, num_labels=len(self._CLASS_NAMES_DEFAULT)
        )
        # try to extract label names if present
        id2label = getattr(config, "id2label", None)
        if id2label:
            # Normalize label names
            labels = [id2label[i] for i in sorted(id2label)]
            rename_map = {
                "toxic": "toxicity",
                "identity_hate": "identity_attack",
                "severe_toxic": "severe_toxicity",
            }
            self.class_names = [rename_map.get(c.lower(), c).lower() for c in labels]
        else:
            self.class_names = self._CLASS_NAMES_DEFAULT

        self.tokenizer = AutoTokenizer.from_pretrained(tok_id, use_fast=False)
        model = AutoModelForSequenceClassification.from_pretrained(model_id)

        # enable multi-GPU if available
        if (
            torch.cuda.is_available()
            and torch.cuda.device_count() > 1
            and self.device == "cuda"
        ):
            model = torch.nn.DataParallel(model)

        self.model = model.to(self.device)
        self.model.eval()

    # --------------- selection ----------------
    def total_count(self) -> int:
        where_clauses = ["p.body IS NOT NULL", "p.body <> ''"]
        params = []

        if self.cfg.only_missing:
            where_clauses.append(
                "NOT EXISTS (SELECT 1 FROM post_enrichments pe WHERE pe.post_id::text = p.post_id::text AND pe.model_id = %s)"
            )
            params.append(self.model_id)

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
            params.append(self.model_id)

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
    def _predict_batch(self, texts: List[str]) -> List[Dict[str, float]]:
        if self.model is None or self.tokenizer is None:
            self.load_model()

        from torch.nn.functional import sigmoid

        # tokenize
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
            probs = sigmoid(logits).detach().cpu()

        out: List[Dict[str, float]] = []
        for logit, prob in zip(logits, probs):
            out.append(
                {
                    "scores": {
                        name: float(score)
                        for name, score in zip(self.class_names, prob)
                    },
                    "logits": {
                        name: float(l) for name, l in zip(self.class_names, logit)
                    },
                }
            )
        return out

    def process_batch(self, rows: List[Dict[str, Any]]) -> List[PostEnrichments]:
        """
        Runs toxicity on rows=[{post_id, body}, ...] in internal model batches
        and returns PostEnrichments objects.
        """
        if not rows:
            return []

        # collect & clean
        post_ids: List[int] = []
        texts: List[str] = []
        created_ats: List[Optional[datetime]] = []
        for r in rows:
            pid = r["post_id"]
            body = (r.get("body") or "").strip()
            if body:
                post_ids.append(pid)
                texts.append(body)
                created_ats.append(r.get("created_at"))

        if not texts:
            return []

        # slice into model batches
        out: List[PostEnrichments] = []
        n = len(texts)
        mb = self.cfg.model_batch_size

        for i in range(0, n, mb):
            chunk_ids = post_ids[i : i + mb]
            chunk_txt = texts[i : i + mb]
            chunk_created_ats = created_ats[i : i + mb]
            scores = self._predict_batch(chunk_txt)

            for pid, created_at, sc in zip(chunk_ids, chunk_created_ats, scores):
                payload = {
                    "vendor": "detoxify",
                    "model_name": self.cfg.model_name,
                    "scores": sc["scores"],
                    "logits": sc["logits"],
                }
                out.append(
                    {
                        "created_at": created_at,
                        "retrieved_at": self.applied_datetime,
                        "post_id": pid,
                        "model_id": self.model_id,
                        "body": payload,
                    }
                )
        return out

    # --------------- persistence ----------------
    def save_results(self, results: List[Dict[str, Any]]) -> None:
        if not results:
            return
        if self.cfg.do_save_to_db:
            results = [
                PostEnrichments(
                    created_at=r["created_at"],
                    retrieved_at=self.applied_datetime,
                    post_id=r["post_id"],
                    model_id=r["model_id"],
                    body=r["body"],  # JSONB
                )
                for r in results
            ]
            self.db.insert_with_fallbacks(results)  # COPY → multi-values → row-by-row
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
                    safe_model_id = self.model_id.replace("/", "_")
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
