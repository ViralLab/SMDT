from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import re
import math


from smdt.enrichers import BaseEnricher, register
from smdt.store.models import PostEnrichments
from smdt.store.standard_db import StandardDB

try:
    import torch
except ImportError as e:
    raise ImportError(
        "The 'torch' package is required. " "Install with 'pip install torch'."
    ) from e


# ----------------------- Config -----------------------
@dataclass
class DetoxifyConfig:
    # Required-ish
    model_name: str = (
        "multilingual"  # one of: original, unbiased, multilingual, original-small, unbiased-small
    )

    # Inference knobs
    model_batch_size: int = 8  # per-forward-pass size
    max_seq_len: int = 256  # tokenizer truncation
    device: Optional[str] = None  # "cuda" | "cpu" | None(auto)

    # Runner/selector knobs
    db_batch_size: int = 1000  # DB page size
    only_missing: bool = True  # process only posts missing our model_id

    # Persistence
    do_save_to_db: bool = True  # False → write to JSONL
    output_dir: Optional[str] = None  # required if do_save_to_db=False

    # Optional: local HF id / path overrides (advanced)
    hf_model_id: Optional[str] = None  # e.g. "unitary/toxic-bert"
    hf_tokenizer_id: Optional[str] = None  # tokenizer override

    def __post_init__(self) -> None:
        if self.model_batch_size <= 0:
            raise ValueError("model_batch_size must be > 0")
        if self.db_batch_size <= 0:
            raise ValueError("db_batch_size must be > 0")
        if not self.do_save_to_db:
            if not self.output_dir:
                raise ValueError("output_dir is required when do_save_to_db=False")
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)

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
    "detoxify_toxicity",
    target="posts",
    description="Multilingual toxicity scoring (Detoxify) via Transformers",
    requires=["torch", "transformers"],  # soft dep check
)
class DetoxifyToxicityEnricher(BaseEnricher):
    """
    Scores post text with a Detoxify variant and writes to post_enrichments.
    - MODEL_ID format: "toxicity_<model_name>"
    - JSONB payload: {"scores": {label: prob, ...}, "vendor": "detoxify", "model_name": "..."}
    """

    TARGET = "posts"

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
        super().__init__(db, config=config)

        if isinstance(config, DetoxifyConfig):
            self.cfg = config
        else:
            self.cfg = DetoxifyConfig(**(config or {}))

        self.device = (
            self.cfg.device
            if self.cfg.device is not None
            else ("cuda" if torch.cuda.is_available() else "cpu")
        )

        self.MODEL_ID = f"toxicity_{self.cfg.model_name}"
        self.applied_datetime = datetime.now(timezone.utc)

        self.model = None
        self.tokenizer = None
        self.class_names: List[str] = self._CLASS_NAMES_DEFAULT

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

        self.tokenizer = AutoTokenizer.from_pretrained(tok_id, use_fast=True)
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
    def total_count(self) -> Optional[int]:
        q = """
            SELECT COUNT(*)
            FROM posts p
            WHERE p.body IS NOT NULL
              AND p.body <> ''
              {missing_clause}
        """
        missing_clause = (
            "AND NOT EXISTS (SELECT 1 FROM post_enrichments pe WHERE pe.post_id = p.id AND pe.model_id = %s)"
            if self.cfg.only_missing
            else ""
        )
        q = q.format(missing_clause=missing_clause)
        conn = self.db.connect(self.db.db_name)
        try:
            with conn.cursor() as cur:
                if self.cfg.only_missing:
                    cur.execute(q, (self.MODEL_ID,))
                else:
                    cur.execute(q)
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
              {missing_clause}
            ORDER BY p.id
            OFFSET %s
            LIMIT %s
        """
        missing_clause = (
            "AND NOT EXISTS (SELECT 1 FROM post_enrichments pe WHERE pe.post_id = p.id AND pe.model_id = %s)"
            if self.cfg.only_missing
            else ""
        )
        q = q.format(missing_clause=missing_clause)

        conn = self.db.connect(self.db.db_name)
        try:
            with conn.cursor() as cur:
                if self.cfg.only_missing:
                    cur.execute(q, (self.MODEL_ID, offset, limit))
                else:
                    cur.execute(q, (offset, limit))
                cols = [d.name if hasattr(d, "name") else d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            conn.close()

    # --------------- inference ----------------
    _MENTION_RE = re.compile(r"@\w+", flags=re.UNICODE)

    def _clean(self, text: str) -> str:
        # Lightweight cleanup; mirror your previous logic
        return self._MENTION_RE.sub("@user", text or "")

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
        for row in probs:
            out.append(
                {name: float(score) for name, score in zip(self.class_names, row)}
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
        for r in rows:
            pid = r["post_id"]
            body = (r.get("body") or "").strip()
            if body:
                post_ids.append(pid)
                texts.append(self._clean(body))

        if not texts:
            return []

        # slice into model batches
        out: List[PostEnrichments] = []
        n = len(texts)
        mb = self.cfg.model_batch_size

        for i in range(0, n, mb):
            chunk_ids = post_ids[i : i + mb]
            chunk_txt = texts[i : i + mb]
            scores = self._predict_batch(chunk_txt)

            for pid, sc in zip(chunk_ids, scores):
                payload = {
                    "vendor": "detoxify",
                    "model_name": self.cfg.model_name,
                    "scores": sc,
                }
                out.append(
                    PostEnrichments(
                        created_at=self.applied_datetime,
                        retrieved_at=self.applied_datetime,
                        post_id=pid,
                        model_id=self.MODEL_ID,
                        content=payload,
                    )
                )
        return out

    def model_batch_size(self) -> int:
        # DB page size hint for runner
        return self.cfg.batch_size

    # --------------- persistence ----------------
    def save_results(self, results: List[PostEnrichments]) -> None:
        if not results:
            return
        if self.cfg.do_save_to_db:
            self.db.insert_with_fallbacks(results)  # COPY → multi-values → row-by-row
            print(f"[INFO] Saved {len(results)} rows to post_enrichments.")
        else:
            outp = Path(self.cfg.output_dir or ".") / f"{self.MODEL_ID}.jsonl"
            outp.parent.mkdir(parents=True, exist_ok=True)
            with outp.open("a", encoding="utf-8") as f:
                for r in results:
                    rec = {
                        "created_at": r.created_at.isoformat(),
                        "retrieved_at": (
                            r.retrieved_at.isoformat() if r.retrieved_at else None
                        ),
                        "post_id": r.post_id,
                        "model_id": r.model_id,
                        "content": r.content,  # {"vendor","model_name","scores":{...}}
                    }
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            print(f"[INFO] Wrote {len(results)} rows to {outp}")
