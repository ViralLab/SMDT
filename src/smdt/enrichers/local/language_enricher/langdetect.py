from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import re, json
from pathlib import Path

from typing import Any, Dict, List, Optional
from smdt.enrichers import BaseEnricher, register
from smdt.store.models import PostEnrichments
from smdt.store.standard_db import StandardDB


@dataclass
class LanguageDetectionConfig:
    # required
    model_id_postfix: str

    do_save_to_db: bool

    # destination
    output_dir: Optional[str] = None  # required if do_save_to_db == False

    def __post_init__(self) -> None:
        self.model_id_postfix = (self.model_id_postfix or "").strip()

        if not self.model_id_postfix:
            raise ValueError("model_id_postfix is required.")

        if self.do_save_to_db is None:
            raise ValueError("do_save_to_db is required and must be a boolean.")

        if not isinstance(self.do_save_to_db, bool):
            raise ValueError("do_save_to_db must be a boolean.")

        if self.do_save_to_db == False:
            if not self.output_dir:
                raise ValueError("output_dir is required when do_save_to_db=False.")
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)


@register(
    "langdetect",  # registry name
    target="posts",  # we enrich posts
    description="Language detection via langdetect.detect_langs",
    requires=["langdetect"],  # soft dependency check
)
class LanguageDetectionEnricher(BaseEnricher):
    """
    Detect languages for post bodies and store:
      table: post_enrichments
      uniqueness: (post_id, model_id)
      payload example: {"langs": [{"lang":"en","prob":0.999}], "len": 123}
    """

    TARGET = "posts"
    ENRICHER_ID = "langdetect"  # becomes post_enrichments.model_id

    def __init__(self, db: StandardDB, *, config: Optional[Dict[str, Any]] = None):
        super().__init__(db, config=config)
        # regexes ported from your previous implementation
        self._emoji_re = re.compile(
            "["  # note: python handles these unicode ranges
            "\U0001f600-\U0001f64f"  # Emoticons
            "\U0001f300-\U0001f5ff"  # Symbols & pictographs
            "\U0001f680-\U0001f6ff"  # Transport & map symbols
            "\U0001f1e0-\U0001f1ff"  # Flags
            "\u2702-\u27b0"
            "\u24c2-\U0001f251"
            "]+",
            flags=re.UNICODE,
        )
        self._mention_re = re.compile(r"@\w+")
        self._emoji_tag_re = re.compile(r"<emoji:\s*\w+>")
        self.applied_datetime = datetime.now(timezone.utc)

    # ---------------- lifecycle / model ----------------
    def load_model(self) -> None:
        # lazy import to honor `requires`
        from langdetect import detect_langs

        self._detect_langs = detect_langs  # keep a bound callable

    # ---------------- DB IO ----------------
    def total_count(self) -> Optional[int]:
        # How many posts do NOT yet have an entry for our MODEL_ID?
        q = """
            SELECT COUNT(*)
            FROM posts p
            WHERE p.body IS NOT NULL
              AND p.body <> ''
              AND NOT EXISTS (
                    SELECT 1
                    FROM post_enrichments pe
                    WHERE pe.post_id = p.post_id
                      AND pe.model_id = %s
                )
        """
        with self.db.connect(self.db.db_name) as conn, conn.cursor() as cur:
            cur.execute(q, (self.ENRICHER_ID,))
            return cur.fetchone()[0]

    def fetch_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        q = """
            SELECT p.id, p.post_id, p.account_id, p.body, p.created_at, p.retrieved_at
            FROM posts p
            WHERE p.body IS NOT NULL
              AND p.body <> ''
              AND NOT EXISTS (
                    SELECT 1
                    FROM post_enrichments pe
                    WHERE pe.post_id = p.post_id
                      AND pe.model_id = %s
                )
            ORDER BY p.id
            OFFSET %s LIMIT %s
        """
        with self.db.connect(self.db.db_name) as conn, conn.cursor() as cur:
            cur.execute(q, (self.ENRICHER_ID, offset, limit))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    # ---------------- core processing ----------------
    def process_batch(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows:
            post_id = r["post_id"]
            raw_text = r.get("body") or ""
            text = self._preprocess(raw_text)

            # skip extremely short strings
            if len(text) < 4:
                payload = {"langs": None, "len": len(text)}
            else:
                try:
                    langs = self._detect_langs(text)  # returns list of LangProbability
                    payload = {
                        "len": len(text),
                        "langs": [
                            {"lang": lp.lang, "prob": float(lp.prob)} for lp in langs
                        ],
                    }
                except Exception:
                    payload = {"langs": None, "len": len(text)}

            out.append(
                {
                    "post_id": post_id,
                    "model_id": self.ENRICHER_ID,
                    "payload": payload,
                }
            )
        return out

    def _preprocess(self, text: str) -> str:
        # match your previous cleaner
        text = self._mention_re.sub("", text)
        text = self._emoji_re.sub("", text)
        text = self._emoji_tag_re.sub("", text)
        return text.strip()

    def save_results(self, results: List[Dict[str, Any]]) -> None:
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
