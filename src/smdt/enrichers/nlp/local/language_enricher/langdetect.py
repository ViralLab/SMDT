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

    reset_cache: bool = False
    cache_dir: Optional[str] = None  # optional

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

        # load the cached IDs
        if self.cfg.reset_cache:
            self.cached_ids = set()
            self.reset_cache()
        else:
            self.cached_ids = set(self.load_cached_output_ids_from_file())

    # ---------------- lifecycle / model ----------------
    def load_model(self) -> None:
        # lazy import to honor `requires`
        from langdetect import detect_langs

        self._detect_langs = detect_langs  # keep a bound callable

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

    # ---------------- core processing ----------------
    def process_batch(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows:
            post_id = r["post_id"]
            raw_text = r.get("body") or ""
            created_at = r.get("created_at")
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
                    "created_at": created_at,
                    "post_id": post_id,
                    "model_id": self.ENRICHER_ID,
                    "body": payload,
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
                        created_at=r["created_at"],
                        retrieved_at=self.applied_datetime,
                        post_id=r["post_id"],
                        model_id=r["model_id"],
                        body=r["body"],  # JSONB
                    )
                    for r in results
                ]

                self.db.insert_with_fallbacks(results)
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
