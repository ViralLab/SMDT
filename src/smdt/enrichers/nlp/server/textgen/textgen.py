from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import asyncio
import json
import time

from smdt.enrichers import BaseEnricher, register
from smdt.store.models import PostEnrichments
from smdt.store.standard_db import StandardDB
from smdt.enrichers.server.prompt_adapters import (
    ChatMessage,
    GenParams,
    ProviderConfig,
    make_adapter,
)
from smdt.enrichers.server.prompt_template import PromptTemplate


@dataclass
class TextGenConfig:
    # --- Required first (dataclass rule) ---
    model_id_postfix: str  # becomes part of post_enrichments.model_id
    chat_model_id: str  # model name served by vLLM/OpenAI
    base_url: str  # OpenAI-compatible endpoint

    # --- Provider/adapters ---
    provider_kind: str = "openai"  # "openai", "anthropic", "hf-text", "ollama"
    provider_model: Optional[str] = None
    prompt_path: Optional[str] = None  # YAML/JSON file
    prompt_id: Optional[str] = None
    extra_vars: Optional[Dict[str, Any]] = None

    # --- Destination ---
    do_save_to_db: bool = True  # False → write to JSONL
    output_dir: Optional[str] = None  # required if do_save_to_db=False

    # --- Auth & request ---
    api_key: str = ""
    system_prompt: str = "You are a helpful assistant."
    user_template: str = "Summarize the following post in one sentence:\n\n{body}"
    temperature: float = 0.2
    max_tokens: int = 256
    top_p: float = 1.0

    # --- Runner / batching knobs ---
    batch_size: int = 32  # DB fetch page size
    max_input_chars: int = 8_000  # crude guard; avoids huge prompts
    requests_per_minute: int = 120  # client-side throttle (approximate)

    reset_cache: bool = False
    cache_dir: Optional[str] = None  # optional

    # internal
    _prompt: Optional[PromptTemplate] = None

    def __post_init__(self) -> None:
        if not self.provider_model:
            self.provider_model = self.chat_model_id  # back-compat
        self._prompt = (
            PromptTemplate.from_file(self.prompt_path, self.prompt_id)
            if self.prompt_path
            else None
        )

        self.model_id_postfix = (self.model_id_postfix or "").strip()
        self.chat_model_id = (self.chat_model_id or "").strip()
        self.base_url = (self.base_url or "").strip()
        self.api_key = (self.api_key or "").strip()

        if not self.model_id_postfix:
            raise ValueError("model_id_postfix is required.")
        if not self.chat_model_id:
            raise ValueError("chat_model_id is required.")
        if not self.base_url:
            raise ValueError("base_url is required.")
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be > 0.")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be > 0.")
        if self.requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be > 0.")
        if not self.do_save_to_db:
            if not self.output_dir:
                raise ValueError("output_dir is required when do_save_to_db=False.")
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        if not self.provider_kind in {
            "openai",
            "anthropic",
            "hf-text",
            "ollama",
        }:
            raise ValueError(
                f"Unsupported provider_kind '{self.provider_kind}'; "
                "must be one of 'openai', 'anthropic', 'hf-text', 'ollama'."
            )


@register(
    "textgen",
    target="posts",
    description="Text generation via provider adapters (OpenAI-compatible, Anthropic, HF, Ollama)",
    requires=[],  # no hard dependency on 'openai' here
)
class TextGenEnricher(BaseEnricher):
    TARGET = "posts"
    ENRICHER_ID_BASE = "textgen"

    def __init__(self, db: StandardDB, *, config: Optional[Dict[str, Any]] = None):
        super().__init__(db, config=config)

        # validate/normalize config
        if isinstance(config, TextGenConfig):
            self.cfg = config
        else:
            self.cfg = TextGenConfig(**(config or {}))

        self._gen_params = GenParams(
            temperature=self.cfg.temperature,
            max_tokens=self.cfg.max_tokens,
            top_p=self.cfg.top_p,
        )
        self._adapter = None  # lazy

        # model_id stored in post_enrichments
        self.ENRICHER_ID = f"{self.ENRICHER_ID_BASE}_{self.cfg.model_id_postfix}"
        self.applied_datetime = datetime.now(timezone.utc)

        # rate limiter
        self._min_interval = 60.0 / float(self.cfg.requests_per_minute)
        self._last_call_at = 0.0
        self._rl_lock: asyncio.Lock = asyncio.Lock()  # initialize here

        # concurrency: sensible default derived from RPM (tune if needed)
        self._concurrency = max(
            1, min(self.cfg.batch_size, self.cfg.requests_per_minute // 4 or 1)
        )

        # load the cached IDs
        if self.cfg.reset_cache:
            self.cached_ids = set()
            self.reset_cache()
        else:
            self.cached_ids = set(self.load_cached_output_ids_from_file())
            self.setup_cache_table()

    async def _ensure_adapter(self) -> None:
        if self._adapter is not None:
            return
        pcfg = ProviderConfig(
            kind=self.cfg.provider_kind,
            model=self.cfg.provider_model or self.cfg.chat_model_id,
            base_url=self.cfg.base_url,
            api_key=self.cfg.api_key,
            endpoint=self.cfg.base_url,  # used by hf-text if applicable
        )
        self._adapter = make_adapter(pcfg)

    # ---- lifecycle ----
    def load_model(self) -> None:
        # sync wrapper (not used directly by async path), kept for BaseEnricher contract
        pass

    # ---- selection ----
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

    # ---- rate limit/throttle ----
    async def _throttle(self) -> None:
        async with self._rl_lock:
            now = time.time()
            wait = self._min_interval - (now - self._last_call_at)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call_at = time.time()

    # ---- async one request ----
    async def _one_completion_async(self, body: str) -> Optional[dict]:
        await self._throttle()
        await self._ensure_adapter()

        snippet = (body or "")[: self.cfg.max_input_chars]

        if self.cfg._prompt:
            messages = self.cfg._prompt.render(
                body=snippet, **(self.cfg.extra_vars or {})
            )
            prompt_meta = {"prompt_source": "file", "prompt_id": self.cfg._prompt.id}
        else:
            messages = [
                ChatMessage(role="system", content=self.cfg.system_prompt),
                ChatMessage(
                    role="user", content=self.cfg.user_template.format(body=snippet)
                ),
            ]
            prompt_meta = {"prompt_source": "inline", "prompt_id": None}

        try:
            text = await self._adapter.complete(messages, self._gen_params)
            return {"text": text, "prompt": prompt_meta}
        except Exception as e:
            print(f"[WARN] textgen request failed: {e}")
            return None

    # ---- async batch processing ----
    async def _process_batch_async(
        self, rows: List[Dict[str, Any]]
    ) -> List[PostEnrichments]:
        if not rows:
            return []

        sem = asyncio.Semaphore(self._concurrency)

        async def run_one(row: Dict[str, Any]) -> Tuple[int, Optional[dict]]:
            async with sem:
                pid = row["post_id"]
                body = row.get("body") or ""
                res = await self._one_completion_async(body)
                return pid, res

        tasks = [asyncio.create_task(run_one(r)) for r in rows]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        out: List[PostEnrichments] = []
        for pid, res in results:
            if not res or not res.get("text"):
                continue
            payload = {
                "text": res["text"],
                "vendor": "vllm",
                "chat_model_id": self.cfg.chat_model_id,
                "temperature": self.cfg.temperature,
                "max_tokens": self.cfg.max_tokens,
                "prompt": res.get("prompt", {}),  # include provenance
            }
            out.append(
                {
                    "created_at": self.applied_datetime,
                    "retrieved_at": self.applied_datetime,
                    "post_id": pid,
                    "model_id": self.ENRICHER_ID,
                    "content": payload,  # JSONB
                }
            )
        return out

    # ---- sync wrapper used by runner ----
    def process_batch(self, rows: List[Dict[str, Any]]) -> List[PostEnrichments]:
        if not rows:
            return []
        return asyncio.run(self._process_batch_async(rows))

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
