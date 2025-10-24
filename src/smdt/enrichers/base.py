# smdt/enrichers/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple


class BaseEnricher(ABC):
    """
    Minimal interface:
      - stream batches from DB
      - process batches with user-supplied model
      - write back results to enrichment tables
    """

    TARGET: str = "posts"  # or "accounts"
    ENRICHER_ID: str = "base"  # used in (post_id, model_id) or (account_id, model_id)

    def __init__(self, db, *, config: Optional[Dict[str, Any]] = None):
        self.db = db
        self.config = config or {}
        self.model = None

    # ---- Lifecycle ---------------------------------------------------------
    def setup(self) -> None:
        """Optional hook (e.g., open connections)."""
        pass

    def teardown(self) -> None:
        """Optional hook (e.g., close connections)."""
        pass

    # ---- Model -------------------------------------------------------------
    def load_model(self) -> None:
        """Optional; load lightweight models here."""
        pass

    # ---- DB IO -------------------------------------------------------------
    @abstractmethod
    def fetch_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        """Return a list of rows to enrich (dict-ish)."""
        ...

    @abstractmethod
    def total_count(self) -> Optional[int]:
        """Return total rows or None (unknown/streaming)."""
        ...

    @abstractmethod
    def process_batch(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform rows into enrichment rows.
        Each result dict should match your enrichment model (e.g., PostEnrichments).
        """
        ...

    @abstractmethod
    def save_results(self, results: List[Dict[str, Any]]) -> None:
        """Persist results (use bulk upsert/insert)."""
        ...

    # ---- Runner ------------------------------------------------------------
    def run(self, *, db_batch_size: int = 1000) -> None:
        try:
            self.setup()
            self.load_model()
            total = self.total_count()
            if total is None:
                # unknown total; just pull until empty
                offset = 0
                while True:
                    batch = self.fetch_batch(offset, db_batch_size)
                    if not batch:
                        break
                    results = self.process_batch(batch)
                    if results:
                        self.save_results(results)
                    offset += len(batch)
            else:
                for offset in range(0, total, db_batch_size):
                    batch = self.fetch_batch(offset, db_batch_size)
                    if not batch:
                        break
                    results = self.process_batch(batch)
                    if results:
                        self.save_results(results)
        finally:
            self.teardown()
