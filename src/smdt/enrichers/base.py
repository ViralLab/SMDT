# smdt/enrichers/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple


class BaseEnricher(ABC):
    """Abstract base class for data enrichers.

    Enrichers are responsible for:
    1. Fetching batches of data from a source (e.g., database).
    2. Processing the data using a model or algorithm.
    3. Saving the enriched results back to the database.

    Attributes:
        TARGET: The target table or entity type (e.g., "posts", "accounts").
        ENRICHER_ID: Unique identifier for the enricher.
    """

    TARGET: str = "posts"  # or "accounts"
    ENRICHER_ID: str = "base"  # used in (post_id, model_id) or (account_id, model_id)

    def __init__(self, db, *, config: Optional[Dict[str, Any]] = None):
        """Initialize the enricher.

        Args:
            db: Database connection or handler.
            config: Optional configuration dictionary.
        """
        self.db = db
        self.config = config or {}
        self.model = None

    # ---- Lifecycle ---------------------------------------------------------
    def setup(self) -> None:
        """Perform setup actions before running the enricher.

        This method can be overridden to open connections, load resources, etc.
        """
        pass

    def teardown(self) -> None:
        """Perform teardown actions after running the enricher.

        This method can be overridden to close connections, release resources, etc.
        """
        pass

    # ---- Model -------------------------------------------------------------
    def load_model(self) -> None:
        """Load the model used for enrichment.

        This method should be overridden to load any necessary models or data.
        """
        pass

    # ---- DB IO -------------------------------------------------------------
    @abstractmethod
    def fetch_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        """Fetch a batch of rows to enrich.

        Args:
            offset: Starting offset.
            limit: Maximum number of rows to fetch.

        Returns:
            List of dictionaries representing the rows.
        """
        ...

    @abstractmethod
    def total_count(self) -> Optional[int]:
        """Get the total number of rows to process.

        Returns:
            Total number of rows, or None if unknown (streaming).
        """
        ...

    @abstractmethod
    def process_batch(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of rows and generate enrichment results.

        Args:
            rows: List of input rows.

        Returns:
            List of enrichment result dictionaries.
        """
        ...

    @abstractmethod
    def save_results(self, results: List[Dict[str, Any]]) -> None:
        """Save enrichment results to the database.

        Args:
            results: List of enrichment result dictionaries.
        """
        ...

    # ---- Runner ------------------------------------------------------------
    def run(self, *, db_batch_size: int = 1000) -> None:
        """Run the enrichment process.

        Iterates through the data in batches, processes them, and saves the results.

        Args:
            db_batch_size: Number of rows to process in each batch.
        """
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
