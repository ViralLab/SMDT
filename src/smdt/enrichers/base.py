# smdt/enrichers/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple
from pathlib import Path
import tqdm
import re
from uuid import uuid4


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

        self.cache_dir = self.config.get("cache_dir") or str(
            Path.home() / ".smdt_enricher_cache"
        )
        self.cache_prefix = "smdt_session_cache_"
        self.generated_temp_table_name = self.cache_prefix + uuid4().hex

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

    def reset_cache(self) -> None:
        """Reset the cache by clearing cached IDs."""
        cache_file = Path(self.cache_dir) / f"{self.ENRICHER_ID}_cached_ids.txt"
        if cache_file.exists():
            cache_file.unlink()

        try:
            with self.db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DROP TABLE IF EXISTS session_cache_{self.generated_temp_table_name}"
                    )
                conn.commit()
        except Exception as e:
            raise Exception(f"Error resetting cache table: {e}")

    def write_current_cache_ids_to_file(self, ids: Iterable[int]) -> None:
        """Cache output IDs to avoid redundant processing.

        Args:
            ids: Iterable of IDs that have already been processed.
        """
        # ensure cache directory exists
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
        cache_file = Path(self.cache_dir) / f"{self.ENRICHER_ID}_cached_ids.txt"
        # append to the file
        with open(cache_file, "a") as f:
            for id_ in ids:
                f.write(f"{id_}\n")

    def setup_cache_table(self):
        if not self.cached_ids or self.cfg.reset_cache:
            return

        # Create a safe table name (e.g., cache_bert_sentiment)
        safe_name = re.sub(r"[^a-zA-Z0-9]", "_", self.ENRICHER_ID).lower()
        self.cache_table_name = f"cache_ids_{safe_name}"

        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                # 1. Create a regular table
                cur.execute(f"DROP TABLE IF EXISTS {self.generated_temp_table_name}")
                cur.execute(
                    f"CREATE TABLE {self.generated_temp_table_name} (post_id TEXT PRIMARY KEY)"
                )

                # 2. Bulk Insert
                data = [(str(i),) for i in self.cached_ids]
                cur.executemany(
                    f"INSERT INTO {self.generated_temp_table_name} (post_id) VALUES (%s)",
                    data,
                )

                conn.commit()
                print(f"Initialized persistent cache table: {self.cache_table_name}")
        finally:
            conn.close()

    def load_cached_output_ids_from_file(self) -> Iterable[int]:
        """Load cached output IDs.

        Returns:
            Iterable of cached IDs.
        """
        # check if cache file exists
        cache_file = Path(self.cache_dir) / f"{self.ENRICHER_ID}_cached_ids.txt"
        if not cache_file.exists():
            return []

        cached_dids = set()
        with open(cache_file, "r") as f:
            for line in f:
                cached_dids.add(line.strip())
        return cached_dids

    def clean_up_persistent_cache_tables(self):
        # remove all of the tables starting with session_cache_
        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT tablename FROM pg_tables WHERE tablename LIKE '{self.cache_prefix}%'"
                )
                tables = cur.fetchall()
                for (table_name,) in tables:
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
        finally:
            conn.close()

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
            total: int | None = self.total_count()
            if total is None:
                # unknown total; just pull until empty
                pbar = None
                offset = 0
                while True:
                    batch = self.fetch_batch(offset, db_batch_size)
                    if not batch:
                        break
                    results = self.process_batch(batch)
                    if results:
                        self.save_results(results)
                    offset += len(batch)
                    if pbar is None:
                        pbar = tqdm.tqdm(desc="Enriching data")
                    pbar.update(len(batch))
            else:
                pbar = tqdm.tqdm(total=total, desc="Enriching data")
                for offset in range(0, total, db_batch_size):
                    batch = self.fetch_batch(offset, db_batch_size)
                    if not batch:
                        break
                    results = self.process_batch(batch)
                    if results:
                        self.save_results(results)
                    pbar.update(len(batch))
                pbar.close()
        finally:
            self.teardown()
