# smdt/enrichers/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Type, TypeVar
from pathlib import Path
import tqdm
import re
from uuid import uuid4

ConfigT = TypeVar("ConfigT")


@dataclass(kw_only=True)
class EnricherRunConfig:
    """Run-behavior settings shared by every enricher.

    Every enricher's own config dataclass should subclass this rather than
    redeclaring these fields, so there's exactly one definition (and one set
    of validation rules) instead of one slightly-different copy per file.

    Attributes:
        only_missing: Skip rows that already have an enrichment for this model.
        reset_cache: Clear the local cache of processed IDs before running.
        cache_dir: Directory for the local cache file. Defaults to
            ``~/.smdt_enricher_cache`` if not set.
        do_save_to_db: Write results to the database; ``False`` writes JSONL
            files to ``output_dir`` instead.
        output_dir: Required when ``do_save_to_db=False``.
    """

    only_missing: bool = True
    reset_cache: bool = False
    cache_dir: Optional[str] = None
    do_save_to_db: bool = True
    output_dir: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.do_save_to_db:
            if not self.output_dir:
                raise ValueError("output_dir is required when do_save_to_db=False.")
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)


class BaseEnricher(ABC):
    """Abstract base class for data enrichers.

    Enrichers are responsible for:
    1. Fetching batches of data from a source (e.g., database).
    2. Processing the data using a model or algorithm.
    3. Saving the enriched results back to the database.

    Subclasses are expected to, in their own ``__init__``:
      1. Call ``super().__init__(db)``.
      2. Build ``self.cfg`` via ``self._coerce_config(config, MyConfig)``.
      3. Set ``self.model_id`` (see ``_make_model_id``).
      4. Call ``self._init_cache()``.

    Class attributes ``TARGET``/``ENRICHER_NAME`` are stamped automatically by
    the ``@register(...)`` decorator; they should not be redeclared by hand.
    """

    TARGET: ClassVar[str] = "posts"
    ENRICHER_NAME: ClassVar[str] = "base"

    def __init__(self, db):
        """Initialize the enricher.

        Args:
            db: Database connection or handler.
        """
        self.db = db
        self.model = None
        self.model_id: str = self.ENRICHER_NAME

        self.cache_prefix = "smdt_session_cache_"
        self.generated_temp_table_name = self.cache_prefix + uuid4().hex

    # ---- Config --------------------------------------------------------
    @staticmethod
    def _coerce_config(config: Any, config_cls: Type[ConfigT]) -> ConfigT:
        """Accept a config as a ready instance, a plain dict, or None.

        Args:
            config: A `config_cls` instance, a dict of its fields, or None.
            config_cls: The enricher's own config dataclass.

        Returns:
            A `config_cls` instance.

        Raises:
            TypeError: If `config` is none of the above.
        """
        if config is None:
            return config_cls()
        if isinstance(config, config_cls):
            return config
        if isinstance(config, dict):
            return config_cls(**config)
        raise TypeError(
            f"config must be a {config_cls.__name__} instance or dict, "
            f"got {type(config).__name__}"
        )

    def _make_model_id(self, suffix: Optional[str] = None) -> str:
        """Build the value stored in `*_enrichments.model_id`.

        Args:
            suffix: Optional per-instance suffix, for enrichers where one
                registered enricher can run many different models (e.g. a
                different HF checkpoint or a different LLM per run).

        Returns:
            `ENRICHER_NAME`, or `f"{ENRICHER_NAME}_{suffix}"` if given.
        """
        return f"{self.ENRICHER_NAME}_{suffix}" if suffix else self.ENRICHER_NAME

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

    # ---- Cache ---------------------------------------------------------
    def _init_cache(self) -> None:
        """Load or reset the on-disk/DB cache of already-processed IDs.

        Every enricher should call this once in `__init__`, after `self.cfg`
        and `self.model_id` are set, instead of duplicating this logic itself.
        Requires `self.cfg` to expose `cache_dir`/`reset_cache` (i.e. subclass
        from `EnricherRunConfig`).
        """
        self.cache_dir = self.cfg.cache_dir or str(Path.home() / ".smdt_enricher_cache")
        if self.cfg.reset_cache:
            self.cached_ids = set()
            self.reset_cache()
        else:
            self.cached_ids = set(self.load_cached_output_ids_from_file())
            self.setup_cache_table()

    def reset_cache(self) -> None:
        """Reset the cache by clearing cached IDs."""
        cache_file = Path(self.cache_dir) / f"{self.model_id}_cached_ids.txt"
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
        cache_file = Path(self.cache_dir) / f"{self.model_id}_cached_ids.txt"
        # append to the file
        with open(cache_file, "a") as f:
            for id_ in ids:
                f.write(f"{id_}\n")

    def setup_cache_table(self):
        if not self.cached_ids or self.cfg.reset_cache:
            return

        # Create a safe table name (e.g., cache_bert_sentiment)
        safe_name = re.sub(r"[^a-zA-Z0-9]", "_", self.model_id).lower()
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
        cache_file = Path(self.cache_dir) / f"{self.model_id}_cached_ids.txt"
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
