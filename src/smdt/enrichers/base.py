# smdt/enrichers/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Optional, Tuple, Type, TypeVar
from pathlib import Path
from urllib.parse import urlparse
import logging
import tqdm
import re
from uuid import uuid4

from smdt.pseudonymizer.pseudonyms import Algorithm
from smdt.pseudonymizer.pii_policy import PiiPolicy

log = logging.getLogger(__name__)

ConfigT = TypeVar("ConfigT")

RowPreprocessor = Callable[[Dict[str, Any]], Dict[str, Any]]

# Known hosted third-party LLM/embedding API hosts. Best-effort: only warns
# about this small list, not every conceivable commercial endpoint, and never
# warns for self-hosted backends (Ollama, local vLLM, HF text-generation-
# inference) since those don't leave the caller's own infrastructure.
_COMMERCIAL_API_HOSTS = {
    "api.openai.com",
    "api.anthropic.com",
    "generativelanguage.googleapis.com",
    "api-inference.huggingface.co",
}


def warn_if_unprotected_commercial_api(cfg: "EnricherRunConfig", base_url: str) -> None:
    """Warn when a server-backed enricher sends unredacted content to a
    known commercial API host with no privacy layer configured.

    Called from a server-backed config's own `__post_init__` (e.g.
    `TextGenerationConfig`, `EmbeddingConfig`), since `EnricherRunConfig` itself has
    no `base_url` field -- only enrichers that actually make network calls
    know what to check.

    Args:
        cfg: The config instance being validated (checked for `privacy_fields`).
        base_url: The endpoint this enricher will send row content to.
    """
    if cfg.privacy_fields:
        return
    host = (urlparse(base_url).hostname or "").lower()
    if host in _COMMERCIAL_API_HOSTS:
        log.warning(
            "base_url '%s' looks like a commercial API host and no "
            "privacy_fields are configured on this enricher -- raw row "
            "content (e.g. post body) will be sent to it unredacted. Set "
            "privacy_fields (and pii_policy) to enable the built-in "
            "redaction/hashing layer if that isn't intended.",
            base_url,
        )


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
        privacy_fields: Row fields to redact/hash via `smdt.pseudonymizer`
            before anything else touches the row. This is the built-in
            privacy layer: disabled by default (empty list); enable it by
            listing the fields to protect (e.g. ``["body"]``, or
            ``["account_name", "bio"]`` for an account-level enricher). Each
            listed field is redacted with the dependency-free `Redactor`
            (mentions/emails/URLs), or with the Presidio-based `PiiEngine`
            for fields `pii_policy` actually configures, exactly like
            `Pseudonymizer._transform_row`'s redact branch.
        pii_policy: Optional `PiiPolicy` enabling Presidio-based PII detection
            (phone numbers, emails, names, ...) for `privacy_fields`. Requires
            the `pii` extra. If `None`, `privacy_fields` still get the
            baseline `Redactor` treatment. Ignored if `privacy_fields` is
            empty.
        pepper: Secret pepper for the `Hasher` used to redact MENTION-type
            entities and build the fallback `Redactor`. Required whenever
            `privacy_fields` is non-empty.
        algorithm: Hashing algorithm for the same `Hasher`.
        nlp_configuration: Presidio `NlpEngineProvider` config, forwarded to
            `PiiEngine` if `pii_policy` is set. See `PiiEngine` for the
            expected shape.
        custom_pii_recognizers: Extra `PatternRecognizer`s scoped per
            (table, column), forwarded to `PiiEngine` if `pii_policy` is set.
        preprocessors: Ordered list of row-transforming functions applied to
            every fetched row, after the privacy layer above and before
            ``process_batch`` sees it. Each takes the row dict and returns a
            (possibly modified) row dict; they run in list order, each seeing
            the previous one's output. Not privacy-specific -- this is a
            general-purpose hook for the caller's own transforms (cleaning up
            artifacts the privacy layer left behind, truncation, whatever).
            Defaults to an empty list: no behavior change unless configured.
    """

    only_missing: bool = True
    reset_cache: bool = False
    cache_dir: Optional[str] = None
    do_save_to_db: bool = True
    output_dir: Optional[str] = None
    privacy_fields: List[str] = field(default_factory=list)
    pii_policy: Optional[PiiPolicy] = None
    pepper: Optional[bytes] = None
    algorithm: Algorithm = Algorithm.SHA256
    nlp_configuration: Optional[Dict[str, Any]] = None
    custom_pii_recognizers: Optional[Dict[Tuple[str, str], List[Any]]] = None
    preprocessors: List[RowPreprocessor] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.do_save_to_db:
            if not self.output_dir:
                raise ValueError("output_dir is required when do_save_to_db=False.")
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        if self.privacy_fields and self.pepper is None:
            raise ValueError(
                "pepper is required when privacy_fields is set (needed to "
                "build the Hasher used for the privacy layer's redaction)."
            )


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

    def _apply_privacy(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Redact/hash `self.cfg.privacy_fields`, before preprocessors run.

        Disabled by default (`privacy_fields` empty -- no `smdt.pseudonymizer`
        objects are even constructed). When enabled, mirrors
        `Pseudonymizer._transform_row`'s redact branch: a field configured in
        `pii_policy` goes through the Presidio-based `PiiEngine`; every other
        listed field falls back to the dependency-free `Redactor`. The
        `Redactor`/`Hasher`/`PiiEngine` are built once and cached on `self`.

        Args:
            rows: Rows as returned by `fetch_batch`.

        Returns:
            Rows with `privacy_fields` redacted/hashed in place, or the same
            rows unchanged if `privacy_fields` is empty.
        """
        fields = getattr(self.cfg, "privacy_fields", None)
        if not fields:
            return rows

        if not hasattr(self, "_privacy_redactor"):
            from smdt.pseudonymizer.pseudonyms import Hasher
            from smdt.pseudonymizer.redact import Redactor

            hasher = Hasher(
                algo=self.cfg.algorithm,
                pepper=self.cfg.pepper,
                normalizer=lambda s: s.strip().lower(),
            )
            self._privacy_redactor = Redactor(
                handle_mapper=lambda h: hasher.make(h) or "",
                map_host=lambda host: host,
            )
            self._privacy_engine = None
            if self.cfg.pii_policy is not None:
                from smdt.pseudonymizer.pii_engine import PiiEngine

                self._privacy_engine = PiiEngine(
                    hasher=hasher,
                    nlp_configuration=self.cfg.nlp_configuration,
                    custom_recognizers=self.cfg.custom_pii_recognizers,
                )

        table = self.TARGET
        policy = self.cfg.pii_policy
        for i, row in enumerate(rows):
            row = dict(row)
            platform = row.get("platform")
            for col in fields:
                val = row.get(col)
                if val is None:
                    continue
                if self._privacy_engine is not None and policy.is_configured(table, col):
                    row[col] = self._privacy_engine.redact(
                        val, table, col, policy, platform=platform
                    )
                else:
                    row[col] = self._privacy_redactor.redact(val)
            rows[i] = row
        return rows

    def _apply_preprocessors(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run `self.cfg.preprocessors` over every row, in order.

        Called centrally by `run()` right after `fetch_batch()`, so no
        individual enricher needs to remember to wire this in itself. A row
        is threaded through each preprocessor in list order before
        `process_batch` ever sees it.

        Args:
            rows: Rows as returned by `fetch_batch`.

        Returns:
            Rows after every configured preprocessor has run, in place if
            `preprocessors` is empty (no copy, no behavior change).
        """
        preprocessors = getattr(self.cfg, "preprocessors", None)
        if not preprocessors:
            return rows
        for i, row in enumerate(rows):
            for step in preprocessors:
                row = step(row)
            rows[i] = row
        return rows

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
                    batch = self._apply_privacy(batch)
                    batch = self._apply_preprocessors(batch)
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
                    batch = self._apply_privacy(batch)
                    batch = self._apply_preprocessors(batch)
                    results = self.process_batch(batch)
                    if results:
                        self.save_results(results)
                    pbar.update(len(batch))
                pbar.close()
        finally:
            self.teardown()
