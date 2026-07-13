"""Main pseudonymization module."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Dict, Any, Optional, Tuple, List
from psycopg.rows import dict_row
from ..store.standard_db import StandardDB
from ..store.models import MODEL_REGISTRY
from ..store.schema_config import SchemaConfig
from .pseudonyms import Hasher, Algorithm
from .redact import Redactor
from .policy import PseudonymPolicy, DEFAULT_POLICY
from .pii_policy import PiiPolicy

import concurrent.futures
import time
import logging, sys


log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)


@dataclass
class PseudonymizeConfig:
    """Configuration for the pseudonymization process.

    Attributes:
        src_db_name: Name of the source database.
        dst_db_name: Name of the destination database.
        pepper: Secret pepper for hashing.
        algorithm: Hashing algorithm to use.
        schema_package: Package containing the schema resource.
        schema_resource: Name of the schema resource file.
        time_window: Optional tuple of (start, end) ISO strings for filtering by created_at.
        chunk_rows: Number of rows to process in each batch.
        owner_schema: Optional owner schema for the destination database.
        ask_reinit: Whether to ask for confirmation before reinitializing the destination schema.
        pii_policy: Optional PiiPolicy enabling the Presidio-based PII detection
            engine for free-text columns (bio/body). If None (default), those
            columns fall back to the dependency-free regex-based Redactor
            (mentions/emails/URLs only) — the Presidio engine is strictly opt-in
            since it requires the "pii" extra (presidio-analyzer/-anonymizer)
            and a configured NLP model.
        nlp_configuration: Presidio NlpEngineProvider config (language model
            selection). Only used if pii_policy is set. See PiiEngine for the
            expected shape; if omitted, Presidio's own default model is used
            and a language-coverage warning is logged.
        custom_pii_recognizers: Extra PatternRecognizers scoped per
            (table, column), layered on top of the built-in platform ones.
            Only used if pii_policy is set.
        num_workers: Number of processes to use for the transform step
            (hashing/redaction). 1 (default) is the original single-process
            behavior. Fetch (one cursor) and flush (one writer) stay
            single-owner in the main process regardless of num_workers --
            only the CPU-bound per-row transform is dispatched to workers,
            so there's no concurrent DB access to worry about.
        transform_chunk_size: Number of rows sent to a worker per task when
            num_workers > 1. Only relevant if num_workers > 1.
        hypertable_config: Per-table chunk interval/space partitioning/
            compression tuning for the destination hypertables. Defaults to
            SchemaConfig() (this project's own Election2023 Twitter
            benchmark tuning) if not provided -- override for a dataset
            with a different volume/shape. See store/schema_config.py.
    """

    src_db_name: str
    dst_db_name: str
    pepper: bytes
    algorithm: Algorithm = Algorithm.SHA256
    schema_package: str = "smdt.store.schemas"
    schema_resource: str = "pseudo_std_schema.sql"
    time_window: Optional[Tuple[str, str]] = None
    chunk_rows: int = 1_000
    owner_schema: Optional[str] = None
    ask_reinit: bool = True
    pii_policy: Optional[PiiPolicy] = None
    nlp_configuration: Optional[Dict[str, Any]] = None
    custom_pii_recognizers: Optional[Dict[Tuple[str, str], List[Any]]] = None
    num_workers: int = 1
    transform_chunk_size: int = 500
    hypertable_config: Optional[SchemaConfig] = None


def _transform_row_impl(
    table: str,
    row: Dict[str, Any],
    policy: PseudonymPolicy,
    hasher: Hasher,
    redactor: Redactor,
    pii_engine: Any,
    pii_policy: Optional[PiiPolicy],
) -> Dict[str, Any]:
    """Pure transform logic, shared by the serial path (Pseudonymizer.
    _transform_row, called with self's own hasher/redactor/etc.) and worker
    processes in the parallel path (called with each worker's own
    independently-constructed hasher/redactor/etc. -- see _init_worker)."""
    out: Dict[str, Any] = {}
    if table == "entities":
        et = row.get("entity_type")
        raw_body = row.get("body")
        for col, val in row.items():
            if col == "body":
                continue
            if policy.is_drop(table, col):
                continue
            if policy.is_hash(table, col):
                out[col] = hasher.make(val)
            elif policy.is_blank(table, col):
                out[col] = None
            else:
                out[col] = val
        out["body"] = redactor.sanitize_entity_body(
            str(et) if et is not None else "", raw_body
        )
        return out

    platform = row.get("platform")
    for col, val in row.items():
        if policy.is_drop(table, col):
            continue
        if policy.is_hash(table, col):
            out[col] = hasher.make(val)
        elif policy.is_redact(table, col):
            if pii_engine is not None and pii_policy is not None and pii_policy.is_configured(table, col):
                out[col] = pii_engine.redact(val, table, col, pii_policy, platform=platform)
            else:
                out[col] = redactor.redact(val)
        elif policy.is_blank(table, col):
            out[col] = None
        else:
            out[col] = val
    return out


# ---------------------------------------------------------------------------
# Worker-process globals for num_workers > 1. Each worker process builds its
# own Hasher/Redactor/PiiEngine exactly once (via _init_worker, a
# ProcessPoolExecutor initializer), not per row or per batch -- same lesson
# as the extract_urls() singleton fix in standardizers/utils.py. Workers
# never open a database connection: fetch and flush both stay single-owner
# in the main process (see Pseudonymizer._copy_table_parallel).
# ---------------------------------------------------------------------------
_worker_hasher: Optional[Hasher] = None
_worker_redactor: Optional[Redactor] = None
_worker_pii_engine: Any = None
_worker_policy: Optional[PseudonymPolicy] = None
_worker_pii_policy: Optional[PiiPolicy] = None


def _init_worker(cfg: "PseudonymizeConfig", policy: PseudonymPolicy) -> None:
    global _worker_hasher, _worker_redactor, _worker_pii_engine, _worker_policy, _worker_pii_policy
    _worker_policy = policy
    _worker_pii_policy = cfg.pii_policy
    _worker_hasher = Hasher(
        algo=cfg.algorithm, pepper=cfg.pepper, normalizer=lambda s: s.strip().lower()
    )
    _worker_redactor = Redactor(
        handle_mapper=lambda h: _worker_hasher.make(h) or "", map_host=lambda host: host
    )
    _worker_pii_engine = None
    if cfg.pii_policy is not None:
        from .pii_engine import PiiEngine

        _worker_pii_engine = PiiEngine(
            hasher=_worker_hasher,
            nlp_configuration=cfg.nlp_configuration,
            custom_recognizers=cfg.custom_pii_recognizers,
        )


def _transform_batch_worker(table: str, rows: List[Dict[str, Any]]) -> List[Any]:
    """Runs in a worker process: transform + hydrate one batch of rows.
    No DB access here -- purely CPU-bound work."""
    out = []
    for row in rows:
        pseudo_row = _transform_row_impl(
            table, row, _worker_policy, _worker_hasher, _worker_redactor,
            _worker_pii_engine, _worker_pii_policy,
        )
        try:
            model_obj = Pseudonymizer._row_to_model(table, pseudo_row)
        except TypeError:
            allowed = set(Pseudonymizer._insert_columns_for_table(table))
            slim = {k: v for k, v in pseudo_row.items() if k in allowed}
            model_obj = Pseudonymizer._row_to_model(table, slim)
        out.append(model_obj)
    return out


class Pseudonymizer:
    """ETL: read from raw DB, transform, write to pseudonymized DB.

    This class handles the entire pseudonymization process, including reading from the
    source database, applying pseudonymization policies (hashing, redaction, etc.),
    and writing to the destination database.
    """

    def __init__(self, cfg: PseudonymizeConfig, policy: PseudonymPolicy = DEFAULT_POLICY):
        """Initialize the Pseudonymizer.

        Args:
            cfg: Configuration for the pseudonymization process.
            policy: Pseudonymization policy to apply.
        """
        self.cfg = cfg
        self.policy = policy
        self.src = StandardDB(cfg.src_db_name)
        self.dst = StandardDB(cfg.dst_db_name)
        self.hasher = Hasher(
            algo=cfg.algorithm,
            pepper=cfg.pepper,
            # Lowercase so e.g. accounts.username="JohnDoe" hashes identically to
            # a "@JohnDoe" mention found in free text (Redactor also lowercases
            # handles before hashing) — keeps pseudonyms consistent across columns.
            normalizer=lambda s: s.strip().lower(),
        )

        domain_mapper = lambda host: host
        self.redactor = Redactor(
            handle_mapper=lambda h: self.hasher.make(h) or "",
            map_host=domain_mapper,
        )

        self.pii_engine = None
        if cfg.pii_policy is not None:
            from .pii_engine import PiiEngine

            self.pii_engine = PiiEngine(
                hasher=self.hasher,
                nlp_configuration=cfg.nlp_configuration,
                custom_recognizers=cfg.custom_pii_recognizers,
            )

    def prepare_destination(self) -> None:
        """Initialize the destination database schema.

        This method initializes the destination database and applies the
        pseudonymization schema.
        """
        from importlib import resources as _res

        self.dst.init_db()
        with _res.as_file(
            _res.files(self.cfg.schema_package) / self.cfg.schema_resource
        ) as p:
            self.dst.init_schema(str(p), hypertable_config=self.cfg.hypertable_config)

    def run(self) -> None:
        """Run the pseudonymization process for all configured tables.

        This method iterates through a predefined list of tables, copies data
        from the source to the destination, applying pseudonymization transformations
        along the way.
        """
        self._ensure_prepared()
        log.info("Starting pseudonymization…")
        for table in [
            "communities",
            "accounts",
            "posts",
            "entities",
            "actions",
            "post_enrichments",
            "account_enrichments",
            "dataset_meta",
        ]:
            start = time.time()
            log.info("Starting pseudonymization for table %s", table)
            rows = self._copy_table(table)
            log.info("Finished %s: %d rows in %.2fs", table, rows, time.time() - start)

    def _ensure_prepared(self) -> None:
        """Ensure the destination database is prepared.

        Checks if the destination database already has the required tables.
        If not, it initializes the schema. If it does, it may ask the user
        whether to reinitialize, depending on configuration.
        """
        from importlib import resources as _res
        import sys

        self.dst.init_db()
        conn = self.dst.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = COALESCE(current_schema(), 'public')
                    AND table_name IN ('accounts', 'posts', 'entities', 'actions', 'post_enrichments', 'account_enrichments', 'communities', 'dataset_meta')
                    """
                )
                existing = {r[0] for r in cur.fetchall()}
        finally:
            conn.close()

        if existing == {
            "accounts",
            "posts",
            "entities",
            "actions",
            "post_enrichments",
            "account_enrichments",
            "communities",
            "dataset_meta",
        }:
            # Destination looks initialized
            if self.cfg.ask_reinit and sys.stdin.isatty():
                try:
                    ans = (
                        input(
                            "Destination DB already initialized. Reinitialize schema? [y/N]: "
                        )
                        .strip()
                        .lower()
                    )
                except EOFError:
                    ans = "n"
                if ans in {"y", "yes"}:
                    log.info("Reinitializing destination schema by user request…")
                    self._reinit_destination_schema()
                    with _res.as_file(
                        _res.files(self.cfg.schema_package) / self.cfg.schema_resource
                    ) as p:
                        self.dst.init_schema(str(p), hypertable_config=self.cfg.hypertable_config)
                else:
                    log.info("Keeping existing destination schema.")
            else:
                log.info(
                    "Destination schema detected; proceeding without reinit "
                    "(non-interactive or ask_reinit=False)."
                )
            return

        # Not initialized apply schema
        with _res.as_file(
            _res.files(self.cfg.schema_package) / self.cfg.schema_resource
        ) as p:
            self.dst.init_schema(str(p), hypertable_config=self.cfg.hypertable_config)
        log.info("Applied pseudonymized schema to destination database.")

    def _select_iter(self, table: str) -> Iterable[Dict[str, Any]]:
        """Iterate over rows from the source table.

        Args:
            table: Name of the table to read from.

        Yields:
            Dictionary representing a row from the table.
        """
        conn = self.src.connect()
        try:
            # A plain (unnamed) cursor's execute() pulls the *entire* result
            # set into client memory immediately -- the fetchmany() loop
            # below only looks like streaming. name= makes this a real
            # server-side cursor, so Postgres streams chunk_rows-sized
            # batches on demand instead (measured: unnamed cursor on a
            # 64M-row table took 297s / 17GB RSS just for execute(), before
            # any row was processed).
            with conn.cursor(name=f"pseudo_select_{table}", row_factory=dict_row) as cur:
                where = ""
                params: List[Any] = []
                if self.cfg.time_window and self._has_col(table, "created_at"):
                    where = " WHERE created_at >= %s AND created_at < %s"
                    params = [self.cfg.time_window[0], self.cfg.time_window[1]]
                # No ORDER BY: row order has no effect on correctness here --
                # _transform_row/_flush operate on each row independently,
                # with no cross-row state, so the destination ends up as the
                # same set of rows regardless of arrival order. Ordering by
                # id forces a full Gather Merge + Sort across every chunk;
                # ordering by created_at avoids that sort but still has to
                # coordinate a ChunkAppend across every chunk (including
                # thinly-populated historical ones) to keep results globally
                # time-ordered. Without ORDER BY, Postgres can just Append
                # chunks in whatever order it likes and start returning rows
                # from the first one immediately -- no sort, no cross-chunk
                # merge coordination at all.
                cur.execute(f"SELECT * FROM {table}{where}", params)
                batch = cur.fetchmany(self.cfg.chunk_rows)
                total = 0
                while batch:
                    total += len(batch)
                    log.debug(
                        "Fetched %d rows (total %d) from %s", len(batch), total, table
                    )
                    for row in batch:
                        yield row
                    batch = cur.fetchmany(self.cfg.chunk_rows)
        finally:
            conn.close()

    def _has_col(self, table: str, col: str) -> bool:
        """Check if a table has a specific column.

        Args:
            table: Name of the table.
            col: Name of the column.

        Returns:
            True if the table has the column, False otherwise.
        """
        for cls, meta in MODEL_REGISTRY.items():
            if meta.get("table") == table:
                cols = set(
                    getattr(cls, "__all_columns__", []) or meta.get("columns", [])
                )
                return col in cols
        return False

    def _transform_row(self, table: str, row: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single row according to the pseudonymization policy.

        Args:
            table: Name of the table the row belongs to.
            row: Dictionary representing the row data.

        Returns:
            Transformed row dictionary.
        """
        return _transform_row_impl(
            table, row, self.policy, self.hasher, self.redactor,
            self.pii_engine, self.cfg.pii_policy,
        )

    @staticmethod
    def _row_to_model(table: str, row: Dict[str, Any]) -> Any:
        """Convert a row dictionary to a model instance.

        Args:
            table: Name of the table.
            row: Dictionary representing the row data.

        Returns:
            Model instance corresponding to the row.

        Raises:
            KeyError: If no model is registered for the table.
        """
        model_cls = None
        for cls, meta in MODEL_REGISTRY.items():
            if meta.get("table") == table:
                model_cls = cls
                break
        if model_cls is None:
            raise KeyError(f"No model registered for table {table}")
        if hasattr(model_cls, "from_dict"):
            return model_cls.from_dict(row)
        if hasattr(model_cls, "from_row"):
            return model_cls.from_row(row)
        return model_cls(**row)

    def _copy_table(self, table: str) -> int:
        """Copy and pseudonymize a table from source to destination.

        Args:
            table: Name of the table to copy.

        Returns:
            Number of rows copied.
        """
        if self.cfg.num_workers and self.cfg.num_workers > 1:
            return self._copy_table_parallel(table, self.cfg.num_workers)

        buf: List[Any] = []
        count = 0
        t0 = time.time()

        for src_row in self._select_iter(table):

            pseudo_row = self._transform_row(table, src_row)
            try:
                model_obj = self._row_to_model(table, pseudo_row)
            except TypeError:
                allowed = set(self._insert_columns_for_table(table))
                slim = {k: v for k, v in pseudo_row.items() if k in allowed}
                model_obj = self._row_to_model(table, slim)
                log.exception(
                    "TypeError hydrating model for table %s; using slim row", table
                )

            buf.append(model_obj)
            count += 1

            if len(buf) >= self.cfg.chunk_rows:
                self._flush(table, buf)
                elapsed = time.time() - t0
                rps = count / elapsed if elapsed > 0 else 0.0
                log.info("Flushed %d rows into %s (%.0f rows/s)", count, table, rps)
                buf.clear()

        if buf:
            self._flush(table, buf)
            elapsed = time.time() - t0
            rps = count / elapsed if elapsed > 0 else 0.0
            log.info("Final flush %d rows into %s (%.0f rows/s)", count, table, rps)

        return count

    def _copy_table_parallel(self, table: str, num_workers: int) -> int:
        """Same as _copy_table, but dispatches the transform step (hashing +
        redaction) to a process pool. Fetch (the single _select_iter cursor)
        and flush (the single _flush writer) stay exactly as they are in the
        serial path -- only _transform_row/_row_to_model moves to worker
        processes. Unlike ingestion's file-level parallelism, workers here
        never touch either database at all, so there's no concurrent-
        connection contention to worry about.

        Runs a bounded pipeline (at most num_workers * 2 batches in flight)
        rather than submitting the whole table upfront, so memory stays
        bounded regardless of table size.

        Args:
            table: Name of the table to copy.
            num_workers: Number of worker processes.

        Returns:
            Number of rows copied.
        """
        flush_buf: List[Any] = []
        count = 0
        t0 = time.time()
        max_in_flight = num_workers * 2

        def row_batches():
            batch: List[Dict[str, Any]] = []
            for row in self._select_iter(table):
                batch.append(row)
                if len(batch) >= self.cfg.transform_chunk_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        batches_iter = row_batches()
        in_flight: Dict[concurrent.futures.Future, None] = {}

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=num_workers,
            initializer=_init_worker,
            initargs=(self.cfg, self.policy),
        ) as pool:

            def submit_next() -> None:
                batch = next(batches_iter, None)
                if batch is not None:
                    in_flight[pool.submit(_transform_batch_worker, table, batch)] = None

            for _ in range(max_in_flight):
                submit_next()

            while in_flight:
                done, _ = concurrent.futures.wait(
                    in_flight, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for fut in done:
                    del in_flight[fut]
                    model_objs = fut.result()
                    flush_buf.extend(model_objs)
                    count += len(model_objs)

                    if len(flush_buf) >= self.cfg.chunk_rows:
                        self._flush(table, flush_buf)
                        elapsed = time.time() - t0
                        rps = count / elapsed if elapsed > 0 else 0.0
                        log.info(
                            "Flushed %d rows into %s (%.0f rows/s, %d workers)",
                            count, table, rps, num_workers,
                        )
                        flush_buf.clear()

                    submit_next()

        if flush_buf:
            self._flush(table, flush_buf)
            elapsed = time.time() - t0
            rps = count / elapsed if elapsed > 0 else 0.0
            log.info(
                "Final flush %d rows into %s (%.0f rows/s, %d workers)",
                count, table, rps, num_workers,
            )

        return count

    @staticmethod
    def _insert_columns_for_table(table: str) -> List[str]:
        """Get the list of columns to insert for a table.

        Args:
            table: Name of the table.

        Returns:
            List of column names.
        """
        for cls, meta in MODEL_REGISTRY.items():
            if meta.get("table") == table:
                sample = getattr(cls, "insert_columns", None)
                if callable(sample):
                    return list(cls.insert_columns())
        return []

    def _flush(self, table: str, items: List[Any]) -> None:
        """Flush a batch of items to the destination database.

        Args:
            table: Name of the table.
            items: List of model instances to insert.
        """
        on_conflict = None
        if table == "accounts":
            on_conflict = "(created_at, id) DO NOTHING"
        elif table == "posts":
            on_conflict = "(created_at, id) DO NOTHING"
        elif table == "communities":
            on_conflict = "(created_at, id) DO NOTHING"
        elif table in {"post_enrichments", "account_enrichments"}:
            on_conflict = None
        self.dst.insert_with_fallbacks(items, on_conflict=on_conflict)

    def _reinit_destination_schema(self) -> None:
        """Drop known tables/policies in the current schema and start fresh.

        Uses owner_schema if provided to reset the whole schema, else drops tables one by one.
        """
        # If an owner schema is configured in DBConfig, prefer StandardDB.reset_schema()
        if self.cfg.owner_schema:
            try:
                # StandardDB.reset_schema uses cfg.owner from DBConfig, not PseudonymizeConfig
                self.dst.reset_schema()  # may require cfg.owner to be set in DBConfig
                return
            except Exception:
                pass

        tables = [
            "account_enrichments",
            "post_enrichments",
            "actions",
            "entities",
            "posts",
            "accounts",
            "communities",
            "dataset_meta",
        ]
        conn = self.dst.connect()
        try:
            with conn.cursor() as cur:
                # Drop TimescaleDB policies if present to avoid dependency errors
                try:
                    cur.execute("SELECT remove_compression_policy('accounts')")
                except Exception:
                    pass
                try:
                    cur.execute("SELECT remove_compression_policy('posts')")
                except Exception:
                    pass
                try:
                    cur.execute("SELECT remove_compression_policy('communities')")
                except Exception:
                    pass
                try:
                    cur.execute("SELECT remove_compression_policy('entities')")
                except Exception:
                    pass
                try:
                    cur.execute("SELECT remove_compression_policy('actions')")
                except Exception:
                    pass
                # Drop tables in dependency order
                for t in tables:
                    try:
                        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
                    except Exception:
                        pass
            conn.commit()
        finally:
            conn.close()
