"""Main anonymization module."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Dict, Any, Optional, Tuple, List
from psycopg.rows import dict_row
from ..store.standard_db import StandardDB
from ..store.models import MODEL_REGISTRY
from .pseudonyms import Pseudonymizer, Algorithm
from .redact import Redactor
from .policy import AnonPolicy, DEFAULT_POLICY

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
class AnonymizeConfig:
    """Configuration for the anonymization process.

    Attributes:
        src_db_name: Name of the source database.
        dst_db_name: Name of the destination database.
        pepper: Secret pepper for hashing.
        algorithm: Hashing algorithm to use.
        output_hex_len: Length of the output hex string.
        schema_package: Package containing the schema resource.
        schema_resource: Name of the schema resource file.
        time_window: Optional tuple of (start, end) ISO strings for filtering by created_at.
        chunk_rows: Number of rows to process in each batch.
        owner_schema: Optional owner schema for the destination database.
        ask_reinit: Whether to ask for confirmation before reinitializing the destination schema.
    """

    src_db_name: str
    dst_db_name: str
    pepper: bytes
    algorithm: Algorithm = Algorithm.SHA256
    output_hex_len: int = 64
    schema_package: str = "smdt.store.schemas"
    schema_resource: str = "anon_std_schema.sql"
    time_window: Optional[Tuple[str, str]] = None
    chunk_rows: int = 1_000
    owner_schema: Optional[str] = None
    ask_reinit: bool = True


class Anonymizer:
    """ETL: read from raw DB, transform, write to anonymized DB.

    This class handles the entire anonymization process, including reading from the
    source database, applying anonymization policies (hashing, redaction, etc.),
    and writing to the destination database.
    """

    def __init__(self, cfg: AnonymizeConfig, policy: AnonPolicy = DEFAULT_POLICY):
        """Initialize the Anonymizer.

        Args:
            cfg: Configuration for the anonymization process.
            policy: Anonymization policy to apply.
        """
        self.cfg = cfg
        self.policy = policy
        self.src = StandardDB(cfg.src_db_name)
        self.dst = StandardDB(cfg.dst_db_name)
        self.pseudo = Pseudonymizer(
            algo=cfg.algorithm,
            pepper=cfg.pepper,
            output_hex_len=cfg.output_hex_len,
            normalizer=lambda s: s.strip(),
        )

        domain_mapper = lambda host: host
        self.redactor = Redactor(
            handle_mapper=lambda h: self.pseudo.make(h) or "",
            map_host=domain_mapper,
        )

    def prepare_destination(self) -> None:
        """Initialize the destination database schema.

        This method initializes the destination database and applies the
        anonymization schema.
        """
        from importlib import resources as _res

        self.dst.init_db()
        with _res.as_file(
            _res.files(self.cfg.schema_package) / self.cfg.schema_resource
        ) as p:
            self.dst.init_schema(str(p))

    def run(self) -> None:
        """Run the anonymization process for all configured tables.

        This method iterates through a predefined list of tables, copies data
        from the source to the destination, applying anonymization transformations
        along the way.
        """
        self._ensure_prepared()
        log.info("Starting anonymization…")
        for table in [
            "communities",
            "accounts",
            "posts",
            "entities",
            "actions",
            "post_enrichments",
            "account_enrichments",
        ]:
            start = time.time()
            log.info("Starting anonymization for table %s", table)
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
                    AND table_name IN ('accounts', 'posts', 'entities', 'actions', 'post_enrichments', 'account_enrichments', 'communities')
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
                        self.dst.init_schema(str(p))
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
            self.dst.init_schema(str(p))
        log.info("Applied anon schema to destination database.")

    def _select_iter(self, table: str) -> Iterable[Dict[str, Any]]:
        """Iterate over rows from the source table.

        Args:
            table: Name of the table to read from.

        Yields:
            Dictionary representing a row from the table.
        """
        conn = self.src.connect()
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                where = ""
                params: List[Any] = []
                if self.cfg.time_window and self._has_col(table, "created_at"):
                    where = " WHERE created_at >= %s AND created_at < %s"
                    params = [self.cfg.time_window[0], self.cfg.time_window[1]]
                order = "id" if self._has_col(table, "id") else "1"
                cur.execute(f"SELECT * FROM {table}{where} ORDER BY {order}", params)
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
        """Transform a single row according to the anonymization policy.

        Args:
            table: Name of the table the row belongs to.
            row: Dictionary representing the row data.

        Returns:
            Transformed row dictionary.
        """
        out: Dict[str, Any] = {}
        if table == "entities":
            et = row.get("entity_type")
            raw_body = row.get("body")
            for col, val in row.items():
                if col == "body":
                    continue
                if self.policy.is_drop(table, col):
                    continue
                if self.policy.is_hash(table, col):
                    out[col] = self.pseudo.make(val)
                elif self.policy.is_blank(table, col):
                    out[col] = None
                else:
                    out[col] = val
            out["body"] = self.redactor.sanitize_entity_body(
                str(et) if et is not None else "", raw_body
            )
            return out

        for col, val in row.items():
            if self.policy.is_drop(table, col):
                continue
            if self.policy.is_hash(table, col):
                out[col] = self.pseudo.make(val)
            elif self.policy.is_redact(table, col):
                out[col] = self.redactor.redact(val)
            elif self.policy.is_blank(table, col):
                out[col] = None
            else:
                out[col] = val
        return out

    def _row_to_model(self, table: str, row: Dict[str, Any]) -> Any:
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
        """Copy and anonymize a table from source to destination.

        Args:
            table: Name of the table to copy.

        Returns:
            Number of rows copied.
        """
        buf: List[Any] = []
        count = 0
        t0 = time.time()

        for src_row in self._select_iter(table):

            anon_row = self._transform_row(table, src_row)
            try:
                model_obj = self._row_to_model(table, anon_row)
            except TypeError:
                allowed = set(self._insert_columns_for_table(table))
                slim = {k: v for k, v in anon_row.items() if k in allowed}
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

    def _insert_columns_for_table(self, table: str) -> List[str]:
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
                # StandardDB.reset_schema uses cfg.owner from DBConfig, not AnonymizeConfig
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
