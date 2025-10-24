import csv
import io
import logging
import re
import time
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple

import psycopg
from psycopg import errors, sql
from psycopg.types.json import Jsonb

from .models import MODEL_REGISTRY
from ..config import DBConfig
from .utils.db_sanitize import scrub_nul_deep, to_copy_text

log = logging.getLogger(__name__)

# ---------------- Identifier / schema helpers ----------------

_SCHEMANAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _maybe_set_search_path(conn: psycopg.Connection, schema: Optional[str]) -> None:
    """If a schema is provided, set search_path to 'schema, public' safely."""
    if not schema:
        return
    if not _SCHEMANAME_RE.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )


def _ident(name: str) -> sql.Identifier:
    return sql.Identifier(name)


# ---------------- Core class ----------------


class StandardDB:
    """
    Helper for creating databases, managing schemas, and bulk inserts.
    DBConfig fields referenced:
      - default_dbname, user, password, host, port, application_name, connect_timeout
      - owner (schema), template, standard_schema_path
    """

    def __init__(
        self, db_name: str, cfg: Optional[DBConfig] = None, *, initialize: bool = False
    ):
        self.db_name = db_name
        self.cfg = cfg or DBConfig()

        required = ("user", "password")
        missing = [k for k in required if not getattr(self.cfg, k, None)]
        if missing:
            raise ValueError(f"Missing required DB env vars: {', '.join(missing)}")

        self.CREATE_DB_OPTS = {
            "encoding": "UTF8",
            "owner": getattr(self.cfg, "owner", None) or None,
            "template": getattr(self.cfg, "template", None) or None,
        }

        if initialize:
            self.recreate_db_interactive(
                schema_sql_path=getattr(self.cfg, "standard_schema_path", "") or ""
            )

    # ---------------- Connections ----------------

    def _maintenance_db(self) -> str:
        return getattr(self.cfg, "default_dbname", None) or "postgres"

    def connect(self, dbname: Optional[str] = None) -> psycopg.Connection:
        # Keep options simple; application_name may contain spaces, so pass only if safe.
        appname = getattr(self.cfg, "application_name", "standarddb")
        options = f"-c application_name={appname!s}"
        return psycopg.connect(
            dbname=dbname or self._maintenance_db(),
            user=self.cfg.user,
            password=self.cfg.password,
            host=self.cfg.host,
            port=self.cfg.port,
            options=options,
            connect_timeout=getattr(self.cfg, "connect_timeout", 10),
        )

    # ---------------- Introspection ----------------

    def _db_exists(self, cur, name: str) -> bool:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (name,))
        return bool(cur.fetchone())

    # ---------------- DB lifecycle ----------------

    def init_db(self) -> None:
        log.info("Creating database %s if it doesn't exist", self.db_name)
        start = time.time()
        conn = self.connect()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                if self._db_exists(cur, self.db_name):
                    log.info(
                        "Database %s already exists; skipping create.", self.db_name
                    )
                    return

                parts = [sql.SQL("CREATE DATABASE "), _ident(self.db_name)]
                opts = []
                enc = self.CREATE_DB_OPTS.get("encoding")
                if enc:
                    opts.append(sql.SQL("ENCODING {}").format(sql.Literal(enc)))
                tmpl = self.CREATE_DB_OPTS.get("template")
                if tmpl:
                    opts.append(sql.SQL("TEMPLATE {}").format(_ident(tmpl)))
                owner = self.CREATE_DB_OPTS.get("owner")
                if owner:
                    opts.append(sql.SQL("OWNER {}").format(_ident(owner)))
                if opts:
                    parts.append(
                        sql.SQL(" WITH " + " ".join(["{}"] * len(opts))).format(*opts)
                    )

                try:
                    cur.execute(sql.Composed(parts))
                    log.info("Created database %s.", self.db_name)
                except errors.DuplicateDatabase:
                    log.info("Race: database %s already exists.", self.db_name)
        finally:
            conn.close()
            log.debug("init_db finished in %.2fs", time.time() - start)

    def reset_schema(self) -> None:
        schema = getattr(self.cfg, "owner", None)
        if not schema:
            raise ValueError("cfg.owner (schema name) is required for reset_schema().")
        log.info("Resetting schema '%s' in database %s", schema, self.db_name)
        start = time.time()
        conn = self.connect(self.db_name)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(_ident(schema))
                )
                cur.execute(sql.SQL("CREATE SCHEMA {}").format(_ident(schema)))
            log.info("Schema '%s' reset in database %s.", schema, self.db_name)
        finally:
            conn.close()
        log.info("Schema '%s' reset in %.2fs", schema, time.time() - start)

    # ---------------- Schema application ----------------

    def init_schema(self, schema_sql_path: Optional[str] = None) -> None:
        path = schema_sql_path or getattr(self.cfg, "standard_schema_path", None)
        if not path:
            raise ValueError("No schema_sql_path provided and no default in config")

        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Schema file not found: {p}")
        sql_text = p.read_text(encoding="utf-8").strip()
        if not sql_text:
            log.warning("Schema file %s is empty; nothing to do.", p)
            return

        conn = self.connect(self.db_name)
        _maybe_set_search_path(conn, getattr(self.cfg, "owner", None))
        try:
            with conn.cursor() as cur:
                cur.execute(sql_text)
            conn.commit()
            log.info("Applied schema from %s to %s.", p, self.db_name)
        except Exception as e:
            conn.rollback()
            log.error("Schema application failed: %s", e)
            raise RuntimeError(
                f"Failed applying schema {p} to {self.db_name}: {e}"
            ) from e
        finally:
            conn.close()

    def recreate_db_interactive(self, schema_sql_path: str) -> None:
        conn = self.connect()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                exists = self._db_exists(cur, self.db_name)
        finally:
            conn.close()

        if not exists:
            self.init_db()
            self.init_schema(schema_sql_path)
            return

        while True:
            ans = (
                input(
                    f"Database '{self.db_name}' already exists. "
                    f"Reset schema (S), or Abort (A)? [S/a]: "
                )
                .strip()
                .lower()
            )
            if ans in ("a", "abort"):
                log.info("Aborted. Leaving database as is.")
                raise SystemExit(0)
            if ans in ("s", "schema", ""):
                break

        self.reset_schema()
        self.init_schema(schema_sql_path)

    # ---------------- Bulk insert paths ----------------

    def bulk_insert_executemany(
        self,
        items: Sequence[Any],
        include_id: bool = False,
        on_conflict: Optional[
            str
        ] = None,  # e.g. "(model_id, account_id, created_at) DO NOTHING"
        continue_on_error: bool = False,  # SAVEPOINT per row to skip failing rows
        use_pipeline: bool = False,  # libpq pipeline to cut round-trips
    ) -> None:
        if not items:
            log.info("bulk_insert_executemany: no items to insert; returning")
            return

        first = items[0]
        model_cls = type(first)
        meta = MODEL_REGISTRY.get(model_cls, {})
        table_name: str = meta.get("table") or getattr(
            model_cls, "__table_name__", None
        )
        if not table_name:
            raise KeyError(
                f"No table mapping for {model_cls.__name__} in MODEL_REGISTRY or __table_name__"
            )

        log.info(
            "bulk_insert_executemany: inserting %d rows into %s", len(items), table_name
        )

        jsonb_fields = set(
            meta.get("jsonb_fields") or getattr(model_cls, "__jsonb_fields__", set())
        )
        cols: Tuple[str, ...] = first.insert_columns(include_id=include_id)
        col_list = sql.SQL(", ").join(map(sql.Identifier, cols))
        placeholders = (
            sql.SQL("(")
            + sql.SQL(", ").join(sql.Placeholder() * len(cols))
            + sql.SQL(")")
        )

        base = sql.SQL("INSERT INTO {} ({}) VALUES {}").format(
            _ident(table_name), col_list, placeholders
        )
        if include_id:
            base = sql.SQL(
                "INSERT INTO {} ({}) OVERRIDING SYSTEM VALUE VALUES {}"
            ).format(_ident(table_name), col_list, placeholders)
        if on_conflict:
            base = base + sql.SQL(" ON CONFLICT ") + sql.SQL(on_conflict)

        jsonb_indexes = [i for i, c in enumerate(cols) if c in jsonb_fields]

        def adapt_row(e: Any) -> Tuple[Any, ...]:
            vals = list(e.insert_values(include_id=include_id))
            for idx in jsonb_indexes:
                vals[idx] = Jsonb(vals[idx])
            return tuple(vals)

        params = [adapt_row(e) for e in items]

        conn = self.connect(self.db_name)
        _maybe_set_search_path(conn, getattr(self.cfg, "owner", None))
        start = time.time()
        try:
            if use_pipeline:
                # Note: savepoints inside pipeline mode can be tricky; prefer executemany path if possible.
                with conn.pipeline():
                    with conn.cursor() as cur:
                        if continue_on_error:
                            for row in params:
                                cur.execute("SAVEPOINT sp")
                                try:
                                    cur.execute(base, row)
                                    cur.execute("RELEASE SAVEPOINT sp")
                                except Exception:
                                    cur.execute("ROLLBACK TO SAVEPOINT sp")
                        else:
                            cur.executemany(base, params)
            else:
                with conn.cursor() as cur:
                    if continue_on_error:
                        for row in params:
                            cur.execute("SAVEPOINT sp")
                            try:
                                cur.execute(base, row)
                                cur.execute("RELEASE SAVEPOINT sp")
                            except Exception:
                                cur.execute("ROLLBACK TO SAVEPOINT sp")
                    else:
                        cur.executemany(base, params)
            conn.commit()
            log.info(
                "bulk_insert_executemany committed %d rows into %s in %.2fs",
                len(items),
                table_name,
                time.time() - start,
            )
        except Exception as e:
            conn.rollback()
            log.error("bulk_insert_executemany failed on %s: %s", table_name, e)
            raise
        finally:
            conn.close()

    def bulk_insert_multi_values(
        self,
        items: Sequence[Any],
        include_id: bool = False,
        on_conflict: Optional[str] = None,
        chunk_size: int = 5000,
    ) -> None:
        if not items:
            log.info("bulk_insert_multi_values: no items to insert; returning")
            return

        first = items[0]
        model_cls = type(first)
        meta = MODEL_REGISTRY.get(model_cls, {})
        table_name: str = meta.get("table") or getattr(
            model_cls, "__table_name__", None
        )
        if not table_name:
            raise KeyError(
                f"No table mapping for {model_cls.__name__} in MODEL_REGISTRY or __table_name__"
            )

        jsonb_fields = set(
            meta.get("jsonb_fields") or getattr(model_cls, "__jsonb_fields__", set())
        )
        cols: Tuple[str, ...] = first.insert_columns(include_id=include_id)
        num_cols = len(cols)

        MAX_PARAMS = 65000
        max_rows_by_params = max(1, MAX_PARAMS // max(1, num_cols))
        eff_chunk = min(chunk_size, max_rows_by_params)

        log.info(
            "bulk_insert_multi_values: inserting %d rows into %s (requested chunk=%d, "
            "columns/row=%d, effective chunk=%d, max_rows_by_params=%d)",
            len(items),
            table_name,
            chunk_size,
            num_cols,
            eff_chunk,
            max_rows_by_params,
        )

        col_list = sql.SQL(", ").join(map(sql.Identifier, cols))
        jsonb_indexes = [i for i, c in enumerate(cols) if c in jsonb_fields]

        def adapt_row(e: Any) -> Tuple[Any, ...]:
            vals = list(e.insert_values(include_id=include_id))
            for idx in jsonb_indexes:
                vals[idx] = Jsonb(vals[idx])
            return tuple(vals)

        conn = self.connect(self.db_name)
        _maybe_set_search_path(conn, getattr(self.cfg, "owner", None))
        start = time.time()
        try:
            with conn.cursor() as cur:
                for i in range(0, len(items), eff_chunk):
                    chunk = items[i : i + eff_chunk]
                    adapted = [adapt_row(e) for e in chunk]

                    row_tpl = (
                        sql.SQL("(")
                        + sql.SQL(", ").join(sql.Placeholder() * num_cols)
                        + sql.SQL(")")
                    )
                    values_clause = sql.SQL(", ").join(row_tpl for _ in chunk)

                    if include_id:
                        base = sql.SQL(
                            "INSERT INTO {} ({}) OVERRIDING SYSTEM VALUE VALUES {}"
                        ).format(_ident(table_name), col_list, values_clause)
                    else:
                        base = sql.SQL("INSERT INTO {} ({}) VALUES {}").format(
                            _ident(table_name), col_list, values_clause
                        )

                    if on_conflict:
                        base = base + sql.SQL(" ON CONFLICT ") + sql.SQL(on_conflict)

                    flat_params: list[Any] = []
                    for row in adapted:
                        flat_params.extend(row)

                    cur.execute(base, flat_params)

            conn.commit()
            log.info(
                "bulk_insert_multi_values committed %d rows into %s in %.2fs",
                len(items),
                table_name,
                time.time() - start,
            )
        except Exception as e:
            conn.rollback()
            log.error("bulk_insert_multi_values failed on %s: %s", table_name, e)
            raise
        finally:
            conn.close()

    def bulk_copy_insert(
        self,
        items: Sequence[Any],
        include_id: bool = False,
        on_conflict: Optional[
            str
        ] = None,  # e.g. "(account_id) DO NOTHING" or "... DO UPDATE ..."
        temp_table: bool = True,  # kept for API compatibility
        csv_null: str = r"\N",  # NULL representation for COPY CSV
    ) -> None:
        """
        Fast path using COPY. With on_conflict=None, copies directly into target.
        Otherwise: COPY -> TEMP TABLE -> INSERT ... ON CONFLICT ... -> DROP TEMP.
        """
        if not items:
            log.info("bulk_copy_insert: no items; returning")
            return

        first = items[0]
        model_cls = type(first)
        meta = MODEL_REGISTRY.get(model_cls, {})
        table_name: str = meta.get("table") or getattr(
            model_cls, "__table_name__", None
        )
        if not table_name:
            raise KeyError(f"No table mapping for {model_cls.__name__}")

        cols: Tuple[str, ...] = first.insert_columns(include_id=include_id)
        col_list = sql.SQL(", ").join(map(sql.Identifier, cols))

        # Build CSV buffer
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        for e in items:
            vals = list(e.insert_values(include_id=include_id))
            row = []
            for v in vals:
                s = to_copy_text(v)
                row.append(
                    csv_null if s is None else s
                )  # tell COPY what NULL looks like
            writer.writerow(row)
        buf.seek(0)

        conn = self.connect(self.db_name)
        _maybe_set_search_path(conn, getattr(self.cfg, "owner", None))
        try:
            with conn.cursor() as cur:
                if on_conflict:
                    tmp = f"tmp_{table_name}_{int(time.time())}_{id(items) % 10000}"
                    cur.execute(
                        sql.SQL(
                            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS)"
                        ).format(_ident(tmp), _ident(table_name))
                    )

                    copy_sql = sql.SQL(
                        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL {})"
                    ).format(_ident(tmp), col_list, sql.Literal(csv_null))
                    cur.copy(copy_sql, buf)

                    insert_sql = (
                        sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                            _ident(table_name), col_list, col_list, _ident(tmp)
                        )
                        + sql.SQL(" ON CONFLICT ")
                        + sql.SQL(on_conflict)
                    )
                    cur.execute(insert_sql)
                    cur.execute(sql.SQL("DROP TABLE {}").format(_ident(tmp)))
                else:
                    copy_sql = sql.SQL(
                        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL {})"
                    ).format(_ident(table_name), col_list, sql.Literal(csv_null))
                    cur.copy(copy_sql, buf)

            conn.commit()
            log.info(
                "bulk_copy_insert: copied %d rows into %s (on_conflict=%s)",
                len(items),
                table_name,
                bool(on_conflict),
            )
        except Exception as e:
            conn.rollback()
            log.error("bulk_copy_insert failed on %s: %s", table_name, e)
            raise
        finally:
            conn.close()

    def insert_with_fallbacks(
        self,
        items: Sequence[Any],
        *,
        include_id: bool = False,
        on_conflict: Optional[str] = None,
        chunk_size: int = 5000,
    ) -> None:
        """
        Best-effort load:
          1) COPY (fastest)
          2) Multi-VALUES
          3) Row-by-row with SAVEPOINT to isolate bad rows
        """
        if not items:
            return

        first = items[0]
        model_cls = type(first)
        meta = MODEL_REGISTRY.get(model_cls, {})
        table_name: str = meta.get("table") or getattr(
            model_cls, "__table_name__", None
        )
        if not table_name:
            raise KeyError(f"No table mapping for {model_cls.__name__}")

        # Resolve columns once
        cols: Tuple[str, ...] = first.insert_columns(include_id=include_id)

        # 1) COPY via temp chunks
        try:
            for i in range(0, len(items), chunk_size):
                self._copy_via_temp(
                    items[i : i + chunk_size],
                    table_name=table_name,
                    cols=cols,
                    on_conflict=on_conflict,
                    chunk_size=chunk_size,
                    include_id=include_id,
                )
            return
        except Exception as e:
            log.warning(
                "COPY failed on %s (%d rows). Falling back to VALUES. Error: %s",
                table_name,
                len(items),
                e,
            )

        # 2) Multi-VALUES
        try:
            self.bulk_insert_multi_values(
                items,
                include_id=include_id,
                on_conflict=on_conflict,
                chunk_size=chunk_size,
            )
            return
        except Exception as e:
            log.warning(
                "VALUES bulk insert failed on %s. Falling back to row-by-row. Error: %s",
                table_name,
                e,
            )

        # 3) Row-by-row with SAVEPOINT and NUL scrubbing
        bad = 0
        ok = 0
        conn = self.connect(self.db_name)
        _maybe_set_search_path(conn, getattr(self.cfg, "owner", None))
        try:
            col_list = sql.SQL(", ").join(map(sql.Identifier, cols))
            placeholders = (
                sql.SQL("(")
                + sql.SQL(", ").join(sql.Placeholder() * len(cols))
                + sql.SQL(")")
            )

            base = sql.SQL("INSERT INTO {} ({}) VALUES {}").format(
                _ident(table_name), col_list, placeholders
            )
            if include_id:
                base = sql.SQL(
                    "INSERT INTO {} ({}) OVERRIDING SYSTEM VALUE VALUES {}"
                ).format(_ident(table_name), col_list, placeholders)
            if on_conflict:
                base = base + sql.SQL(" ON CONFLICT ") + sql.SQL(on_conflict)

            jsonb_fields = set(
                meta.get("jsonb_fields")
                or getattr(model_cls, "__jsonb_fields__", set())
            )
            jsonb_idx = [i for i, c in enumerate(cols) if c in jsonb_fields]

            def adapt_row(e: Any) -> Tuple[Any, ...]:
                vals = list(e.insert_values(include_id=include_id))
                # deep NUL scrub on serializable types
                for i, v in enumerate(vals):
                    if isinstance(v, (str, bytes, dict, list, tuple, set, Mapping)):
                        vals[i] = scrub_nul_deep(v)
                for j in jsonb_idx:
                    vals[j] = Jsonb(vals[j])
                return tuple(vals)

            with conn.cursor() as cur:
                for idx, e in enumerate(items):
                    row = adapt_row(e)
                    cur.execute("SAVEPOINT sp")
                    try:
                        cur.execute(base, row)
                        cur.execute("RELEASE SAVEPOINT sp")
                        ok += 1
                    except Exception as ex:
                        cur.execute("ROLLBACK TO SAVEPOINT sp")
                        bad += 1
                        try:
                            preview = getattr(e, "insert_values")(include_id)
                        except Exception:
                            preview = "<unpreviewable>"
                        log.error(
                            "Row %d failed for %s: %s | payload=%r",
                            idx,
                            table_name,
                            ex,
                            preview,
                        )

            conn.commit()
            log.warning(
                "Row-by-row finished for %s: inserted=%d, failed=%d",
                table_name,
                ok,
                bad,
            )
        except Exception as e:
            conn.rollback()
            log.error("Row-by-row fallback also failed on %s: %s", table_name, e)
            raise
        finally:
            conn.close()

    # ---------------- Internal COPY helper ----------------

    def _copy_via_temp(
        self,
        items: Sequence[Any],
        *,
        table_name: str,
        cols: Tuple[str, ...],
        on_conflict: Optional[str],
        chunk_size: int,  # kept for signature symmetry
        include_id: bool = False,
    ) -> None:
        if not items:
            return

        # Build a CSV buffer for this chunk
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")

        def rowvals(e):
            vals = list(e.insert_values(include_id=include_id))
            # Scrub NULs everywhere; JSONB is passed as text in COPY (server casts)
            for i, v in enumerate(vals):
                if isinstance(v, (str, bytes, dict, list, tuple, set, Mapping)):
                    vals[i] = scrub_nul_deep(v)
            # Represent None as empty; we’ll specify NULL '' to COPY
            return ["" if v is None else to_copy_text(v) for v in vals]

        for e in items:
            w.writerow(rowvals(e))
        buf.seek(0)

        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
        csv_null = ""  # empty string => NULL
        copy_stmt = sql.SQL(
            "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL {})"
        ).format(_ident(table_name), col_list, sql.Literal(csv_null))

        tmp_name = f"tmp_{table_name}_{int(time.time())}_{id(items) % 10000}"

        conn = self.connect(self.db_name)
        _maybe_set_search_path(conn, getattr(self.cfg, "owner", None))
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
                        _ident(tmp_name), _ident(table_name)
                    )
                )

                with cur.copy(copy_stmt) as cp:
                    cp.write(buf.read())

                insert_stmt = sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                    _ident(table_name), col_list, col_list, _ident(tmp_name)
                )
                if on_conflict:
                    insert_stmt = (
                        insert_stmt + sql.SQL(" ON CONFLICT ") + sql.SQL(on_conflict)
                    )
                cur.execute(insert_stmt)

                cur.execute(sql.SQL("DROP TABLE {}").format(_ident(tmp_name)))

            conn.commit()
            log.info("COPY->INSERT staged %d rows into %s", len(items), table_name)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
