from __future__ import annotations
import dataclasses
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)


# =============================================================
# Data classes
# =============================================================
@dataclass
class ColStat:
    """Statistics for a single column.

    Attributes:
        data_type: The data type of the column.
        completeness: Fraction of non-null values (0..1), or None if not computable.
        enum_counts: List of (val, count, pct) tuples for enum values, if applicable.
    """

    data_type: str
    completeness: Optional[float]
    enum_counts: Optional[List[Tuple[str, int, float]]] = None


@dataclass
class TableStat:
    """Statistics for a table: estimated rows and per-column stats.

    Attributes:
        est_rows: Estimated number of rows in the table.
        columns: Dictionary mapping column names to ColStat objects.
        extra: Optional extra payload for table-specific stats
               (e.g., actions links per action_type).
        is_estimated: True if these stats came from TABLESAMPLE-based
            sampling (Inspector(sample_pct=...)) rather than an exact
            full-table scan -- completeness/enum fractions are still
            statistically representative, but absolute counts are
            extrapolated, not exact.
    """

    est_rows: int
    columns: Dict[str, ColStat]
    extra: Optional[Dict[str, Any]] = None
    is_estimated: bool = False


# =============================================================
# Coloring & ANSI helpers
# =============================================================

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _color_pct(p: Optional[float]) -> str:
    """Colorize a percentage (0..1)."""
    if p is None:
        return f"{Style.DIM}n/a{Style.RESET_ALL}"
    pct = p * 100.0
    txt = f"{pct:.1f}%"
    if pct >= 80:
        return f"{Fore.GREEN}{txt}{Style.RESET_ALL}"
    if pct >= 20:
        return f"{Fore.YELLOW}{txt}{Style.RESET_ALL}"
    return f"{Fore.RED}{txt}{Style.RESET_ALL}"


def _color_table(name: str) -> str:
    """Colorize table name."""
    return f"{Fore.CYAN}{name}{Style.RESET_ALL}"


def _dim(s: str) -> str:
    """Dim the string."""
    return f"{Style.DIM}{s}{Style.RESET_ALL}"


def _vislen(s: str) -> int:
    """Visible length (strip ANSI codes)."""
    return len(ANSI_RE.sub("", s))


def _rpad_ansi(s: str, width: int) -> str:
    """Right-pad string with spaces, accounting for ANSI codes."""
    return s + " " * max(0, width - _vislen(s))


def _lpad_ansi(s: str, width: int) -> str:
    """Left-pad string with spaces, accounting for ANSI codes."""
    return " " * max(0, width - _vislen(s)) + s


# =============================================================
# SQL identifier helpers
# =============================================================


def _norm_tbl_name(name: str) -> str:
    """Normalize table identifier (strip schema, lowercase)."""
    if not name:
        return ""
    s = str(name).strip().strip('"').lower()
    return s.rsplit(".", 1)[-1]


def psql_ident(ident: str) -> str:
    """Quote a PostgreSQL identifier."""
    return '"' + ident.replace('"', '""') + '"'


def psql_ident_full(schema: str, table: str) -> str:
    """Quote a full PostgreSQL identifier (schema.table)."""
    return f"{psql_ident(schema)}.{psql_ident(table)}"


# =============================================================
# Inspector class
# =============================================================


class Inspector:
    """Database schema/data inspector with completeness & enum stats."""

    def __init__(
        self,
        db,
        schema: str,
        *,
        max_enum_items: int = 8,
        sample_pct: Optional[float] = None,
        time_window: Optional[Tuple[Any, Any]] = None,
    ):
        """Initialize the Inspector.

        Args:
            db: Database connection or handler.
            schema: Schema name to inspect.
            max_enum_items: Maximum number of enum items to collect stats for.
            sample_pct: If set, completeness/enum-distribution stats are
                computed from a `TABLESAMPLE SYSTEM (sample_pct)` sample of
                each table instead of an exact full-table scan, and row
                counts use the database's own cached estimate
                (pg_stat_user_tables/pg_class) instead of an exact COUNT(*).
                Trades a small amount of accuracy for a large speedup on big
                tables (measured: a full exact snapshot() of a 64M-row table
                took 18.4s; sampling avoids scanning the whole table at all).
                None (default) preserves the original exact, full-scan
                behavior -- existing callers are unaffected.
            time_window: Optional (start, end) restricting every query to
                `created_at >= start AND created_at < end`, for tables that
                have a `created_at` column (same convention as
                `PseudonymizeConfig.time_window`). Combine with
                `report_schemas([...])`'s existing multi-inspector
                comparison to compare the *same* time slice across several
                databases -- one Inspector per database, same time_window,
                passed together. Chunk exclusion on the hypertable's time
                dimension keeps this cheap even in exact mode, since a
                narrow window only touches a handful of chunks regardless
                of total table size.
        """
        self.db = db
        self.schema = schema
        self.max_enum_items = max_enum_items
        self.sample_pct = sample_pct
        self.time_window = time_window

    # ---------------------------------------------------------
    # Snapshot of schema stats
    # ---------------------------------------------------------
    def snapshot(
        self, *, only_tables: Optional[List[str]] = None
    ) -> Dict[str, TableStat]:
        """Collect statistics for all tables in schema (optionally filtered)."""
        allow = (
            {_norm_tbl_name(t) for t in only_tables if t and str(t).strip()}
            if only_tables
            else None
        )
        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.relname,
                           COALESCE(s.n_live_tup, NULLIF(c.reltuples::bigint, 0)) AS est_rows
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                    WHERE n.nspname = %s AND c.relkind IN ('r','p')
                    ORDER BY 1
                    """,
                    (self.schema,),
                )
                tables = cur.fetchall()

                # pg_stat_user_tables/reltuples are 0 for a TimescaleDB
                # hypertable's parent relation -- all rows live in per-chunk
                # child tables, so the parent itself is a shell with no
                # tracked stats of its own (measured: entities showed
                # n_live_tup=0 despite 64M real rows). TimescaleDB's own
                # approximate_row_count() sums the per-chunk estimates
                # correctly and also works on plain (non-hypertable) tables,
                # so it's what sampled mode uses for its cheap row estimate.
                # Checked once via pg_proc (a catalog lookup, not a function
                # call) so a missing extension can't abort the transaction.
                # Not useful when time_window is set -- approximate_row_count()
                # counts the whole hypertable and can't be filtered, so a
                # (possibly sampled) filtered COUNT(*) is the only number
                # available either way; skip the extra query.
                has_approx_row_count = False
                if self.sample_pct is not None and self.time_window is None:
                    cur.execute(
                        "SELECT 1 FROM pg_proc WHERE proname = 'approximate_row_count'"
                    )
                    has_approx_row_count = cur.fetchone() is not None

                out: Dict[str, TableStat] = {}
                for tname, est_rows in tables:
                    if allow and _norm_tbl_name(tname) not in allow:
                        continue

                    if has_approx_row_count:
                        cur.execute(
                            "SELECT approximate_row_count(%s)",
                            (psql_ident_full(self.schema, tname),),
                        )
                        row = cur.fetchone()
                        if row and row[0]:
                            est_rows = row[0]

                    # columns
                    cur.execute(
                        """
                        SELECT a.attname,
                               pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                               t.typtype, a.atttypid
                        FROM pg_attribute a
                        JOIN pg_class c ON c.oid = a.attrelid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        JOIN pg_type t ON t.oid = a.atttypid
                        WHERE n.nspname = %s AND c.relname = %s
                          AND a.attnum > 0 AND NOT a.attisdropped
                        ORDER BY a.attnum
                        """,
                        (self.schema, tname),
                    )
                    cols = cur.fetchall()
                    time_where, time_params = self._time_where(cols)

                    cstats: Dict[str, ColStat] = {}
                    total: Optional[int] = None  # exact count, or sample count (denominator for fractions only)
                    extra: Optional[Dict[str, Any]] = None
                    display_rows = est_rows or -1  # what we'll report as est_rows

                    if cols:
                        parts = ["COUNT(*) AS total"]
                        for col, *_ in cols:
                            alias = f"nn_{_norm_tbl_name(col)}"
                            parts.append(f"COUNT({psql_ident(col)}) AS {alias}")
                        select_sql = f"SELECT {', '.join(parts)} FROM {psql_ident_full(self.schema, tname)}"

                        if self.sample_pct is None:
                            cur.execute(select_sql + time_where, time_params)
                            row = cur.fetchone()
                            total = int(row[0] or 0)
                            # Exact count is always authoritative -- except
                            # approximate_row_count() can't apply time_where,
                            # so a filtered count is the only number we have
                            # either way; chunk exclusion keeps it cheap.
                            display_rows = total
                        else:
                            row = None
                            for pct in self._escalating_pcts(self.sample_pct):
                                cur.execute(
                                    f"{select_sql} TABLESAMPLE SYSTEM ({pct}){time_where}",
                                    time_params,
                                )
                                row = cur.fetchone()
                                if row and row[0]:
                                    break
                            total = int(row[0] or 0) if row else 0
                            if self.time_window:
                                # approximate_row_count() can't be filtered by
                                # time_window, so there's no cheaper number
                                # than the (sampled) filtered count itself.
                                display_rows = total

                        for i, (col, typ, typtype, atttypid) in enumerate(
                            cols, start=1
                        ):
                            nn = int(row[i] or 0) if row else 0
                            comp = None if not total else (nn / total)
                            cstats[col] = ColStat(data_type=str(typ), completeness=comp)

                            # enum sub-stats
                            if self._is_enum_or_domain_enum(cur, typtype, atttypid):
                                cstats[col].enum_counts = self._value_counts(
                                    cur, tname, col, display_rows,
                                    limit=self.max_enum_items,
                                    time_where=time_where, time_params=time_params,
                                )

                        # Table-specific extras: actions link stats per action_type
                        if _norm_tbl_name(tname) == "actions":
                            extra = self._actions_link_stats(
                                cur, tname, time_where=time_where, time_params=time_params
                            )

                    out[tname] = TableStat(
                        est_rows=int(display_rows if display_rows else -1),
                        columns=cstats,
                        extra=extra,
                        is_estimated=self.sample_pct is not None,
                    )

                if allow:
                    out = {k: v for k, v in out.items() if _norm_tbl_name(k) in allow}
                return out
        finally:
            conn.close()

    def snapshot_and_save(
        self, path: Union[str, Path], *, only_tables: Optional[List[str]] = None
    ) -> Dict[str, TableStat]:
        """Convenience: snapshot() then save_snapshot() in one call, for
        capturing a timestamped record of this database's stats to compare
        against later (see load_snapshot() to read it back).

        Args:
            path: File path to write the JSON snapshot to.
            only_tables: Optional table filter, same as snapshot().

        Returns:
            The same Dict[str, TableStat] snapshot() would return.
        """
        snap = self.snapshot(only_tables=only_tables)
        save_snapshot(self, snap, path)
        return snap

    # ---------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------
    def _is_enum_or_domain_enum(self, cur, typtype: str, atttypid: int) -> bool:
        """Check if a type is an enum or a domain over an enum.

        Args:
            cur: Database cursor.
            typtype: Type type code.
            atttypid: Attribute type ID.

        Returns:
            True if the type is an enum or domain over enum, False otherwise.
        """
        if typtype == "e":  # enum
            return True
        if typtype == "d":  # domain -> base type
            cur.execute(
                "SELECT bt.typtype FROM pg_type t "
                "JOIN pg_type bt ON bt.oid = t.typbasetype "
                "WHERE t.oid = %s",
                (atttypid,),
            )
            row = cur.fetchone()
            return bool(row and row[0] == "e")
        return False

    @staticmethod
    def _escalating_pcts(start_pct: float) -> List[float]:
        """1% -> 10% -> 100% (a full scan). TABLESAMPLE SYSTEM samples by
        page, so a narrow starting percentage can come back completely
        empty for a small table or a low-cardinality filter; only escalate
        if the narrower one actually came back empty."""
        pcts = [start_pct]
        if start_pct < 10:
            pcts.append(10.0)
        if start_pct < 100:
            pcts.append(100.0)
        return pcts

    def _time_where(self, cols) -> Tuple[str, List[Any]]:
        """WHERE clause restricting to self.time_window, if set and this
        table actually has a created_at column (same convention as
        PseudonymizeConfig.time_window) -- empty string/params otherwise, a
        no-op when appended to any query."""
        if not self.time_window:
            return "", []
        if not any(col == "created_at" for col, *_ in cols):
            return "", []
        return " WHERE created_at >= %s AND created_at < %s", list(self.time_window)

    def _value_counts(
        self,
        cur,
        table: str,
        column: str,
        total_rows: int,
        *,
        limit: int = 8,
        time_where: str = "",
        time_params: Optional[List[Any]] = None,
    ) -> List[Tuple[str, int, float]]:
        """Get value counts for a column.

        Args:
            cur: Database cursor.
            table: Table name.
            column: Column name.
            total_rows: Total rows in the table (the cheap estimate when
                self.sample_pct is set, exact otherwise -- used as the
                scaling basis for estimated counts in sampled mode).
            limit: Maximum number of values to return.
            time_where: Optional WHERE clause fragment from _time_where().
            time_params: Params for time_where.

        Returns:
            List of (value, count, percentage) tuples. In sampled mode,
            `count` is extrapolated from the sample (percentage is the
            direct, more reliable estimate); in exact mode both are exact.
        """
        if total_rows <= 0:
            return []
        time_params = time_params or []
        base_sql = (
            f"SELECT {psql_ident(column)}::text AS val, COUNT(*) AS cnt "
            f"FROM {psql_ident_full(self.schema, table)}"
        )
        group_sql = " GROUP BY 1 ORDER BY cnt DESC, val ASC LIMIT %s"

        if self.sample_pct is None:
            cur.execute(base_sql + time_where + group_sql, (*time_params, max(1, limit)))
            rows = cur.fetchall()
            top = [
                (("NULL" if v is None else v), int(c), int(c) / total_rows)
                for v, c in rows
            ]
            shown = sum(c for _v, c, _p in top)
            if shown < total_rows:
                other = total_rows - shown
                top.append(("other", other, other / total_rows))
            return top

        # Sampled mode: percentages come directly from the sample (a
        # reliable estimate); displayed counts are extrapolated onto
        # total_rows (the cheap row-count estimate) so they read
        # consistently with the rest of the report, not as tiny raw sample
        # counts next to a big percentage.
        rows: List[Tuple[Optional[str], int]] = []
        for pct in self._escalating_pcts(self.sample_pct):
            cur.execute(
                f"{base_sql} TABLESAMPLE SYSTEM ({pct}){time_where}{group_sql}",
                (*time_params, max(1, limit)),
            )
            rows = cur.fetchall()
            if rows:
                break
        if not rows:
            return []

        sample_total = sum(int(c) for _v, c in rows)
        if sample_total == 0:
            return []
        top = []
        for v, c in rows:
            frac = int(c) / sample_total
            top.append((("NULL" if v is None else v), round(frac * total_rows), frac))
        shown_frac = sum(p for _v, _c, p in top)
        if shown_frac < 1.0:
            other_frac = 1.0 - shown_frac
            top.append(("other", round(other_frac * total_rows), other_frac))
        return top

    # -------- actions-specific helper --------------------------------
    def _actions_link_stats(
        self,
        cur,
        table: str,
        *,
        time_where: str = "",
        time_params: Optional[List[Any]] = None,
    ) -> Dict[str, Any]:
        """Compute per-action_type completeness statistics for the actions table.

        Computes completeness for: target_account_id, target_post_id,
        originator_account_id, originator_post_id, originator_community_id,
        target_community_id.

        Args:
            cur: Database cursor.
            table: Table name.
            time_where: Optional WHERE clause fragment from _time_where().
            time_params: Params for time_where.

        Returns:
            Dictionary containing 'actions_links_per_type' list. In sampled
            mode (self.sample_pct set), each group's `nn`/`total` counts are
            the raw counts *from the sample* (not extrapolated -- there's no
            cheap per-action_type row-count estimate to scale onto, unlike
            the table-level stats above), so they'll read smaller than the
            rest of the report; `pct` is still a reliable estimate either
            way, since nn and total both come from the same sampled rows.
        """
        time_params = time_params or []
        base_sql = f"""
            SELECT
                action_type::text AS action_type,
                COUNT(*) AS total,
                COUNT(target_account_id)      AS nn_target_account_id,
                COUNT(target_post_id)         AS nn_target_post_id,
                COUNT(originator_account_id)  AS nn_originator_account_id,
                COUNT(originator_post_id)     AS nn_originator_post_id,
                COUNT(originator_community_id) AS nn_originator_community_id,
                COUNT(target_community_id)    AS nn_target_community_id
            FROM {psql_ident_full(self.schema, table)}
        """
        group_sql = " GROUP BY 1 ORDER BY 1"

        if self.sample_pct is None:
            cur.execute(base_sql + time_where + group_sql, time_params)
            rows = cur.fetchall()
        else:
            rows = []
            for pct in self._escalating_pcts(self.sample_pct):
                cur.execute(
                    f"{base_sql} TABLESAMPLE SYSTEM ({pct}){time_where}{group_sql}",
                    time_params,
                )
                rows = cur.fetchall()
                if rows:
                    break
        per_type: List[Dict[str, Any]] = []
        for (
            action_type,
            total,
            nn_tacc,
            nn_tpost,
            nn_oacc,
            nn_opost,
            nn_ocomm,
            nn_tcomm,
        ) in rows:
            total = int(total or 0)

            def _pct(nn: int) -> Optional[float]:
                return None if total == 0 else nn / total

            nn_tacc = int(nn_tacc or 0)
            nn_tpost = int(nn_tpost or 0)
            nn_oacc = int(nn_oacc or 0)
            nn_opost = int(nn_opost or 0)
            nn_ocomm = int(nn_ocomm or 0)
            nn_tcomm = int(nn_tcomm or 0)

            per_type.append(
                {
                    "action_type": action_type,
                    "total": total,
                    "nn": {
                        "target_account_id": nn_tacc,
                        "target_post_id": nn_tpost,
                        "originator_account_id": nn_oacc,
                        "originator_post_id": nn_opost,
                        "originator_community_id": nn_ocomm,
                        "target_community_id": nn_tcomm,
                    },
                    "pct": {
                        "target_account_id": _pct(nn_tacc),
                        "target_post_id": _pct(nn_tpost),
                        "originator_account_id": _pct(nn_oacc),
                        "originator_post_id": _pct(nn_opost),
                        "originator_community_id": _pct(nn_ocomm),
                        "target_community_id": _pct(nn_tcomm),
                    },
                }
            )

        return {"actions_links_per_type": per_type}


# =============================================================
# Structured output (save/load snapshots as JSON)
# =============================================================


def snapshot_to_dict(
    inspector: "Inspector", snap: Dict[str, TableStat]
) -> Dict[str, Any]:
    """Serialize a snapshot (as returned by Inspector.snapshot()) into a
    JSON-serializable dict, alongside the metadata needed to make sense of
    it later: when it was taken, which database/schema, and whether it was
    exact or sampled. This is the envelope save_snapshot()/load_snapshot()
    round-trip; report_schemas() still works only on live Inspector
    instances, this is for saving results and comparing across runs.

    Args:
        inspector: The Inspector that produced `snap` (for its metadata).
        snap: A snapshot dict, e.g. `inspector.snapshot()`.

    Returns:
        A JSON-serializable dict: {timestamp, db_name, schema, sample_pct,
        time_window, tables: {table_name: {...TableStat fields...}}}.
    """
    return {
        "timestamp": time.time(),
        "db_name": getattr(inspector.db, "db_name", None),
        "schema": inspector.schema,
        "sample_pct": inspector.sample_pct,
        "time_window": (
            [str(t) for t in inspector.time_window] if inspector.time_window else None
        ),
        "tables": {
            tname: dataclasses.asdict(tstat) for tname, tstat in snap.items()
        },
    }


def save_snapshot(
    inspector: "Inspector", snap: Dict[str, TableStat], path: Union[str, Path]
) -> None:
    """Save a snapshot to `path` as JSON. See snapshot_to_dict() for the
    envelope shape.

    Args:
        inspector: The Inspector that produced `snap` (for its metadata).
        snap: A snapshot dict, e.g. `inspector.snapshot()`.
        path: File path to write to (parent directory must already exist).
    """
    data = snapshot_to_dict(inspector, snap)
    Path(path).write_text(json.dumps(data, indent=2, default=str))


def _tablestat_from_dict(d: Dict[str, Any]) -> TableStat:
    columns = {
        cname: ColStat(
            data_type=cd["data_type"],
            completeness=cd["completeness"],
            enum_counts=(
                [tuple(item) for item in cd["enum_counts"]]
                if cd.get("enum_counts")
                else None
            ),
        )
        for cname, cd in d.get("columns", {}).items()
    }
    return TableStat(
        est_rows=d["est_rows"],
        columns=columns,
        extra=d.get("extra"),
        is_estimated=d.get("is_estimated", False),
    )


def load_snapshot(
    path: Union[str, Path]
) -> Tuple[Dict[str, Any], Dict[str, TableStat]]:
    """Load a snapshot previously written by save_snapshot().

    Args:
        path: File path to read.

    Returns:
        (metadata, tables) -- metadata is the envelope's timestamp/db_name/
        schema/sample_pct/time_window; tables is a Dict[str, TableStat],
        reconstructed so it's usable exactly like a live
        `Inspector.snapshot()` result (e.g. `tables["posts"].est_rows`).
    """
    raw = json.loads(Path(path).read_text())
    tables = {
        tname: _tablestat_from_dict(tdict)
        for tname, tdict in raw.get("tables", {}).items()
    }
    metadata = {k: v for k, v in raw.items() if k != "tables"}
    return metadata, tables


_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def _default_snapshot_filename(inspector: "Inspector") -> str:
    """Build a filesystem-safe file name that corresponds to one inspector,
    for report_schemas()'s default auto-save. Encodes enough of the
    inspector's identity (db, schema, time_window, sample_pct) that two
    different inspectors don't collide on the same file."""
    db_name = getattr(inspector.db, "db_name", None) or "unknown_db"
    parts = [str(db_name), inspector.schema]
    if inspector.time_window:
        parts.append(f"{inspector.time_window[0]}_{inspector.time_window[1]}")
    if inspector.sample_pct is not None:
        parts.append(f"sampled{inspector.sample_pct}pct")
    safe = _UNSAFE_FILENAME_CHARS.sub("_", "__".join(parts))
    return f"{safe}.json"


# =============================================================
# Reporting
# =============================================================


def report_schemas(
    inspectors: List["Inspector"],
    *,
    only_tables: Optional[List[str]] = None,
    show_counts: bool = True,
    save: bool = True,
    save_dir: Union[str, Path] = "inspector_snapshots",
) -> None:
    """Generate and print a schema report.

    Args:
        inspectors: List of Inspector instances.
        only_tables: Optional list of tables to include.
        show_counts: Whether to show counts in enum stats (completeness always shows nn/total).
        save: If True (the default), also write each inspector's snapshot to
            a JSON file under `save_dir` -- one file per inspector, named
            after its db/schema/time_window/sample_pct (see
            _default_snapshot_filename()). Set to False to only print.
        save_dir: Directory the JSON snapshots are written to (created if
            missing). Ignored if save=False.
    """
    allow = {_norm_tbl_name(t) for t in only_tables} if only_tables else None

    # Collect snapshots and labels
    snaps: List[Dict[str, TableStat]] = []
    labels: List[str] = []
    if save:
        Path(save_dir).mkdir(parents=True, exist_ok=True)
    for ins in inspectors:
        snap = ins.snapshot(only_tables=only_tables)
        snaps.append(snap)
        label = f"{ins.db.db_name}:{ins.schema}"
        if ins.time_window:
            label += f" [{ins.time_window[0]}..{ins.time_window[1]}]"
        labels.append(label)
        if save:
            out_path = Path(save_dir) / _default_snapshot_filename(ins)
            save_snapshot(ins, snap, out_path)
            print(_dim(f"[inspector] Saved snapshot: {out_path}"))

    # -------- Tables to show (preserve user order if provided) --------
    norm_to_actual: Dict[str, str] = {}
    for snap in snaps:
        for t in snap.keys():
            n = _norm_tbl_name(t)
            if n and n not in norm_to_actual:
                norm_to_actual[n] = t

    if only_tables:
        want_norm = [_norm_tbl_name(t) for t in only_tables if t and str(t).strip()]
        seen = set()
        all_tables: List[str] = []
        for n in want_norm:
            if n in seen:
                continue
            seen.add(n)
            if n in norm_to_actual:
                all_tables.append(norm_to_actual[n])
        if not all_tables:
            print(
                f"[inspector] No tables matched across databases: "
                + ", ".join(only_tables)
            )
            return
    else:
        all_tables = sorted({t for snap in snaps for t in snap.keys()})

    ACTION_ID_COLS = [
        "originator_account_id",
        "originator_post_id",
        "target_account_id",
        "target_post_id",
        "originator_community_id",
        "target_community_id",
    ]

    # -------- Small helpers (rendering) --------
    def _enum_cell(cnt: Optional[int], pct: Optional[float]) -> str:
        """Format an enum cell with count and percentage."""
        if pct is None:
            return _dim("—")
        pct_s = _color_pct(pct)
        return (
            f"{_dim(f'({cnt:,}) ')}{pct_s}"
            if (show_counts and cnt is not None)
            else pct_s
        )

    def _comp_cell(
        nn: Optional[int], total: Optional[int], pct: Optional[float]
    ) -> str:
        """Format a completeness cell with non-null count, total, and percentage."""
        if pct is None or nn is None or total is None:
            return _dim("—")
        pct_s = _color_pct(pct)
        return f"{_dim(f'({nn:,}/{total:,})')} {pct_s}"

    def _all_action_types_for_table(tname: str) -> List[str]:
        """Get all unique action types seen across snapshots for a given table."""
        if _norm_tbl_name(tname) != "actions":
            return []
        seen_list: List[str] = []
        seen = set()
        for snap in snaps:
            st = snap.get(tname)
            if not st or not isinstance(st.extra, dict):
                continue
            per_type = st.extra.get("actions_links_per_type") or []
            for row in per_type:
                at = str(row.get("action_type"))
                if at not in seen:
                    seen.add(at)
                    seen_list.append(at)
        return seen_list

    for tname in all_tables:
        print()
        any_estimated = any(
            snap.get(tname) and snap[tname].is_estimated for snap in snaps
        )
        suffix = _dim(" (sampled estimate)") if any_estimated else ""
        print(f"{Style.BRIGHT}TABLE{Style.RESET_ALL} {_color_table(tname)}{suffix}")

        # Union of columns across all inspectors for this table
        all_cols = sorted(
            {
                col
                for snap in snaps
                for col in snap.get(tname, TableStat(0, {})).columns.keys()
            }
        )

        def _dtype_for(col: str) -> str:
            """Get data type for a column from the first available snapshot."""
            for snap in snaps:
                st = snap.get(tname)
                if st and col in st.columns:
                    return st.columns[col].data_type
            return "—"

        # Left column width (column / type + subrows)
        left_texts = [f"{col} : {_dtype_for(col)}" for col in all_cols] or [
            "column / type"
        ]
        left_w = max(
            len("column / type"), max((_vislen(s) for s in left_texts), default=0)
        )

        def _enum_values_for(col: str) -> List[str]:
            """Get all unique enum values across snapshots for a column."""
            seen_vals, seen_set = [], set()
            for snap in snaps:
                st = snap.get(tname)
                if not st or col not in st.columns:
                    continue
                lst = getattr(st.columns[col], "enum_counts", None)
                if not lst:
                    continue
                for v, _c, _p in lst:
                    if v not in seen_set:
                        seen_vals.append(v)
                        seen_set.add(v)
            if "other" in seen_vals:
                seen_vals = [v for v in seen_vals if v != "other"] + ["other"]
            return seen_vals

        # enum value labels
        for col in all_cols:
            for v in _enum_values_for(col):
                left_w = max(left_w, _vislen(f"↳ {v}"))

        # actions-specific labels under action_type
        if _norm_tbl_name(tname) == "actions":
            for at in _all_action_types_for_table(tname):
                left_w = max(left_w, _vislen(f"{at}"))
                for id_col in ACTION_ID_COLS:
                    left_w = max(left_w, _vislen(f"↳ {id_col}"))

        # -------- Per-DB widths for THIS table --------
        db_cell_w = [max(_vislen(lbl), 6) for lbl in labels]
        for idx, snap in enumerate(snaps):
            widest = db_cell_w[idx]
            widest = max(widest, _vislen(_color_pct(1.0)))  # baseline: "100.0%"

            st = snap.get(tname)
            if st:
                for col in all_cols:
                    if col not in st.columns:
                        continue
                    cstat = st.columns[col]
                    total = st.est_rows if st.est_rows and st.est_rows > 0 else None
                    nn = (
                        int(total * cstat.completeness)
                        if (total and cstat.completeness is not None)
                        else None
                    )
                    widest = max(
                        widest, _vislen(_comp_cell(nn, total, cstat.completeness))
                    )

                    if cstat.enum_counts:
                        for _v, c, p in cstat.enum_counts:
                            widest = max(widest, _vislen(_enum_cell(c, p)))

                # actions: per-action-type link rows under action_type
                if _norm_tbl_name(tname) == "actions" and isinstance(st.extra, dict):
                    per_type = st.extra.get("actions_links_per_type") or []
                    by_type = {str(r["action_type"]): r for r in per_type}
                    for at in _all_action_types_for_table(tname):
                        r = by_type.get(at)
                        for id_col in ACTION_ID_COLS:
                            if not r:
                                cell = _dim("—")
                            else:
                                total_at = r["total"]
                                nn_at = r["nn"].get(id_col, 0)
                                pct_at = r["pct"].get(id_col)
                                cell = _comp_cell(nn_at, total_at, pct_at)
                            widest = max(widest, _vislen(cell))

            db_cell_w[idx] = widest

        # Header
        hdr_left = _rpad_ansi(_dim("column / type"), left_w)
        hdr_right = " | ".join(
            _rpad_ansi(f"{Style.BRIGHT}{lbl}{Style.RESET_ALL}", w)
            for lbl, w in zip(labels, db_cell_w)
        )
        print(f"{hdr_left} | {hdr_right}")

        # Separator
        sep_len = (
            left_w + 3 + sum(db_cell_w) + 3 * (len(db_cell_w) - 1 if db_cell_w else 0)
        )
        print("-" * sep_len)

        # -------- Rows --------
        for col in all_cols:
            dtype = _dtype_for(col)
            left_cell = _rpad_ansi(f"{col} : {dtype}", left_w)

            # main completeness row
            cells = []
            for snap, w in zip(snaps, db_cell_w):
                st = snap.get(tname)
                if not st or col not in st.columns:
                    cells.append(_rpad_ansi(_dim("—"), w))
                else:
                    cstat = st.columns[col]
                    total = st.est_rows if st.est_rows and st.est_rows > 0 else None
                    nn = (
                        int(total * cstat.completeness)
                        if (total and cstat.completeness is not None)
                        else None
                    )
                    cells.append(
                        _lpad_ansi(_comp_cell(nn, total, cstat.completeness), w)
                    )
            print(f"{left_cell} | " + " | ".join(cells))

            # enum subrows
            enum_lists: List[Optional[List[Tuple[str, int, float]]]] = []
            for snap in snaps:
                st = snap.get(tname)
                enum_lists.append(
                    getattr(st.columns[col], "enum_counts", None)
                    if (st and col in st.columns)
                    else None
                )

            if any(enum_lists):
                ordered_vals = _enum_values_for(col)
                for v in ordered_vals:
                    left_val = _rpad_ansi(
                        f"{Fore.LIGHTYELLOW_EX}↳ {v}{Fore.RESET}", left_w
                    )
                    cells = []
                    for lst, w in zip(enum_lists, db_cell_w):
                        if not lst:
                            cells.append(_rpad_ansi(_dim("—"), w))
                            continue
                        cnt, pct = None, None
                        for vv, c, p in lst:
                            if vv == v:
                                cnt, pct = c, p
                                break
                        cells.append(_lpad_ansi(_enum_cell(cnt, pct), w))
                    print(f"{left_val} | " + " | ".join(cells))

            # actions: per-action-type group under action_type
            if (
                _norm_tbl_name(tname) == "actions"
                and _norm_tbl_name(col) == "action_type"
            ):
                ordered_types = _all_action_types_for_table(tname)
                if ordered_types:
                    for at in ordered_types:
                        # header row per action_type (empty stats)
                        header_left = _rpad_ansi(f"{at}", left_w)
                        header_cells = [_rpad_ansi(_dim("—"), w) for w in db_cell_w]
                        print(f"{header_left} | " + " | ".join(header_cells))

                        # subrows: link columns
                        for id_col in ACTION_ID_COLS:
                            left_val = _rpad_ansi(
                                f"{Fore.LIGHTYELLOW_EX}↳ {id_col}{Fore.RESET}", left_w
                            )
                            cells = []
                            for snap, w in zip(snaps, db_cell_w):
                                st = snap.get(tname)
                                if not st or not isinstance(st.extra, dict):
                                    cells.append(_rpad_ansi(_dim("—"), w))
                                    continue
                                per_type = st.extra.get("actions_links_per_type") or []
                                by_type = {str(r["action_type"]): r for r in per_type}
                                r = by_type.get(at)
                                if not r:
                                    cells.append(_rpad_ansi(_dim("—"), w))
                                    continue
                                total_at = r["total"]
                                nn_at = r["nn"].get(id_col, 0)
                                pct_at = r["pct"].get(id_col)
                                cells.append(
                                    _lpad_ansi(
                                        _comp_cell(nn_at, total_at, pct_at),
                                        w,
                                    )
                                )
                            print(f"{left_val} | " + " | ".join(cells))

        print("-" * sep_len)
