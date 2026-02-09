from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import re

from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)


# =============================================================
# Data classes
# =============================================================
@dataclass
class ColStat:
    """Statistics for a single column."""

    data_type: str
    completeness: Optional[float]  # 0..1, or None if not computable
    enum_counts: Optional[List[Tuple[str, int, float]]] = None  # (val, count, pct)


@dataclass
class TableStat:
    """Statistics for a table: estimated rows and per-column stats."""

    est_rows: int
    columns: Dict[str, ColStat]  # col_name -> ColStat
    # Optional extra payload for table-specific stats (e.g., actions links per action_type)
    extra: Optional[Dict[str, Any]] = None


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

    def __init__(self, db, schema: str, *, max_enum_items: int = 8):
        """Initialize the Inspector.

        Args:
            db: Database connection or handler.
            schema: Schema name to inspect.
            max_enum_items: Maximum number of enum items to collect stats for.
        """
        self.db = db
        self.schema = schema
        self.max_enum_items = max_enum_items

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

                out: Dict[str, TableStat] = {}
                for tname, est_rows in tables:
                    if allow and _norm_tbl_name(tname) not in allow:
                        continue

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

                    cstats: Dict[str, ColStat] = {}
                    total = 0
                    extra: Optional[Dict[str, Any]] = None

                    if cols:
                        parts = ["COUNT(*) AS total"]
                        for col, *_ in cols:
                            alias = f"nn_{_norm_tbl_name(col)}"
                            parts.append(f"COUNT({psql_ident(col)}) AS {alias}")
                        cur.execute(
                            f"SELECT {', '.join(parts)} FROM {psql_ident_full(self.schema, tname)}"
                        )
                        row = cur.fetchone()
                        total = int(row[0] or 0)

                        for i, (col, typ, typtype, atttypid) in enumerate(
                            cols, start=1
                        ):
                            nn = int(row[i] or 0)
                            comp = None if total == 0 else (nn / total)
                            cstats[col] = ColStat(data_type=str(typ), completeness=comp)

                            # enum sub-stats
                            if self._is_enum_or_domain_enum(cur, typtype, atttypid):
                                cstats[col].enum_counts = self._value_counts(
                                    cur, tname, col, total, limit=self.max_enum_items
                                )

                        # Table-specific extras: actions link stats per action_type
                        if _norm_tbl_name(tname) == "actions":
                            extra = self._actions_link_stats(cur, tname)

                    out[tname] = TableStat(
                        est_rows=int(total if total > 0 else (est_rows or -1)),
                        columns=cstats,
                        extra=extra,
                    )

                if allow:
                    out = {k: v for k, v in out.items() if _norm_tbl_name(k) in allow}
                return out
        finally:
            conn.close()

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

    def _value_counts(
        self,
        cur,
        table: str,
        column: str,
        total_rows: int,
        *,
        limit: int = 8,
    ) -> List[Tuple[str, int, float]]:
        """Get value counts for a column.

        Args:
            cur: Database cursor.
            table: Table name.
            column: Column name.
            total_rows: Total rows in the table.
            limit: Maximum number of values to return.

        Returns:
            List of (value, count, percentage) tuples.
        """
        if total_rows <= 0:
            return []
        cur.execute(
            f"""
            SELECT {psql_ident(column)}::text AS val, COUNT(*) AS cnt
            FROM {psql_ident_full(self.schema, table)}
            GROUP BY 1 ORDER BY cnt DESC, val ASC
            LIMIT %s
            """,
            (max(1, limit),),
        )
        rows = cur.fetchall()
        top = [
            (("NULL" if v is None else v), int(c), int(c) / total_rows) for v, c in rows
        ]
        shown = sum(c for _v, c, _p in top)
        if shown < total_rows:
            other = total_rows - shown
            top.append(("other", other, other / total_rows))
        return top

    # -------- actions-specific helper --------------------------------
    def _actions_link_stats(self, cur, table: str) -> Dict[str, Any]:
        """
        For the actions table, compute per-action_type completeness of:
        target_account_id, target_post_id, originator_account_id, originator_post_id,
        originator_community_id, target_community_id.
        """
        cur.execute(
            f"""
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
            GROUP BY 1
            ORDER BY 1
            """
        )
        rows = cur.fetchall()
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
# Reporting
# =============================================================


def report_schemas(
    inspectors: List["Inspector"],
    *,
    only_tables: Optional[List[str]] = None,
    show_counts: bool = True,  # applies to enum rows; completeness always shows (nn/total)
) -> None:
    """Generate and print a schema report.

    Args:
        inspectors: List of Inspector instances.
        only_tables: Optional list of tables to include.
        show_counts: Whether to show counts in enum stats.
    """
    allow = {_norm_tbl_name(t) for t in only_tables} if only_tables else None

    # Collect snapshots and labels
    snaps: List[Dict[str, TableStat]] = []
    labels: List[str] = []
    for ins in inspectors:
        snaps.append(ins.snapshot(only_tables=only_tables))
        labels.append(f"{ins.db.db_name}:{ins.schema}")

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
        if pct is None or nn is None or total is None:
            return _dim("—")
        pct_s = _color_pct(pct)
        return f"{_dim(f'({nn:,}/{total:,})')} {pct_s}"

    def _all_action_types_for_table(tname: str) -> List[str]:
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
        print(f"{Style.BRIGHT}TABLE{Style.RESET_ALL} {_color_table(tname)}")

        # Union of columns across all inspectors for this table
        all_cols = sorted(
            {
                col
                for snap in snaps
                for col in snap.get(tname, TableStat(0, {})).columns.keys()
            }
        )

        def _dtype_for(col: str) -> str:
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
