from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

import duckdb

from smdt.config import DBConfig

_VALID_ALIAS = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class AttachedDataset:
    """Metadata for one dataset attached to a `MultiStore`.

    Attributes:
        alias: The SQL schema name it was attached under (e.g. `"twitter"`).
        db_name: The dataset's underlying Postgres database name.
        cfg: Connection details used to attach it.
    """

    alias: str
    db_name: str
    cfg: DBConfig


class MultiStore:
    """Attach multiple SMDT-standardized Postgres databases into one DuckDB
    connection, for cross-dataset (e.g. cross-platform) analysis.

    Every SMDT dataset lives in its own Postgres database, but all of them
    share the exact same standardized schema (`accounts`, `posts`, `entities`,
    `actions`, ...). That means cross-dataset analysis is mostly a matter of
    running the same SQL against more than one database at once -- which is
    exactly what DuckDB's `postgres` extension does well via `ATTACH`. Each
    attached dataset shows up as a DuckDB schema under the alias you give it,
    so a two-dataset join is just:

        SELECT tw.username, bs.username
        FROM twitter.accounts tw
        JOIN bluesky.accounts bs ON tw.username = bs.username

    Example:
        >>> from smdt.multistore import MultiStore
        >>> with MultiStore() as ms:
        ...     ms.attach("twitter", db_name="twitter_db")
        ...     ms.attach("bluesky", db_name="bluesky_db")
        ...     df = ms.query('''
        ...         SELECT tw.username, bs.username
        ...         FROM twitter.accounts tw
        ...         JOIN bluesky.accounts bs ON tw.username = bs.username
        ...     ''')

    Note:
        PostGIS `geometry` columns (`accounts.location`, `posts.location`)
        come through an attached scan as opaque raw bytes, not usable
        geometry -- DuckDB's postgres scanner doesn't understand the
        PostGIS wire type. Use `raw()` to run PostGIS functions (e.g.
        `ST_AsText(location)`) on the Postgres side before the value ever
        crosses into DuckDB.

    Read-only by default: `ATTACH ... (TYPE POSTGRES, READ_ONLY)` means
    MultiStore can't accidentally write back into a dataset's database.
    Writes belong to each dataset's own `StandardDB`, not here.
    """

    def __init__(self, *, read_only: bool = True):
        """Initialize a MultiStore.

        Args:
            read_only: Attach every dataset read-only (default). Set to
                `False` only if you specifically need to write through an
                attached connection -- most analysis workflows don't.
        """
        self.read_only = read_only
        self._con = duckdb.connect()
        self._con.execute("INSTALL postgres;")
        self._con.execute("LOAD postgres;")
        self._datasets: Dict[str, AttachedDataset] = {}

    def attach(self, alias: str, *, db_name: str, cfg: Optional[DBConfig] = None) -> None:
        """Attach a dataset's Postgres database under `alias`.

        Args:
            alias: Name to attach under; referenced in SQL as `alias.table`
                (e.g. `"twitter"` -> `twitter.accounts`). Must be a valid,
                unquoted SQL identifier.
            db_name: The dataset's Postgres database name, exactly as you'd
                pass to `StandardDB(db_name, cfg)`.
            cfg: Connection details (host/user/password/port). Defaults to
                a fresh `DBConfig()` (reads `DB_*` env vars) if not given --
                pass one explicitly if different datasets live on different
                hosts/credentials.

        Raises:
            ValueError: If `alias` isn't a valid SQL identifier, or is
                already attached.
        """
        if not _VALID_ALIAS.match(alias):
            raise ValueError(
                f"alias {alias!r} must be a valid SQL identifier (letters, "
                "digits, underscore; can't start with a digit)."
            )
        if alias in self._datasets:
            raise ValueError(f"{alias!r} is already attached.")

        cfg = cfg or DBConfig()
        dsn = (
            f"host={cfg.host} port={cfg.port} dbname={db_name} "
            f"user={cfg.user} password={cfg.password} "
            f"application_name={cfg.application_name} "
            f"connect_timeout={cfg.connect_timeout}"
        )
        options = ", READ_ONLY" if self.read_only else ""
        self._con.execute(f"ATTACH '{dsn}' AS {alias} (TYPE POSTGRES{options});")
        self._datasets[alias] = AttachedDataset(alias=alias, db_name=db_name, cfg=cfg)

    def detach(self, alias: str) -> None:
        """Detach a previously-attached dataset.

        Args:
            alias: The alias it was attached under.

        Raises:
            KeyError: If `alias` isn't currently attached.
        """
        if alias not in self._datasets:
            raise KeyError(f"{alias!r} is not attached.")
        self._con.execute(f"DETACH {alias};")
        del self._datasets[alias]

    @property
    def datasets(self) -> Dict[str, AttachedDataset]:
        """Currently attached datasets, keyed by alias."""
        return dict(self._datasets)

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """The underlying DuckDB connection, for anything not covered above
        (e.g. `.sql(...)` for a relation instead of a DataFrame, `.pl()` for
        polars, streaming reads via `.fetch_record_batch()`)."""
        return self._con

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> "pandas.DataFrame":  # noqa: F821
        """Run a SQL query across attached datasets.

        Args:
            sql: SQL to run. Reference attached datasets as
                `<alias>.<table>` (e.g. `twitter.posts`).
            params: Optional positional parameters for `?`-style
                placeholders in `sql`.

        Returns:
            Result as a pandas DataFrame.
        """
        if params is not None:
            return self._con.execute(sql, params).df()
        return self._con.sql(sql).df()

    def raw(self, alias: str, sql: str) -> "pandas.DataFrame":  # noqa: F821
        """Run `sql` directly on the attached Postgres connection `alias`,
        bypassing DuckDB's scan/type mapping.

        Use this for PostGIS geometry columns (e.g.
        `ms.raw("twitter", "SELECT account_id, ST_AsText(location) FROM accounts")`)
        or any other Postgres-side function DuckDB's scanner wouldn't
        otherwise apply.

        Args:
            alias: The alias to run `sql` against.
            sql: Raw SQL, executed as-is on that dataset's Postgres
                connection (not spread across other attached datasets).

        Returns:
            Result as a pandas DataFrame.

        Raises:
            KeyError: If `alias` isn't currently attached.
        """
        if alias not in self._datasets:
            raise KeyError(f"{alias!r} is not attached.")
        escaped_sql = sql.replace("'", "''")
        return self._con.sql(f"SELECT * FROM postgres_query('{alias}', '{escaped_sql}')").df()

    def close(self) -> None:
        """Detach every dataset and close the DuckDB connection."""
        for alias in list(self._datasets):
            self.detach(alias)
        self._con.close()

    def __enter__(self) -> "MultiStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
