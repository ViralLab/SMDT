import os
from dataclasses import dataclass, field

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STANDARD_SCHEMA_SQL_PATH = os.path.join(
    BASE_DIR, "store", "schemas", "standard_schema.sql"
)


def _env_int(name: str, default: int) -> int:
    """Read an int env var, falling back to `default` if unset/invalid."""
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


@dataclass
class DBConfig:
    """Database configuration parameters.

    Fields are re-read from the environment on every `DBConfig()` call (via
    `default_factory`), not frozen at import time -- so changing `DB_*` env
    vars mid-process (e.g. in a test) and then constructing a fresh
    `DBConfig()` picks up the new values.

    Attributes:
        default_dbname: Default database name.
        user: Database user.
        password: Database password.
        owner: Database owner.
        host: Database host.
        port: Database port.
        application_name: Application name for DB connections.
        connect_timeout: Connection timeout in seconds.
        standard_schema_path: Path to the standard schema SQL file.
    """

    default_dbname: str = field(default_factory=lambda: os.getenv("DEFAULT_DB_NAME", ""))
    user: str = field(default_factory=lambda: os.getenv("DB_USER", ""))
    password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", ""))
    owner: str = field(default_factory=lambda: os.getenv("DB_OWNER", ""))
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: _env_int("DB_PORT", 5432))
    application_name: str = field(default_factory=lambda: os.getenv("APP_NAME", "standarddb"))
    connect_timeout: int = field(default_factory=lambda: _env_int("DB_CONNECT_TIMEOUT", 10))
    standard_schema_path: str = STANDARD_SCHEMA_SQL_PATH


@dataclass
class PseudonymizationVariables:
    """Configuration for pseudonymization secrets."""

    @property
    def pepper(self) -> bytes:
        """Get the pepper for hashing from the environment.

        Returns:
            The pepper as bytes.

        Raises:
            RuntimeError: If the PEPPER environment variable is not set.
        """
        val: str = os.getenv("PEPPER")
        if not val:
            raise RuntimeError(
                "PEPPER environment variable is required for pseudonymization"
            )

        return val.encode("utf-8")
