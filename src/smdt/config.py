import os
from dataclasses import dataclass

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STANDARD_SCHEMA_SQL_PATH = os.path.join(
    BASE_DIR, "store", "schemas", "standard_schema.sql"
)


@dataclass
class DBConfig:
    """Database configuration parameters.

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

    default_dbname: str = os.getenv("DEFAULT_DB_NAME", "")
    user: str = os.getenv("DB_USER", "")
    password: str = os.getenv("DB_PASSWORD", "")
    owner: str = os.getenv("DB_OWNER", "")
    host: str = os.getenv("DB_HOST", "localhost")
    # parse integers from env with safe fallback to defaults
    try:
        _port_val = os.getenv("DB_PORT", "5432")
        port: int = int(_port_val)
    except Exception:
        port: int = 5432
    application_name: str = os.getenv("APP_NAME", "standarddb")
    try:
        _ct = os.getenv("DB_CONNECT_TIMEOUT", "10")
        connect_timeout: int = int(_ct)
    except Exception:
        connect_timeout: int = 10
    standard_schema_path: str = STANDARD_SCHEMA_SQL_PATH


@dataclass
class AnonymizationVariables:
    """Configuration for anonymization secrets."""

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
                "PEPPER environment variable is required for anonymization"
            )

        return val.encode("utf-8")
