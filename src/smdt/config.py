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
    default_dbname: str = os.getenv("DEFAULT_DB_NAME", "")
    user: str = os.getenv("DB_USER", "")
    password: str = os.getenv("DB_PASSWORD", "")
    owner: str = os.getenv("DB_OWNER", "")
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    application_name: str = os.getenv("APP_NAME", "standarddb")
    connect_timeout: int = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
    standard_schema_path: str = STANDARD_SCHEMA_SQL_PATH
