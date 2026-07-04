import os

from smdt.config import DBConfig


def test_db_config_reads_current_env_vars_not_import_time_snapshot(monkeypatch):
    """Regression test: DBConfig's field defaults used to be plain
    `os.getenv(...)` expressions evaluated once when the class body executed
    (at import time), so changing DB_* env vars later in the same process and
    constructing a new DBConfig() silently reused the stale values."""
    monkeypatch.setenv("DB_USER", "first_user")
    monkeypatch.setenv("DB_HOST", "first_host")
    cfg1 = DBConfig()
    assert cfg1.user == "first_user"
    assert cfg1.host == "first_host"

    monkeypatch.setenv("DB_USER", "second_user")
    monkeypatch.setenv("DB_HOST", "second_host")
    cfg2 = DBConfig()
    assert cfg2.user == "second_user"
    assert cfg2.host == "second_host"

    # cfg1 is a snapshot at its own construction time -- unaffected by later changes.
    assert cfg1.user == "first_user"
    assert cfg1.host == "first_host"


def test_db_config_port_and_connect_timeout_parse_from_env(monkeypatch):
    monkeypatch.setenv("DB_PORT", "6543")
    monkeypatch.setenv("DB_CONNECT_TIMEOUT", "30")
    cfg = DBConfig()
    assert cfg.port == 6543
    assert cfg.connect_timeout == 30


def test_db_config_port_falls_back_on_invalid_value(monkeypatch):
    monkeypatch.setenv("DB_PORT", "not-a-number")
    cfg = DBConfig()
    assert cfg.port == 5432


def test_db_config_defaults_when_env_unset(monkeypatch):
    for var in ("DEFAULT_DB_NAME", "DB_USER", "DB_PASSWORD", "DB_OWNER", "DB_HOST", "DB_PORT", "APP_NAME", "DB_CONNECT_TIMEOUT"):
        monkeypatch.delenv(var, raising=False)
    cfg = DBConfig()
    assert cfg.default_dbname == ""
    assert cfg.user == ""
    assert cfg.host == "localhost"
    assert cfg.port == 5432
    assert cfg.application_name == "standarddb"
    assert cfg.connect_timeout == 10
