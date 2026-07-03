from unittest.mock import MagicMock

from smdt.enrichers.language_detection import (
    LanguageDetectionConfig,
    LanguageDetectionEnricher,
)


def test_instantiation_does_not_crash() -> None:
    """Regression test: this enricher used to crash immediately on construction.

    self.cfg was never assigned in __init__ (only the base class's self.config
    was set), so the very next line (self.cfg.reset_cache) raised AttributeError
    before the object even finished constructing.
    """
    db = MagicMock()
    e = LanguageDetectionEnricher(db, config=LanguageDetectionConfig())
    assert e.model_id == "langdetect"


def test_config_only_missing_exists_and_defaults_true() -> None:
    """Regression test: LanguageDetectionConfig previously had no only_missing
    field at all, so total_count()/fetch_batch() crashed with AttributeError
    the moment they read self.cfg.only_missing."""
    cfg = LanguageDetectionConfig()
    assert cfg.only_missing is True


def test_model_id_with_postfix() -> None:
    db = MagicMock()
    e = LanguageDetectionEnricher(
        db, config=LanguageDetectionConfig(model_id_postfix="v2")
    )
    assert e.model_id == "langdetect_v2"


def test_preprocess_strips_mentions_and_emoji() -> None:
    db = MagicMock()
    e = LanguageDetectionEnricher(db, config=LanguageDetectionConfig())
    cleaned = e._preprocess("hey @someone 😀 great post")
    assert "@someone" not in cleaned
    assert "😀" not in cleaned
    assert "great post" in cleaned


def test_total_count_builds_query_without_crashing() -> None:
    """This used to raise AttributeError on self.cfg.only_missing before even
    reaching the DB call."""
    db = MagicMock()
    cur = db.connect.return_value.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = (5,)
    e = LanguageDetectionEnricher(db, config=LanguageDetectionConfig())
    assert e.total_count() == 5
