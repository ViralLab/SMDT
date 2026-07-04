from unittest.mock import MagicMock

from smdt.enrichers.language_detection import (
    LanguageDetectionConfig,
    LanguageDetectionEnricher,
    default_text_cleanup_preprocessor,
)


def test_instantiation_does_not_crash() -> None:
    """Regression test: this enricher used to crash immediately on construction.

    self.cfg was never assigned in __init__ (only the base class's self.config
    was set), so the very next line (self.cfg.reset_cache) raised AttributeError
    before the object even finished constructing.
    """
    db = MagicMock()
    e = LanguageDetectionEnricher(db, config=LanguageDetectionConfig())
    assert e.model_id == "language_detection"


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
    assert e.model_id == "language_detection_v2"


def test_default_preprocessor_strips_mentions_and_emoji() -> None:
    """Cleanup used to be a hardcoded method called from process_batch;
    it's now a standalone preprocessor, detached from the model itself,
    wired in as LanguageDetectionConfig's default `preprocessors` entry."""
    cleaned = default_text_cleanup_preprocessor({"body": "hey @someone 😀 great post"})[
        "body"
    ]
    assert "@someone" not in cleaned
    assert "😀" not in cleaned
    assert "great post" in cleaned


def test_default_preprocessor_is_wired_into_config_by_default() -> None:
    cfg = LanguageDetectionConfig()
    assert cfg.preprocessors == [default_text_cleanup_preprocessor]


def test_default_preprocessor_can_be_overridden() -> None:
    """Passing a custom preprocessors list replaces the default entirely --
    no hidden merging, consistent with EnricherRunConfig.preprocessors."""
    my_fn = lambda row: row
    cfg = LanguageDetectionConfig(preprocessors=[my_fn])
    assert cfg.preprocessors == [my_fn]


def test_total_count_builds_query_without_crashing() -> None:
    """This used to raise AttributeError on self.cfg.only_missing before even
    reaching the DB call."""
    db = MagicMock()
    cur = db.connect.return_value.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = (5,)
    e = LanguageDetectionEnricher(db, config=LanguageDetectionConfig())
    assert e.total_count() == 5
