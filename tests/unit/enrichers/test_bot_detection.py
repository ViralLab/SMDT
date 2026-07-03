from unittest.mock import MagicMock

import pytest

from smdt.enrichers.bot_detection import BotometerConfig, BotometerEnricher


def test_config_defaults() -> None:
    cfg = BotometerConfig()
    assert cfg.only_missing is True
    assert cfg.model_path is None


def test_config_requires_output_dir_when_not_saving_to_db(tmp_path) -> None:
    with pytest.raises(ValueError, match="output_dir"):
        BotometerConfig(do_save_to_db=False)
    cfg = BotometerConfig(do_save_to_db=False, output_dir=str(tmp_path))
    assert cfg.output_dir == str(tmp_path)


def test_enricher_registration() -> None:
    assert BotometerEnricher.TARGET == "accounts"
    assert BotometerEnricher.ENRICHER_NAME == "bot_detection"


def test_model_id_has_no_suffix() -> None:
    """bot_detection has exactly one model, so model_id is just the registered name."""
    db = MagicMock()
    e = BotometerEnricher(db, config=BotometerConfig())
    assert e.model_id == "bot_detection"


def test_extract_features_handles_none_input() -> None:
    """_extract_features(None) should return zeroed defaults, not raise."""
    db = MagicMock()
    e = BotometerEnricher(db, config=BotometerConfig())
    features = e._extract_features(None)
    assert features["follower_count"] == 0
    assert features["ff_ratio"] == 1.0


def test_extract_features_computes_ff_ratio() -> None:
    db = MagicMock()
    e = BotometerEnricher(db, config=BotometerConfig())
    row = {
        "friend_count": 9,
        "follower_count": 4,
        "post_count": 10,
        "creation_timestamp": None,
    }
    features = e._extract_features(row)
    assert features["ff_ratio"] == pytest.approx((9 + 1) / (4 + 1))
