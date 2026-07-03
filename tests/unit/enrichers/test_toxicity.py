from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")

from smdt.enrichers.toxicity import DetoxifyConfig, DetoxifyToxicityEnricher


def test_config_defaults() -> None:
    cfg = DetoxifyConfig()
    assert cfg.model_name == "multilingual"
    assert cfg.only_missing is True  # inherited from EnricherRunConfig


def test_rejects_unsupported_model_name() -> None:
    with pytest.raises(ValueError, match="not one of the supported Detoxify variants"):
        DetoxifyConfig(model_name="not-a-real-variant")


def test_rejects_non_positive_model_batch_size() -> None:
    with pytest.raises(ValueError, match="model_batch_size"):
        DetoxifyConfig(model_batch_size=0)


@pytest.mark.parametrize(
    "name", ["original", "unbiased", "multilingual", "original-small", "unbiased-small"]
)
def test_accepts_all_known_model_names(name) -> None:
    cfg = DetoxifyConfig(model_name=name)
    assert cfg.model_name == name


def test_model_id_includes_model_name() -> None:
    db = MagicMock()
    e = DetoxifyToxicityEnricher(db, config=DetoxifyConfig(model_name="unbiased"))
    assert e.model_id == "toxicity_unbiased"
