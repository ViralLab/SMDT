from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")

from smdt.enrichers.toxicity import (
    ToxicityConfig,
    ToxicityEnricher,
    default_mention_preprocessor,
)


def test_config_defaults() -> None:
    cfg = ToxicityConfig()
    assert cfg.model_name == "multilingual"
    assert cfg.only_missing is True  # inherited from EnricherRunConfig


def test_rejects_unsupported_model_name() -> None:
    with pytest.raises(ValueError, match="not one of the supported Detoxify variants"):
        ToxicityConfig(model_name="not-a-real-variant")


def test_rejects_non_positive_model_batch_size() -> None:
    with pytest.raises(ValueError, match="model_batch_size"):
        ToxicityConfig(model_batch_size=0)


@pytest.mark.parametrize(
    "name", ["original", "unbiased", "multilingual", "original-small", "unbiased-small"]
)
def test_accepts_all_known_model_names(name) -> None:
    cfg = ToxicityConfig(model_name=name)
    assert cfg.model_name == name


def test_model_id_includes_model_name() -> None:
    db = MagicMock()
    e = ToxicityEnricher(db, config=ToxicityConfig(model_name="unbiased"))
    assert e.model_id == "toxicity_unbiased"


def test_default_mention_preprocessor_collapses_mentions_to_user() -> None:
    """Mention normalization used to be a hardcoded method (_clean) called
    from process_batch; it's now a standalone preprocessor, detached from
    the model itself, wired in as ToxicityConfig's default preprocessors entry."""
    out = default_mention_preprocessor({"body": "hey @JohnDoe how are you"})
    assert out["body"] == "hey @user how are you"


def test_default_mention_preprocessor_is_wired_into_config_by_default() -> None:
    cfg = ToxicityConfig()
    assert cfg.preprocessors == [default_mention_preprocessor]


def test_default_mention_preprocessor_can_be_overridden() -> None:
    my_fn = lambda row: row
    cfg = ToxicityConfig(preprocessors=[my_fn])
    assert cfg.preprocessors == [my_fn]
