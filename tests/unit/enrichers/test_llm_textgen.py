from unittest.mock import MagicMock

import pytest

from smdt.enrichers.llm_textgen import TextGenConfig, TextGenEnricher


def _valid_kwargs(**overrides):
    kwargs = dict(
        chat_model_id="gpt-4o-mini",
        base_url="https://api.openai.com/v1",
    )
    kwargs.update(overrides)
    return kwargs


def test_requires_chat_model_id() -> None:
    with pytest.raises(ValueError, match="chat_model_id"):
        TextGenConfig(base_url="https://api.openai.com/v1")


def test_requires_base_url() -> None:
    with pytest.raises(ValueError, match="base_url"):
        TextGenConfig(chat_model_id="gpt-4o-mini")


def test_rejects_unsupported_provider_kind() -> None:
    with pytest.raises(ValueError, match="Unsupported provider_kind"):
        TextGenConfig(**_valid_kwargs(provider_kind="not-a-real-provider"))


@pytest.mark.parametrize(
    "kind", ["openai", "anthropic", "hf-text", "ollama", "gemini"]
)
def test_accepts_all_known_provider_kinds(kind) -> None:
    cfg = TextGenConfig(**_valid_kwargs(provider_kind=kind))
    assert cfg.provider_kind == kind


def test_provider_model_defaults_to_chat_model_id() -> None:
    cfg = TextGenConfig(**_valid_kwargs())
    assert cfg.provider_model == cfg.chat_model_id


def test_rejects_non_positive_max_tokens() -> None:
    with pytest.raises(ValueError, match="max_tokens"):
        TextGenConfig(**_valid_kwargs(max_tokens=0))


def test_rejects_non_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        TextGenConfig(**_valid_kwargs(batch_size=0))


def test_model_id_without_postfix() -> None:
    db = MagicMock()
    e = TextGenEnricher(db, config=TextGenConfig(**_valid_kwargs()))
    assert e.model_id == "textgen"


def test_model_id_with_postfix() -> None:
    db = MagicMock()
    cfg = TextGenConfig(**_valid_kwargs(model_id_postfix="v1_sentiment"))
    e = TextGenEnricher(db, config=cfg)
    assert e.model_id == "textgen_v1_sentiment"
