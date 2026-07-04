from unittest.mock import MagicMock

import pytest

from smdt.enrichers.text_generation import TextGenerationConfig, TextGenerationEnricher


def _valid_kwargs(**overrides):
    kwargs = dict(
        chat_model_id="gpt-4o-mini",
        base_url="https://api.openai.com/v1",
    )
    kwargs.update(overrides)
    return kwargs


def test_requires_chat_model_id() -> None:
    with pytest.raises(ValueError, match="chat_model_id"):
        TextGenerationConfig(base_url="https://api.openai.com/v1")


def test_requires_base_url() -> None:
    with pytest.raises(ValueError, match="base_url"):
        TextGenerationConfig(chat_model_id="gpt-4o-mini")


def test_rejects_unsupported_provider_kind() -> None:
    with pytest.raises(ValueError, match="Unsupported provider_kind"):
        TextGenerationConfig(**_valid_kwargs(provider_kind="not-a-real-provider"))


@pytest.mark.parametrize(
    "kind", ["openai", "anthropic", "hf-text", "ollama", "gemini"]
)
def test_accepts_all_known_provider_kinds(kind) -> None:
    cfg = TextGenerationConfig(**_valid_kwargs(provider_kind=kind))
    assert cfg.provider_kind == kind


def test_provider_model_defaults_to_chat_model_id() -> None:
    cfg = TextGenerationConfig(**_valid_kwargs())
    assert cfg.provider_model == cfg.chat_model_id


def test_rejects_non_positive_max_tokens() -> None:
    with pytest.raises(ValueError, match="max_tokens"):
        TextGenerationConfig(**_valid_kwargs(max_tokens=0))


def test_rejects_non_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        TextGenerationConfig(**_valid_kwargs(batch_size=0))


def test_model_id_without_postfix() -> None:
    db = MagicMock()
    e = TextGenerationEnricher(db, config=TextGenerationConfig(**_valid_kwargs()))
    assert e.model_id == "text_generation"


def test_model_id_with_postfix() -> None:
    db = MagicMock()
    cfg = TextGenerationConfig(**_valid_kwargs(model_id_postfix="v1_sentiment"))
    e = TextGenerationEnricher(db, config=cfg)
    assert e.model_id == "text_generation_v1_sentiment"


def test_warns_when_commercial_base_url_has_no_privacy_layer(caplog) -> None:
    """base_url pointing at a known commercial API host with no
    privacy_fields configured should log a warning, not raise."""
    with caplog.at_level("WARNING"):
        TextGenerationConfig(**_valid_kwargs(base_url="https://api.openai.com/v1"))
    assert any(
        "commercial API host" in r.message and "https://api.openai.com/v1" in r.message
        for r in caplog.records
    )


def test_no_warning_when_privacy_fields_configured(caplog) -> None:
    with caplog.at_level("WARNING"):
        TextGenerationConfig(
            **_valid_kwargs(
                base_url="https://api.openai.com/v1",
                privacy_fields=["body"],
                pepper=b"pepper",
            )
        )
    assert not any("commercial API host" in r.message for r in caplog.records)


def test_no_warning_for_self_hosted_base_url(caplog) -> None:
    with caplog.at_level("WARNING"):
        TextGenerationConfig(
            **_valid_kwargs(
                provider_kind="ollama",
                base_url="http://localhost:11434/v1",
            )
        )
    assert not any("commercial API host" in r.message for r in caplog.records)


def test_for_openai_prefills_base_url_and_provider_kind() -> None:
    cfg = TextGenerationConfig.for_openai(model="gpt-4o-mini", api_key="sk-test")
    assert cfg.chat_model_id == "gpt-4o-mini"
    assert cfg.provider_kind == "openai"
    assert cfg.base_url == "https://api.openai.com/v1"
    assert cfg.api_key == "sk-test"


def test_for_openai_forwards_extra_kwargs() -> None:
    cfg = TextGenerationConfig.for_openai(
        model="gpt-4o-mini", api_key="sk-test", temperature=0.0, batch_size=20
    )
    assert cfg.temperature == 0.0
    assert cfg.batch_size == 20


def test_for_anthropic_prefills_base_url_and_provider_kind() -> None:
    cfg = TextGenerationConfig.for_anthropic(model="claude-3-5-sonnet-20241022", api_key="sk-ant-test")
    assert cfg.provider_kind == "anthropic"
    assert cfg.base_url == "https://api.anthropic.com/v1/messages"


def test_for_gemini_prefills_base_url_and_provider_kind() -> None:
    cfg = TextGenerationConfig.for_gemini(model="gemini-1.5-pro", api_key="AIza-test")
    assert cfg.provider_kind == "gemini"
    assert cfg.base_url == "https://generativelanguage.googleapis.com/v1beta/openai/"


def test_for_ollama_prefills_provider_kind_and_default_base_url() -> None:
    cfg = TextGenerationConfig.for_ollama(model="llama3")
    assert cfg.provider_kind == "ollama"
    assert cfg.base_url == "http://localhost:11434/v1"
    assert cfg.api_key == "ollama"


def test_for_ollama_accepts_custom_base_url() -> None:
    cfg = TextGenerationConfig.for_ollama(model="llama3", base_url="http://my-gpu-box:11434/v1")
    assert cfg.base_url == "http://my-gpu-box:11434/v1"


def test_provider_factories_still_go_through_full_validation() -> None:
    """Factories are sugar over __init__, not a bypass -- validation still runs."""
    with pytest.raises(ValueError, match="max_tokens"):
        TextGenerationConfig.for_openai(model="gpt-4o-mini", api_key="sk-test", max_tokens=0)
