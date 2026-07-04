from unittest.mock import MagicMock

import pytest

from smdt.enrichers.embeddings import EmbeddingConfig, EmbeddingEnricher


def test_requires_embedding_model_id() -> None:
    with pytest.raises(ValueError, match="embedding_model_id"):
        EmbeddingConfig(base_url="http://localhost:8010/v1")


def test_requires_base_url() -> None:
    with pytest.raises(ValueError, match="base_url"):
        EmbeddingConfig(embedding_model_id="intfloat/e5-large")


def test_only_missing_exists_and_defaults_true() -> None:
    """Regression test: EmbeddingConfig (formerly VLLMEmbeddingConfig) previously
    had no only_missing field, so total_count()/fetch_batch() crashed the
    moment they read self.cfg.only_missing."""
    cfg = EmbeddingConfig(
        embedding_model_id="intfloat/e5-large", base_url="http://localhost:8010/v1"
    )
    assert cfg.only_missing is True


def test_model_id_without_postfix_is_exactly_embeddings() -> None:
    """Regression test: this used to read self._ENRICHER_ID, which was never
    overridden (only a dead no-underscore `ENRICHER_ID` attribute existed), so
    it silently resolved to the BaseEnricher default "base", producing
    "base_<postfix>" instead of "embeddings_<postfix>"."""
    db = MagicMock()
    cfg = EmbeddingConfig(
        embedding_model_id="intfloat/e5-large", base_url="http://localhost:8010/v1"
    )
    e = EmbeddingEnricher(db, config=cfg)
    assert e.model_id == "embeddings"


def test_model_id_with_postfix() -> None:
    db = MagicMock()
    cfg = EmbeddingConfig(
        embedding_model_id="intfloat/e5-large",
        base_url="http://localhost:8010/v1",
        model_id_postfix="e5-large",
    )
    e = EmbeddingEnricher(db, config=cfg)
    assert e.model_id == "embeddings_e5-large"


def test_rejects_non_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        EmbeddingConfig(
            embedding_model_id="intfloat/e5-large",
            base_url="http://localhost:8010/v1",
            batch_size=0,
        )


def test_total_count_builds_query_without_crashing() -> None:
    db = MagicMock()
    cur = db.connect.return_value.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = (3,)
    cfg = EmbeddingConfig(
        embedding_model_id="intfloat/e5-large", base_url="http://localhost:8010/v1"
    )
    e = EmbeddingEnricher(db, config=cfg)
    assert e.total_count() == 3


def test_warns_when_commercial_base_url_has_no_privacy_layer(caplog) -> None:
    """base_url pointing at a known commercial API host with no
    privacy_fields configured should log a warning, not raise."""
    with caplog.at_level("WARNING"):
        EmbeddingConfig(
            embedding_model_id="text-embedding-3-small",
            base_url="https://api.openai.com/v1",
        )
    assert any(
        "commercial API host" in r.message and "https://api.openai.com/v1" in r.message
        for r in caplog.records
    )


def test_no_warning_when_privacy_fields_configured(caplog) -> None:
    with caplog.at_level("WARNING"):
        EmbeddingConfig(
            embedding_model_id="text-embedding-3-small",
            base_url="https://api.openai.com/v1",
            privacy_fields=["body"],
            pepper=b"pepper",
        )
    assert not any("commercial API host" in r.message for r in caplog.records)


def test_no_warning_for_self_hosted_base_url(caplog) -> None:
    with caplog.at_level("WARNING"):
        EmbeddingConfig(
            embedding_model_id="intfloat/e5-large", base_url="http://localhost:8010/v1"
        )
    assert not any("commercial API host" in r.message for r in caplog.records)


def test_for_openai_prefills_base_url() -> None:
    cfg = EmbeddingConfig.for_openai(model="text-embedding-3-small", api_key="sk-test")
    assert cfg.embedding_model_id == "text-embedding-3-small"
    assert cfg.base_url == "https://api.openai.com/v1"
    assert cfg.api_key == "sk-test"


def test_for_openai_forwards_extra_kwargs() -> None:
    cfg = EmbeddingConfig.for_openai(
        model="text-embedding-3-small", api_key="sk-test", batch_size=256
    )
    assert cfg.batch_size == 256


def test_for_openai_still_goes_through_full_validation() -> None:
    """Factory is sugar over __init__, not a bypass -- validation still runs."""
    with pytest.raises(ValueError, match="batch_size"):
        EmbeddingConfig.for_openai(
            model="text-embedding-3-small", api_key="sk-test", batch_size=0
        )
