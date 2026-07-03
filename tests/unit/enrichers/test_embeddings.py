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
