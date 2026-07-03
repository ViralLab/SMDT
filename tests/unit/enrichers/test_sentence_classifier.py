from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")

from smdt.enrichers.sentence_classifier import (
    SentenceClassifierConfig,
    SentenceClassifierEnricher,
)


def test_requires_hf_model_id() -> None:
    with pytest.raises(ValueError, match="hf_model_id"):
        SentenceClassifierConfig()


def test_rejects_non_positive_model_batch_size() -> None:
    with pytest.raises(ValueError, match="model_batch_size"):
        SentenceClassifierConfig(hf_model_id="some/model", model_batch_size=0)


def test_model_kwargs_and_tokenizer_kwargs_default_to_empty_dict() -> None:
    """The escape hatch for checkpoint-specific args should default to {},
    not None, so callers can always do **cfg.model_kwargs safely."""
    cfg = SentenceClassifierConfig(hf_model_id="some/model")
    assert cfg.model_kwargs == {}
    assert cfg.tokenizer_kwargs == {}


def test_model_kwargs_escape_hatch_is_preserved() -> None:
    cfg = SentenceClassifierConfig(
        hf_model_id="some/model",
        model_kwargs={"trust_remote_code": True},
        tokenizer_kwargs={"use_fast": False},
    )
    assert cfg.model_kwargs == {"trust_remote_code": True}
    assert cfg.tokenizer_kwargs == {"use_fast": False}


def test_model_id_includes_model_name() -> None:
    db = MagicMock()
    cfg = SentenceClassifierConfig(hf_model_id="some/model", model_name="sentiment-v1")
    e = SentenceClassifierEnricher(db, config=cfg)
    assert e.model_id == "sentence_classifier_sentiment-v1"


def test_save_results_uses_body_key_not_content() -> None:
    """Regression test: save_results used to read r["content"], but
    process_batch only ever produces a "body" key -- guaranteed KeyError on
    every save. Verify save_results can consume process_batch's actual shape."""
    db = MagicMock()
    cfg = SentenceClassifierConfig(hf_model_id="some/model", do_save_to_db=True)
    e = SentenceClassifierEnricher(db, config=cfg)

    from datetime import datetime, timezone

    fake_result = {
        "created_at": datetime.now(timezone.utc),
        "retrieved_at": datetime.now(timezone.utc),
        "post_id": "p1",
        "model_id": e.model_id,
        "body": {"scores": {"positive": 0.9}, "logits": {"positive": 2.1}},
    }
    # Should not raise KeyError("content").
    e.save_results([fake_result])
    db.insert_with_fallbacks.assert_called_once()
