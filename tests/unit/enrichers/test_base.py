from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from smdt.enrichers.base import (
    BaseEnricher,
    EnricherRunConfig,
    warn_if_unprotected_commercial_api,
)


class _DummyEnricher(BaseEnricher):
    """Minimal concrete BaseEnricher for testing shared mechanics."""

    def fetch_batch(self, offset, limit):
        return []

    def total_count(self):
        return 0

    def process_batch(self, rows):
        return []

    def save_results(self, results):
        pass


def test_enricher_run_config_defaults() -> None:
    """EnricherRunConfig should have sane defaults with no required fields."""
    cfg = EnricherRunConfig()
    assert cfg.only_missing is True
    assert cfg.reset_cache is False
    assert cfg.do_save_to_db is True
    assert cfg.output_dir is None


def test_enricher_run_config_requires_output_dir_when_not_saving_to_db(tmp_path) -> None:
    """do_save_to_db=False without output_dir should fail fast."""
    with pytest.raises(ValueError, match="output_dir"):
        EnricherRunConfig(do_save_to_db=False)

    # With output_dir given, it should succeed and create the directory.
    out = tmp_path / "out"
    cfg = EnricherRunConfig(do_save_to_db=False, output_dir=str(out))
    assert cfg.output_dir == str(out)
    assert out.exists()


def test_enricher_run_config_requires_pepper_when_privacy_fields_set() -> None:
    """privacy_fields is the "enable" switch for the built-in privacy layer;
    it needs a pepper to build the Hasher it redacts with."""
    with pytest.raises(ValueError, match="pepper"):
        EnricherRunConfig(privacy_fields=["body"])

    cfg = EnricherRunConfig(privacy_fields=["body"], pepper=b"pepper")
    assert cfg.privacy_fields == ["body"]


def test_enricher_run_config_privacy_disabled_by_default() -> None:
    cfg = EnricherRunConfig()
    assert cfg.privacy_fields == []
    assert cfg.pii_policy is None
    assert cfg.pepper is None


def test_subclass_config_can_add_required_fields() -> None:
    """A subclass config can add its own required (no-default) fields.

    This is the kw_only=True trick that avoids Python's dataclass
    "non-default argument follows default argument" error when inheriting
    from a base whose fields all have defaults.
    """

    @dataclass
    class MyConfig(EnricherRunConfig):
        hf_model_id: str

    cfg = MyConfig(hf_model_id="some/model", only_missing=False)
    assert cfg.hf_model_id == "some/model"
    assert cfg.only_missing is False


def test_default_target_and_enricher_name_are_base() -> None:
    """Unregistered classes fall back to the BaseEnricher class defaults."""
    assert BaseEnricher.TARGET == "posts"
    assert BaseEnricher.ENRICHER_NAME == "base"


def test_coerce_config_accepts_instance_dict_or_none() -> None:
    """_coerce_config should accept a ready instance, a dict, or None."""

    @dataclass
    class MyConfig(EnricherRunConfig):
        x: int = 1

    instance = MyConfig(x=5)
    assert BaseEnricher._coerce_config(instance, MyConfig) is instance
    assert BaseEnricher._coerce_config({"x": 5}, MyConfig).x == 5
    assert BaseEnricher._coerce_config(None, MyConfig).x == 1


def test_coerce_config_rejects_wrong_type() -> None:
    """A non-dict, non-instance, non-None config should raise TypeError."""

    @dataclass
    class MyConfig(EnricherRunConfig):
        x: int = 1

    with pytest.raises(TypeError, match="MyConfig"):
        BaseEnricher._coerce_config(123, MyConfig)


def test_make_model_id_without_suffix() -> None:
    """With no suffix, model_id is exactly ENRICHER_NAME."""
    db = MagicMock()
    e = _DummyEnricher(db)
    assert e._make_model_id() == "base"
    assert e._make_model_id(None) == "base"


def test_make_model_id_with_suffix() -> None:
    """With a suffix, model_id is "ENRICHER_NAME_suffix"."""
    db = MagicMock()
    e = _DummyEnricher(db)
    assert e._make_model_id("v1") == "base_v1"


def test_apply_privacy_is_a_noop_when_privacy_fields_empty() -> None:
    """Disabled by default -- no smdt.pseudonymizer objects get built at all."""
    db = MagicMock()
    e = _DummyEnricher(db)
    e.cfg = EnricherRunConfig()
    rows = [{"body": "hey @JohnDoe, call me at 212-555-0182"}]
    assert e._apply_privacy(rows) == rows
    assert not hasattr(e, "_privacy_redactor")


def test_apply_privacy_redacts_mentions_via_baseline_redactor_without_pii_policy() -> None:
    """No pii_policy configured -> privacy_fields still get the dependency-free
    Redactor (mentions/emails/URLs), consistent with Pseudonymizer's fallback."""
    db = MagicMock()
    e = _DummyEnricher(db)
    e.cfg = EnricherRunConfig(privacy_fields=["body"], pepper=b"pepper")
    rows = [{"body": "hey @JohnDoe check my.email@example.com"}]
    out = e._apply_privacy(rows)
    assert "@JohnDoe" not in out[0]["body"]
    assert "@u_" in out[0]["body"]
    assert out[0]["body"].count("[EMAIL]") == 1


def test_apply_privacy_ignores_fields_not_configured() -> None:
    """Only fields named in privacy_fields are touched."""
    db = MagicMock()
    e = _DummyEnricher(db)
    e.cfg = EnricherRunConfig(privacy_fields=["body"], pepper=b"pepper")
    rows = [{"body": "hey @JohnDoe", "account_name": "@JohnDoe"}]
    out = e._apply_privacy(rows)
    assert out[0]["account_name"] == "@JohnDoe"


def test_apply_privacy_skips_none_values() -> None:
    db = MagicMock()
    e = _DummyEnricher(db)
    e.cfg = EnricherRunConfig(privacy_fields=["body"], pepper=b"pepper")
    rows = [{"body": None}]
    assert e._apply_privacy(rows) == [{"body": None}]


def test_apply_privacy_uses_pii_engine_when_policy_configures_the_column() -> None:
    """A field pii_policy actually configures routes through the Presidio
    PiiEngine instead of the baseline Redactor."""
    pytest.importorskip("presidio_analyzer")
    pytest.importorskip("presidio_anonymizer")
    try:
        import spacy

        spacy.load("en_core_web_sm")
    except Exception:
        pytest.skip("en_core_web_sm not installed")

    from smdt.pseudonymizer.pii_policy import PiiAction, PiiPolicy, PiiRule

    db = MagicMock()
    e = _DummyEnricher(db)
    policy = PiiPolicy(
        rules={"posts": {"body": {"PHONE_NUMBER": PiiRule(PiiAction.REPLACE)}}}
    )
    e.cfg = EnricherRunConfig(
        privacy_fields=["body"],
        pepper=b"pepper",
        pii_policy=policy,
        nlp_configuration={
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
        },
    )
    rows = [{"body": "call 212-555-0182 now"}]
    out = e._apply_privacy(rows)
    assert out[0]["body"] == "call [PHONE_NUMBER] now"


def test_apply_privacy_runs_before_preprocessors_in_run() -> None:
    """Integration: run() must apply the privacy layer before preprocessors,
    matching the "redact first, clean up gibberish after" ordering."""
    db = MagicMock()

    class _RecordingEnricher(BaseEnricher):
        def __init__(self, db):
            super().__init__(db)
            self.seen_batches = []
            self.cfg = EnricherRunConfig(
                privacy_fields=["body"],
                pepper=b"pepper",
                preprocessors=[lambda row: {**row, "body": row["body"] + "-cleaned"}],
            )

        def fetch_batch(self, offset, limit):
            if offset > 0:
                return []
            return [{"body": "hey @JohnDoe"}]

        def total_count(self):
            return None

        def process_batch(self, rows):
            self.seen_batches.append(rows)
            return []

        def save_results(self, results):
            pass

    e = _RecordingEnricher(db)
    e.run()
    body = e.seen_batches[0][0]["body"]
    assert "@u_" in body
    assert body.endswith("-cleaned")


def test_apply_preprocessors_is_a_noop_when_none_configured() -> None:
    """No preprocessors configured -> rows pass through unchanged."""
    db = MagicMock()
    e = _DummyEnricher(db)
    e.cfg = EnricherRunConfig()
    rows = [{"body": "hello"}]
    assert e._apply_preprocessors(rows) == [{"body": "hello"}]


def test_apply_preprocessors_runs_single_preprocessor_on_every_row() -> None:
    db = MagicMock()
    e = _DummyEnricher(db)

    def upper_body(row):
        row = dict(row)
        row["body"] = row["body"].upper()
        return row

    e.cfg = EnricherRunConfig(preprocessors=[upper_body])
    rows = [{"body": "hello"}, {"body": "world"}]
    assert e._apply_preprocessors(rows) == [{"body": "HELLO"}, {"body": "WORLD"}]


def test_apply_preprocessors_composes_multiple_steps_in_list_order() -> None:
    """Each preprocessor sees the previous one's output, in list order."""
    db = MagicMock()
    e = _DummyEnricher(db)

    def append_a(row):
        row = dict(row)
        row["body"] = row["body"] + "a"
        return row

    def append_b(row):
        row = dict(row)
        row["body"] = row["body"] + "b"
        return row

    e.cfg = EnricherRunConfig(preprocessors=[append_a, append_b])
    rows = [{"body": "x"}]
    assert e._apply_preprocessors(rows) == [{"body": "xab"}]


def test_apply_preprocessors_can_touch_non_text_fields() -> None:
    """Preprocessors are not text-specific; any row field is fair game."""
    db = MagicMock()
    e = _DummyEnricher(db)

    def redact_account_name(row):
        row = dict(row)
        row["account_name"] = "REDACTED"
        return row

    e.cfg = EnricherRunConfig(preprocessors=[redact_account_name])
    rows = [{"account_name": "alice", "body": "hi"}]
    assert e._apply_preprocessors(rows) == [{"account_name": "REDACTED", "body": "hi"}]


def test_run_applies_preprocessors_before_process_batch_sees_rows() -> None:
    """Integration: run() must call preprocessors on each batch before
    handing it to process_batch, for both the known-total and
    unknown-total code paths."""

    def make_enricher(total):
        db = MagicMock()

        class _RecordingEnricher(BaseEnricher):
            def __init__(self, db):
                super().__init__(db)
                self.seen_batches = []
                self.cfg = EnricherRunConfig(
                    preprocessors=[lambda row: {**row, "seen": True}]
                )

            def fetch_batch(self, offset, limit):
                if offset > 0:
                    return []
                return [{"body": "hi"}]

            def total_count(self):
                return total

            def process_batch(self, rows):
                self.seen_batches.append(rows)
                return []

            def save_results(self, results):
                pass

        return _RecordingEnricher(db)

    for total in (None, 1):
        e = make_enricher(total)
        e.run()
        assert e.seen_batches == [[{"body": "hi", "seen": True}]]


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.openai.com/v1",
        "https://api.anthropic.com/v1/messages",
        "https://generativelanguage.googleapis.com/v1beta/openai/",
        "https://api-inference.huggingface.co/models/some/model",
    ],
)
def test_warn_if_unprotected_commercial_api_warns_for_known_hosts(base_url, caplog) -> None:
    cfg = EnricherRunConfig()
    with caplog.at_level("WARNING"):
        warn_if_unprotected_commercial_api(cfg, base_url)
    assert any("commercial API host" in r.message for r in caplog.records)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://localhost:11434/v1",
        "http://localhost:8010/v1",
        "https://my-internal-vllm.example.org/v1",
    ],
)
def test_warn_if_unprotected_commercial_api_silent_for_self_hosted(base_url, caplog) -> None:
    cfg = EnricherRunConfig()
    with caplog.at_level("WARNING"):
        warn_if_unprotected_commercial_api(cfg, base_url)
    assert not any("commercial API host" in r.message for r in caplog.records)


def test_warn_if_unprotected_commercial_api_silent_when_privacy_fields_set(caplog) -> None:
    cfg = EnricherRunConfig(privacy_fields=["body"], pepper=b"pepper")
    with caplog.at_level("WARNING"):
        warn_if_unprotected_commercial_api(cfg, "https://api.openai.com/v1")
    assert not any("commercial API host" in r.message for r in caplog.records)
