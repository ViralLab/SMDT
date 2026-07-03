from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from smdt.enrichers.base import BaseEnricher, EnricherRunConfig


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
