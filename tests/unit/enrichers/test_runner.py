from unittest.mock import MagicMock

import pytest

from smdt.enrichers import registry as registry_module
from smdt.enrichers.registry import register
from smdt.enrichers.runner import run_enricher


@pytest.fixture(autouse=True)
def _isolated_registry(monkeypatch):
    monkeypatch.setattr(registry_module, "_ENRICHERS", {})


def _make_enricher_class():
    @register("dispatch_test", target="posts", description="x")
    class DispatchTestEnricher:
        def __init__(self, db, *, config=None):
            self.db = db
            self.config = config
            self.run_called_with = None

        def run(self, *, db_batch_size):
            self.run_called_with = db_batch_size

    return DispatchTestEnricher


def test_run_enricher_dispatches_by_registered_name() -> None:
    """run_enricher('name', ...) should look up and construct the registered class."""
    cls = _make_enricher_class()
    db = MagicMock()
    config = object()

    # Patch __init__ won't let us inspect the instance directly, so subclass
    # instead: construct manually to compare against what run_enricher builds.
    captured = {}
    orig_init = cls.__init__

    def spy_init(self, db_, *, config=None):
        orig_init(self, db_, config=config)
        captured["instance"] = self

    cls.__init__ = spy_init

    run_enricher("dispatch_test", db=db, config=config, db_batch_size=42)

    instance = captured["instance"]
    assert instance.db is db
    assert instance.config is config
    assert instance.run_called_with == 42


def test_run_enricher_dispatches_by_class_directly() -> None:
    """run_enricher(SomeClass, ...) should work without any registry lookup,
    even for a class that was never registered at all."""

    class UnregisteredEnricher:
        def __init__(self, db, *, config=None):
            self.db = db
            self.config = config
            self.run_called_with = None

        def run(self, *, db_batch_size):
            self.run_called_with = db_batch_size

    db = MagicMock()
    config = object()

    # run_enricher doesn't return the instance, so wrap to capture it.
    instances = []
    real_init = UnregisteredEnricher.__init__

    def spy_init(self, db_, *, config=None):
        real_init(self, db_, config=config)
        instances.append(self)

    UnregisteredEnricher.__init__ = spy_init

    run_enricher(UnregisteredEnricher, db=db, config=config, db_batch_size=7)

    assert len(instances) == 1
    assert instances[0].run_called_with == 7


def test_run_enricher_default_db_batch_size() -> None:
    """db_batch_size should default to 1000 if not specified."""
    cls = _make_enricher_class()
    db = MagicMock()

    captured = {}
    orig_init = cls.__init__

    def spy_init(self, db_, *, config=None):
        orig_init(self, db_, config=config)
        captured["instance"] = self

    cls.__init__ = spy_init

    run_enricher("dispatch_test", db=db)
    assert captured["instance"].run_called_with == 1000
