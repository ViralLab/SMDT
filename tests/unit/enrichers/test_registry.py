import pytest

from smdt.enrichers import registry as registry_module
from smdt.enrichers.registry import (
    ensure_dependencies,
    get_enricher,
    list_enrichers,
    register,
)


@pytest.fixture(autouse=True)
def _isolated_registry(monkeypatch):
    """Each test gets its own empty registry, so tests don't leak into each other."""
    monkeypatch.setattr(registry_module, "_ENRICHERS", {})


def test_register_stamps_target_and_enricher_name_on_class() -> None:
    """@register should stamp TARGET/ENRICHER_NAME directly onto the class.

    This is what replaced the old pattern of every enricher hand-declaring
    (and frequently mis-declaring) these as separate class attributes.
    """

    @register("my_enricher", target="accounts", description="test")
    class MyEnricher:
        pass

    assert MyEnricher.TARGET == "accounts"
    assert MyEnricher.ENRICHER_NAME == "my_enricher"


def test_get_enricher_returns_registered_metadata() -> None:
    @register("my_enricher", target="posts", description="does a thing")
    class MyEnricher:
        pass

    meta = get_enricher("my_enricher")
    assert meta["cls"] is MyEnricher
    assert meta["target"] == "posts"
    assert meta["description"] == "does a thing"


def test_list_enrichers_reports_ok_status_when_deps_installed() -> None:
    @register("no_deps", target="posts", requires=[])
    class NoDepsEnricher:
        pass

    out = list_enrichers()
    assert out["no_deps"]["status"] == "ok"


def test_list_enrichers_reports_missing_status_for_uninstalled_dep() -> None:
    @register("needs_fake_pkg", target="posts", requires=["this_package_does_not_exist_xyz"])
    class NeedsFakePkgEnricher:
        pass

    out = list_enrichers()
    assert out["needs_fake_pkg"]["status"] == "missing"


def test_ensure_dependencies_raises_for_missing_package() -> None:
    @register("needs_fake_pkg2", target="posts", requires=["this_package_does_not_exist_xyz"])
    class NeedsFakePkg2Enricher:
        pass

    with pytest.raises(RuntimeError, match="needs_fake_pkg2"):
        ensure_dependencies("needs_fake_pkg2")


def test_ensure_dependencies_noop_for_satisfied_deps() -> None:
    @register("stdlib_only", target="posts", requires=["json"])
    class StdlibOnlyEnricher:
        pass

    ensure_dependencies("stdlib_only")  # should not raise
