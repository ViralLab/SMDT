from __future__ import annotations
import importlib.util
from typing import Any, Dict, List

_ENRICHERS: Dict[str, Dict[str, Any]] = {}


def register(
    name: str, *, target: str, description: str = "", requires: List[str] | None = None
):
    """
    Decorator to register an enricher class.
    """

    def decorator(cls):
        _ENRICHERS[name] = {
            "cls": cls,
            "target": target,  # e.g. "posts" | "accounts"
            "description": description,
            "requires": requires or [],
        }
        return cls

    return decorator


def _deps_ok(pkgs: List[str]) -> bool:
    for dep in pkgs:
        if importlib.util.find_spec(dep) is None:
            return False
    return True


def list_enrichers() -> Dict[str, Dict[str, Any]]:
    """Return registry with a computed 'status' field."""
    out: Dict[str, Dict[str, Any]] = {}
    for name, meta in _ENRICHERS.items():
        out[name] = dict(meta)
        out[name]["status"] = "ok" if _deps_ok(meta["requires"]) else "missing"
    return out


def get_enricher(name: str):
    return _ENRICHERS[name]


def ensure_dependencies(name: str) -> None:
    info = _ENRICHERS[name]
    missing = [d for d in info["requires"] if importlib.util.find_spec(d) is None]
    if missing:
        raise RuntimeError(
            f"Enricher '{name}' requires {missing}. "
            "Install with: pip install 'smdt[enrichers]' or the specific packages."
        )
