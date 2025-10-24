from __future__ import annotations

from importlib.metadata import entry_points
from typing import Iterable, List, Optional, Protocol, Any

from .base import Reader  # expects .supports(...) and .stream(...)

_READERS: List[Reader] = []


def register(reader: Reader) -> None:
    """
    Register a reader instance. Newer registrations take precedence.
    """
    _READERS.insert(0, reader)


def discover(group: str = "smdt.readers") -> None:
    """
    Load and register readers exposed via entry points.

    Accepts either:
      - an instance with .supports/.stream
      - a callable returning such an instance
    """
    # Py3.10+ provides entry_points(group=...), older style is entry_points().select(...)
    try:
        eps = entry_points(group=group)  # type: ignore[call-arg]
    except TypeError:
        # Fallback for very old importlib.metadata
        eps_all = entry_points()
        eps = getattr(eps_all, "select", lambda **kw: [])(group=group)  # type: ignore

    for ep in eps:
        obj: Any = ep.load()
        # If the entry point is a factory, call it
        if callable(obj) and not hasattr(obj, "supports"):
            obj = obj()  # try to instantiate
        # Only register if it looks like a Reader
        if hasattr(obj, "supports") and (
            hasattr(obj, "stream") or hasattr(obj, "read")
        ):
            register(obj)  # type: ignore[arg-type]


def get_reader(uri: str, *, content_type: Optional[str] = None) -> Optional[Reader]:
    """
    Return the first registered reader that supports the given URI / content type.
    """
    for r in _READERS:
        try:
            if r.supports(uri, content_type=content_type):
                return r
        except Exception:
            # Ignore a misbehaving plugin and try the next
            continue
    return None


def read(uri: str, **kwargs) -> Iterable[dict]:
    """
    Convenience helper: select a reader and iterate records.

    Prefers Reader.stream(...). Falls back to Reader.read(...) for legacy plugins.
    """
    content_type = kwargs.pop("content_type", None)
    r = get_reader(uri, content_type=content_type)
    if not r:
        raise RuntimeError(f"No reader found for {uri}")

    # Prefer the modern stream(...) API
    if hasattr(r, "stream"):
        return r.stream(uri, **kwargs)  # type: ignore[attr-defined]
    # Back-compat with older readers implementing read(...)
    if hasattr(r, "read"):
        return r.read(uri, **kwargs)  # type: ignore[attr-defined]

    # Shouldn’t get here because of the check in discover()
    raise RuntimeError(f"Reader {type(r).__name__} does not implement stream/read")
