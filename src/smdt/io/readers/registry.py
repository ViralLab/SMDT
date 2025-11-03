from __future__ import annotations

from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Iterable, List, Optional, IO, Mapping

from .base import Reader

__all__ = [
    "register",
    "discover",
    "get_reader",
    "read",
    "read_from_filelike",
]

# Most-recent registrations take precedence
_READERS: List[Reader] = []


def register(reader: Reader) -> None:
    """Register a reader instance; newest wins during selection."""
    _READERS.insert(0, reader)


def discover(group: str = "smdt.readers") -> None:
    """
    Load and register readers exposed via entry points.

    Accepts entry points that are either:
      - a Reader instance (has .supports/.stream)
      - a callable that returns a Reader instance
    """
    try:
        eps = entry_points(group=group)  # py3.10+
    except TypeError:
        eps_all = entry_points()  # older importlib.metadata
        eps = getattr(eps_all, "select", lambda **kw: [])(group=group)  # type: ignore

    for ep in eps:
        obj: Any = ep.load()
        if callable(obj) and not hasattr(obj, "supports"):
            obj = obj()  # factory returning a Reader
        if hasattr(obj, "supports") and (
            hasattr(obj, "stream") or hasattr(obj, "read")
        ):
            register(obj)  # type: ignore[arg-type]


def get_reader(uri: str, *, content_type: Optional[str] = None) -> Optional[Reader]:
    """
    Return the first registered reader that supports the given URI / content type.
    Newest registered readers are tried first.
    """
    for r in _READERS:
        try:
            if r.supports(uri, content_type=content_type):
                return r
        except Exception:
            # Ignore a misbehaving plugin and try the next one
            continue
    return None


def read(uri: str, **kwargs) -> Iterable[dict]:
    """
    Convenience helper: select a reader and iterate records for a path/URI.

    Prefers Reader.stream(...). Falls back to Reader.read(...) for legacy plugins.
    """
    content_type = kwargs.pop("content_type", None)
    r = get_reader(uri, content_type=content_type)
    if not r:
        raise RuntimeError(f"No reader found for {uri}")

    if hasattr(r, "stream"):
        return r.stream(uri, **kwargs)  # type: ignore[attr-defined]
    if hasattr(r, "read"):
        return r.read(uri, **kwargs)  # type: ignore[attr-defined]

    # Shouldn't happen due to checks above
    raise RuntimeError(f"Reader {type(r).__name__} does not implement stream/read")


def read_from_filelike(
    f: IO[bytes], *, member_name: Optional[str] = None, **kwargs
) -> Iterable[Mapping[str, Any]]:
    """
    Dispatch a *file-like* stream (e.g., archive member) to the appropriate reader.

    If the selected reader implements stream_from_filelike(...), use it directly,
    passing member_name to enable nested compression handling (.csv.gz inside .tar).
    Otherwise, materialize to a temporary file and reuse read().
    """
    # Find a reader based on the member name (may include multi-suffixes)
    r = get_reader(member_name or "")
    if r and hasattr(r, "stream_from_filelike"):
        return r.stream_from_filelike(f, member_name=member_name, **kwargs)  # type: ignore[attr-defined]

    # Fallback: dump to a temp file and call read()
    import tempfile, shutil, os

    suffix = "".join(Path(member_name or "member.bin").suffixes) or ".bin"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        shutil.copyfileobj(f, tmp)
        tmp_path = tmp.name
    try:
        return read(tmp_path, **kwargs)
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


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
