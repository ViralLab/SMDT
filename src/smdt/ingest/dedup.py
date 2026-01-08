from __future__ import annotations
from typing import Any, Dict, Hashable, Iterable, List, Sequence, Tuple


def _has_value(v: Any) -> bool:
    """Check if a value is considered 'present'.

    Args:
        v: Value to check.

    Returns:
        True if the value is not None and not an empty string, False otherwise.
    """
    return v is not None and (not isinstance(v, str) or v.strip() != "")


def _info_score(obj: Any) -> int:
    """Count how many attributes are 'filled'.

    Works for dataclasses / simple objects.
    If obj is a dict, you can change obj.__dict__ to obj directly.

    Args:
        obj: Object to inspect.

    Returns:
        Number of non-empty attributes.
    """
    d = getattr(obj, "__dict__", {})
    return sum(1 for _, v in d.items() if _has_value(v))


def dedup_best(
    items: Iterable[Any],
    *,
    key_fields: Sequence[str],
) -> List[Any]:
    """Keep exactly one item per key (key_fields).

    Chooses the one with:
      1) highest _info_score()
      2) if tied, larger created_at (when present)

    Args:
        items: Iterable of items to deduplicate.
        key_fields: Sequence of field names to use as the key.

    Returns:
        List of deduplicated items.
    """
    best: Dict[Tuple[Hashable, ...], Any] = {}

    for it in items:
        key = tuple(getattr(it, f, None) for f in key_fields)
        cand = best.get(key)
        if cand is None:
            best[key] = it
            continue

        s_new = _info_score(it)
        s_old = _info_score(cand)
        if s_new > s_old:
            best[key] = it
        elif s_new == s_old:
            ca_new = getattr(it, "created_at", None)
            ca_old = getattr(cand, "created_at", None)
            if ca_new and ca_old and ca_new > ca_old:
                best[key] = it

    return list(best.values())
