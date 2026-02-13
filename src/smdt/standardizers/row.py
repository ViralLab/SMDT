from collections.abc import Mapping, ItemsView, KeysView, ValuesView
from typing import Any, Dict, Iterator, List


class Record(Mapping[str, Any]):
    """
    Lightweight read-only mapping that behaves like a dict[str, Any],
    but internally stores values as a tuple with a shared name->index map.

    - Users can do rec["text"], rec.get("lang"), "id" in rec, for k in rec, etc.
    - Nested objects (e.g. rec["user"]) are left as-is.
    """

    __slots__ = ("_values", "_index", "_keys")

    def __init__(self, values: tuple[Any, ...], index: Dict[str, int], keys: List[str]):
        """Initialize a Record.

        Args:
            values: Tuple of values for this row.
            index: Shared mapping of column name to position.
            keys: Shared list of column names.
        """
        self._values = values  # per-row tuple of values
        self._index = index  # shared: col_name -> position
        self._keys = keys  # shared: list of column names

    def __getitem__(self, key: str) -> Any:
        """Get the value for a specific column name."""
        return self._values[self._index[key]]

    def __iter__(self) -> Iterator[str]:
        """Iterate over column names."""
        return iter(self._keys)

    def __len__(self) -> int:
        """Return the number of columns."""
        return len(self._keys)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value by key, returning a default if missing."""
        idx = self._index.get(key)
        if idx is None:
            return default
        return self._values[idx]

    def keys(self) -> KeysView[str]:
        """Return a view of the record's keys."""
        return KeysView(self)

    def values(self) -> ValuesView[Any]:
        """Return a view of the record's values."""
        return ValuesView(self)

    def items(self) -> ItemsView[str, Any]:
        """Return a view of the record's items (key, value pairs)."""
        return ItemsView(self)

    def to_dict(self) -> Dict[str, Any]:
        """Materialize a real dict. Slower, but available if users absolutely need it.

        Returns:
            Dictionary representation of the record.
        """
        return {k: self[k] for k in self._keys}

    def __repr__(self) -> str:
        """Return a string representation of the record (like a dict)."""
        # short repr so logs / debugging aren't insane
        # make it like json like {"id": 123, "text": "hello", ...}
        items = ", ".join(f'"{k}": {repr(self[k])}' for k in self._keys)
        return f"{{{items}}}"
