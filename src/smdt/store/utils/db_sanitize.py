import json
from datetime import datetime, timezone
from typing import Any, Mapping, Iterable


def scrub_nul_str(s: str) -> str:
    """
    Remove NUL bytes from a string.
    Fast path: only replace if actually present.
    """
    return s if "\x00" not in s else s.replace("\x00", "")


def scrub_nul_deep(obj: Any) -> Any:
    """
    Recursively scrub NULs from an object.
    Handles str/bytes/dict/list/tuple/set.
    - For sets: returns list (JSON-safe).
    - For non-string scalars: returns unchanged.
    """
    if obj is None:
        return None
    if isinstance(obj, str):
        return scrub_nul_str(obj)
    if isinstance(obj, bytes):
        try:
            s = obj.decode("utf-8", errors="replace")
        except Exception:
            s = obj.decode("utf-8", errors="ignore")
        return scrub_nul_str(s)
    if isinstance(obj, Mapping):
        return {scrub_nul_deep(k): scrub_nul_deep(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        t = type(obj)
        return t(scrub_nul_deep(x) for x in obj)
    if isinstance(obj, set):
        return [scrub_nul_deep(x) for x in obj]
    return obj


def iso_dt(v: datetime) -> str:
    """
    Format datetime as ISO8601 string with timezone.
    If naive, assume UTC.
    """
    if v is None:
        return None
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.isoformat()


def to_copy_text(v: Any) -> str:
    """
    Convert Python value into safe text for PostgreSQL COPY.
    Rules:
      - None → '\\N' (Postgres NULL marker)
      - datetime → ISO string
      - bool/int/float → str()
      - str/bytes → scrubbed text
      - dict/list/set/tuple → JSON string
    """
    if v is None:
        return r"\N"
    if isinstance(v, datetime):
        return iso_dt(v)
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return scrub_nul_str(v)
    if isinstance(v, bytes):
        try:
            s = v.decode("utf-8", errors="replace")
        except Exception:
            s = v.decode("utf-8", errors="ignore")
        return scrub_nul_str(s)
    if isinstance(v, (list, tuple, set, dict)):
        return json.dumps(scrub_nul_deep(v), ensure_ascii=False)
    # Fallback
    return scrub_nul_str(str(v))
