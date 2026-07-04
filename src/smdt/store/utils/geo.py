from typing import List, Optional, Tuple, Union


def normalize_point(
    val: Optional[Union[str, Tuple[float, float], List[float]]],
) -> Optional[str]:
    """Normalize a location value into PostGIS-compatible EWKT text.

    `accounts.location`/`posts.location` are PostGIS `geometry(Point, 4326)`
    columns, not Postgres's built-in `point` type -- they need WKT/EWKT text
    (e.g. `"SRID=4326;POINT(-122.4 37.8)"`), not `"(lon,lat)"`.

    Accepts:
      - A WKT/EWKT string (e.g. from a standardizer's own EWKT builder) --
        passed through unchanged (aside from stripping whitespace).
      - A `(lon, lat)` tuple/list -- converted to EWKT.
      - `None`.

    Args:
        val: Location value in one of the accepted shapes.

    Returns:
        EWKT string (`"SRID=4326;POINT(lon lat)"`), the input string
        stripped, or `None`.
    """
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s or None
    if isinstance(val, (tuple, list)) and len(val) == 2:
        lon, lat = val
        try:
            return f"SRID=4326;POINT({float(lon)} {float(lat)})"
        except (TypeError, ValueError):
            return None
    return None
