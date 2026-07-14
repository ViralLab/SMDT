"""Inspector module for analyzing database schema and data statistics."""

from .inspector import (
    Inspector,
    report_schemas,
    save_snapshot,
    load_snapshot,
    snapshot_to_dict,
)

__all__ = [
    "Inspector",
    "report_schemas",
    "save_snapshot",
    "load_snapshot",
    "snapshot_to_dict",
]
