from __future__ import annotations
from typing import Any, Dict
from smdt.enrichers.registry import get, available
from smdt.store.standard_db import StandardDB


def run_enricher(name: str, *, db: StandardDB, **kwargs: Any) -> None:
    cls = get(name)
    enricher = cls(db, **kwargs)
    enricher.run()


def list_enrichers() -> Dict[str, str]:
    return {k: v._enricher_description for k, v in available().items()}
