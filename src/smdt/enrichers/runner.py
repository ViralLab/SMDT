from __future__ import annotations
from typing import Any, Dict
from smdt.enrichers.registry import get, available
from smdt.store.standard_db import StandardDB


def run_enricher(name: str, *, db: StandardDB, **kwargs: Any) -> None:
    """Run a specific enricher.

    Args:
        name: Name of the enricher to run.
        db: Database connection or handler.
        **kwargs: Additional arguments passed to the enricher.
    """
    cls = get(name)
    enricher = cls(db, **kwargs)
    enricher.run()


def list_enrichers() -> Dict[str, str]:
    """List available enrichers and their descriptions.

    Returns:
        Dictionary mapping enricher names to their descriptions.
    """
    return {k: v._enricher_description for k, v in available().items()}
