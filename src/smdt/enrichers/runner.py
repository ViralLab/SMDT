from __future__ import annotations
from typing import Any, Dict, Type, Union
from smdt.enrichers.base import BaseEnricher
from smdt.enrichers.registry import (
    get_enricher,
    list_enrichers as list_registry_enrichers,
)
from smdt.store.standard_db import StandardDB


def run_enricher(
    enricher: Union[str, Type[BaseEnricher]],
    *,
    db: StandardDB,
    config: Any = None,
    db_batch_size: int = 1000,
) -> None:
    """Construct and run an enricher.

    Args:
        enricher: Either a registered enricher name (e.g. ``"text_generation"``) or
            an enricher class directly (e.g. ``TextGenerationEnricher`` -- works
            even if it isn't registered).
        db: Database connection or handler.
        config: The enricher's own config dataclass instance (recommended --
            gives autocomplete/type checking), or a plain dict of its fields.
        db_batch_size: Rows fetched from the DB per iteration.

    Example:
        >>> from smdt.enrichers.text_generation import TextGenerationEnricher, TextGenerationConfig
        >>> run_enricher(
        ...     TextGenerationEnricher,  # or "text_generation"
        ...     db=db,
        ...     config=TextGenerationConfig.for_openai(model="gpt-4o-mini", api_key="..."),
        ... )
    """
    cls = get_enricher(enricher)["cls"] if isinstance(enricher, str) else enricher
    instance = cls(db, config=config)
    instance.run(db_batch_size=db_batch_size)


def list_enrichers() -> Dict[str, str]:
    """List available enrichers and their descriptions.

    Returns:
        Dictionary mapping enricher names to their descriptions.
    """
    return {k: v["description"] for k, v in list_registry_enrichers().items()}
