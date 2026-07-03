"""PII detection policy for the Presidio-based redaction engine.

Unlike `PseudonymPolicy` (which maps whole columns to an action), `PiiPolicy`
governs PII *detected inside free text* — e.g. a phone number typed into
`posts.body`. It is per-table, per-column, per-entity-type: an entity type
absent from a column's rules is not even scanned for in that column.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Mapping, Optional, Union

from .redact import normalize_host


class PiiAction(str, Enum):
    """Action to take on a detected PII span."""

    HASH = "hash"
    REPLACE = "replace"
    DROP = "drop"


@dataclass(frozen=True)
class PiiRule:
    """How to handle one PII entity type in one column.

    Attributes:
        action: HASH (pepper-keyed, consistent across the dataset), REPLACE
            (fixed placeholder or a callable transform of the matched text),
            or DROP (remove the span entirely).
        replacement: For REPLACE only. A fixed string (e.g. "[PHONE_NUMBER]"),
            or a callable applied to the matched text (e.g. to reduce a URL to
            just its domain). Defaults to "[<ENTITY_TYPE>]" if not given.
    """

    action: PiiAction
    replacement: Optional[Union[str, Callable[[str], str]]] = None


@dataclass(frozen=True)
class PiiPolicy:
    """Per-table, per-column PII detection policy.

    `rules[table][column]` maps a Presidio (or platform-specific) entity type
    — e.g. "PHONE_NUMBER", "PERSON", "MENTION", "HASHTAG" — to a `PiiRule`.
    """

    rules: Mapping[str, Mapping[str, Mapping[str, PiiRule]]] = field(
        default_factory=dict
    )

    def entities_for(self, table: str, column: str) -> List[str]:
        """Entity types configured for a given table/column (possibly empty)."""
        return list(self.rules.get(table, {}).get(column, {}).keys())

    def rule_for(self, table: str, column: str, entity_type: str) -> Optional[PiiRule]:
        """Look up the rule for one entity type in one table/column."""
        return self.rules.get(table, {}).get(column, {}).get(entity_type)

    def is_configured(self, table: str, column: str) -> bool:
        """Whether any PII scanning is configured for this table/column."""
        return bool(self.rules.get(table, {}).get(column))


def _url_to_domain(matched_url: str) -> str:
    """Reduce a matched URL to "URL_<domain>", preserving old Redactor behavior."""
    host = normalize_host(matched_url)
    return f"URL_{host}" if host else "[URL]"


_COMMON_RULES = {
    "MENTION": PiiRule(PiiAction.HASH),
    "URL": PiiRule(PiiAction.REPLACE, replacement=_url_to_domain),
    "PHONE_NUMBER": PiiRule(PiiAction.REPLACE),
    "EMAIL_ADDRESS": PiiRule(PiiAction.REPLACE),
    "CREDIT_CARD": PiiRule(PiiAction.REPLACE),
    "PERSON": PiiRule(PiiAction.REPLACE),
}

DEFAULT_PII_POLICY = PiiPolicy(
    rules={
        "accounts": {"bio": dict(_COMMON_RULES)},
        "communities": {"bio": dict(_COMMON_RULES)},
        "posts": {
            "body": {
                **_COMMON_RULES,
                "HASHTAG": PiiRule(PiiAction.REPLACE, replacement=lambda t: t.lower()),
            },
        },
    }
)
