import pytest

from smdt.pseudonymizer.pii_policy import (
    DEFAULT_PII_POLICY,
    PiiAction,
    PiiPolicy,
    PiiRule,
)


def test_entities_for_returns_configured_entity_types() -> None:
    """entities_for should list exactly the entity types configured for a table/column."""
    policy = PiiPolicy(
        rules={"posts": {"body": {"PHONE_NUMBER": PiiRule(PiiAction.REPLACE)}}}
    )
    assert policy.entities_for("posts", "body") == ["PHONE_NUMBER"]


def test_entities_for_missing_table_or_column_returns_empty() -> None:
    """entities_for should return [] for unconfigured tables/columns, not raise."""
    policy = PiiPolicy(rules={"posts": {"body": {}}})
    assert policy.entities_for("posts", "missing_col") == []
    assert policy.entities_for("missing_table", "body") == []


def test_rule_for_returns_none_when_unconfigured() -> None:
    """rule_for should return None rather than raise for an unconfigured entity type."""
    policy = PiiPolicy(rules={})
    assert policy.rule_for("posts", "body", "PHONE_NUMBER") is None


def test_rule_for_returns_the_configured_rule() -> None:
    """rule_for should return the exact PiiRule configured for an entity type."""
    rule = PiiRule(PiiAction.HASH)
    policy = PiiPolicy(rules={"posts": {"body": {"MENTION": rule}}})
    assert policy.rule_for("posts", "body", "MENTION") is rule


@pytest.mark.parametrize(
    "table,column,expected",
    [
        ("posts", "body", True),
        ("posts", "conversation_id", False),
        ("nonexistent", "body", False),
    ],
)
def test_is_configured(table, column, expected) -> None:
    """is_configured should reflect whether any rules exist for a table/column."""
    policy = PiiPolicy(rules={"posts": {"body": {"MENTION": PiiRule(PiiAction.HASH)}}})
    assert policy.is_configured(table, column) is expected


def test_default_pii_policy_covers_expected_columns() -> None:
    """DEFAULT_PII_POLICY should scan the three known free-text columns."""
    assert DEFAULT_PII_POLICY.is_configured("accounts", "bio")
    assert DEFAULT_PII_POLICY.is_configured("communities", "bio")
    assert DEFAULT_PII_POLICY.is_configured("posts", "body")
    # Structured/identifier columns should never be PII-scanned.
    assert not DEFAULT_PII_POLICY.is_configured("accounts", "account_id")


def test_default_pii_policy_mention_action_is_hash() -> None:
    """MENTION must be pepper-hashed (not replaced) to preserve graph structure."""
    rule = DEFAULT_PII_POLICY.rule_for("posts", "body", "MENTION")
    assert rule.action == PiiAction.HASH


def test_default_pii_policy_url_rule_has_callable_replacement() -> None:
    """URL should reduce to a domain via a callable, not a fixed placeholder."""
    rule = DEFAULT_PII_POLICY.rule_for("posts", "body", "URL")
    assert rule.action == PiiAction.REPLACE
    assert callable(rule.replacement)
    assert rule.replacement("https://www.Example.com/x") == "URL_example.com"


def test_default_pii_policy_hashtag_lowercases_only_on_posts_body() -> None:
    """HASHTAG has a posts.body-specific override (lowercase, kept) not shared elsewhere."""
    rule = DEFAULT_PII_POLICY.rule_for("posts", "body", "HASHTAG")
    assert rule is not None
    assert rule.replacement("#ElectionNight") == "#electionnight"
    # accounts.bio has no HASHTAG rule at all (not part of the shared common rules).
    assert DEFAULT_PII_POLICY.rule_for("accounts", "bio", "HASHTAG") is None
