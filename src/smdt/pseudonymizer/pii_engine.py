"""Presidio-based PII detection and redaction engine."""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from .pii_policy import PiiAction, PiiPolicy, PiiRule
from .platform_patterns import platform_recognizers
from .pseudonyms import Hasher

if TYPE_CHECKING:
    from presidio_analyzer import EntityRecognizer

log = logging.getLogger(__name__)


def _require_presidio():
    try:
        from presidio_analyzer import AnalyzerEngine
        from presidio_anonymizer import AnonymizerEngine
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "presidio-analyzer and presidio-anonymizer are required for PII "
            "detection. Install with: pip install 'smdt[pii]'"
        ) from e
    return AnalyzerEngine, AnonymizerEngine


class PiiEngine:
    """Wraps Presidio's Analyzer+Anonymizer for platform-aware PII redaction.

    Three recognizer sources feed one analysis pass:
      1. Presidio's built-in generic recognizers (phone, credit card, IBAN,
         IP, person name via NER, etc.) — identifier-grade PII only, no
         GDPR Art. 9 special-category (health/political/religious/...)
         detection.
      2. Built-in per-platform MENTION/HASHTAG PatternRecognizers, selected by
         the row's `platform` (see platform_patterns.py).
      3. User-supplied custom PatternRecognizers, scoped per (table, column).

    A PiiPolicy then resolves, per (table, column, entity_type), whether to
    HASH (pepper-keyed via the shared Hasher — consistent with account_id/
    username hashing), REPLACE (placeholder string or callable transform), or
    DROP each detected span.

    There is deliberately no automatic language detection: the caller
    configures which language(s)/NLP models to use via `nlp_configuration`
    (see presidio_analyzer.nlp_engine.NlpEngineProvider for the expected
    shape), and this class warns about coverage gaps at construction time.
    """

    def __init__(
        self,
        hasher: Hasher,
        nlp_configuration: Optional[Dict[str, Any]] = None,
        custom_recognizers: Optional[Dict[Tuple[str, str], List["EntityRecognizer"]]] = None,
        language: str = "en",
    ):
        """Initialize the PiiEngine.

        Args:
            hasher: Shared Hasher used for the HASH action, so PII hashed here
                is consistent with account_id/username pseudonyms elsewhere.
            nlp_configuration: Presidio NlpEngineProvider configuration dict,
                e.g. {"nlp_engine_name": "spacy", "models": [{"lang_code": "en",
                "model_name": "en_core_web_lg"}]}. If omitted, Presidio's own
                default NLP engine is used and a coverage warning is logged.
            custom_recognizers: Extra PatternRecognizers scoped per
                (table, column), layered on top of the built-in platform ones.
            language: Default language passed to the analyzer per call.
        """
        AnalyzerEngine, AnonymizerEngine = _require_presidio()

        self.hasher = hasher
        self.language = language
        self.custom_recognizers = custom_recognizers or {}

        nlp_engine = None
        supported_languages = [language]
        if nlp_configuration:
            from presidio_analyzer.nlp_engine import NlpEngineProvider

            nlp_engine = NlpEngineProvider(
                nlp_configuration=nlp_configuration
            ).create_engine()
            supported_languages = [
                m["lang_code"] for m in nlp_configuration.get("models", [])
            ] or supported_languages

        self.analyzer = AnalyzerEngine(
            nlp_engine=nlp_engine, supported_languages=supported_languages
        )
        self.anonymizer = AnonymizerEngine()

        self._warn_language_coverage(nlp_configuration, supported_languages)

    def _warn_language_coverage(
        self, nlp_configuration: Optional[Dict[str, Any]], languages: List[str]
    ) -> None:
        """Emit a coverage-gap warning based on the configured language(s)."""
        if not nlp_configuration:
            log.warning(
                "PiiEngine built without an explicit nlp_configuration; falling "
                "back to Presidio's default NLP engine/model. NER-based entities "
                "(PERSON, LOCATION) may not be detected correctly outside that "
                "default model's language. Pattern-based recognizers "
                "(PHONE_NUMBER, EMAIL_ADDRESS, CREDIT_CARD, IBAN_CODE, MENTION, "
                "HASHTAG, ...) are language-agnostic and unaffected."
            )
            return
        log.warning(
            "PiiEngine configured for language(s) %s only: NER-based entities "
            "(PERSON, LOCATION) will not be detected correctly in other "
            "languages present in your dataset. Pattern-based recognizers work "
            "regardless of language. Configure additional models via "
            "`nlp_configuration` if you need broader NER coverage.",
            languages,
        )

    def redact(
        self,
        text: Optional[str],
        table: str,
        column: str,
        policy: PiiPolicy,
        platform: Optional[str] = None,
        language: Optional[str] = None,
    ) -> Optional[str]:
        """Detect and redact configured PII entity types in `text`.

        Args:
            text: Free text to scan (e.g. a post body or account bio).
            table: Table name (policy lookup key).
            column: Column name (policy lookup key).
            policy: PiiPolicy governing what to detect and how to handle it.
            platform: Canonical source platform, for MENTION/HASHTAG pattern
                selection (see platform_patterns.py).
            language: Override the engine's default language for this call.

        Returns:
            Redacted text. Returns `text` unchanged if it's None/empty, or if
            no entity types are configured for this table/column.
        """
        if not text:
            return text

        entities = policy.entities_for(table, column)
        if not entities:
            return text

        entity_set = set(entities)
        ad_hoc = list(platform_recognizers(platform)) + list(
            self.custom_recognizers.get((table, column), [])
        )
        # Only keep ad-hoc recognizers whose entity type is actually requested,
        # so enabling e.g. PHONE_NUMBER doesn't accidentally also pull in
        # MENTION detection nobody asked for on this column.
        ad_hoc = [
            r for r in ad_hoc if set(r.supported_entities or []) & entity_set
        ]

        results = self.analyzer.analyze(
            text=text,
            language=language or self.language,
            entities=entities,
            ad_hoc_recognizers=ad_hoc,
        )
        if not results:
            return text

        operators = {}
        for entity_type in entity_set:
            rule = policy.rule_for(table, column, entity_type)
            if rule is not None:
                operators[entity_type] = self._operator_config(entity_type, rule)

        outcome = self.anonymizer.anonymize(
            text=text, analyzer_results=results, operators=operators
        )
        return outcome.text

    def _operator_config(self, entity_type: str, rule: PiiRule) -> Any:
        """Translate a PiiRule into a Presidio OperatorConfig."""
        from presidio_anonymizer.entities import OperatorConfig

        if rule.action == PiiAction.HASH:
            if entity_type == "MENTION":
                # PatternRecognizer match spans are whole-regex-match (no capture
                # group support), so the matched text includes the leading '@'.
                # Strip it before hashing so "@JohnDoe" in free text hashes
                # identically to accounts.username="JohnDoe" (Hasher lowercases
                # both), and restore a readable "@u_<hash>" marker on output.
                def _hash_mention(matched: str) -> str:
                    return "@u_" + (self.hasher.make(matched.lstrip("@")) or "")

                return OperatorConfig("custom", {"lambda": _hash_mention})
            return OperatorConfig("custom", {"lambda": self.hasher.make})
        if rule.action == PiiAction.DROP:
            return OperatorConfig("redact", {})
        # REPLACE
        if callable(rule.replacement):
            return OperatorConfig("custom", {"lambda": rule.replacement})
        new_value = rule.replacement or f"[{entity_type}]"
        return OperatorConfig("replace", {"new_value": new_value})
