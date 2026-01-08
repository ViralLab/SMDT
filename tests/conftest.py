"""
Root pytest configuration for the SMDT test suite.

Test structure:
  tests/unit/         - Fast unit tests, no external dependencies
  tests/integration/  - Tests requiring database or external services

Run selectively:
  pytest tests/unit                      # unit tests only (fast, CI default)
  pytest tests/integration               # integration tests (requires DB)
  pytest -m "not integration"            # exclude integration tests
  pytest                                 # everything
"""
import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test (requires external services like DB)",
    )
