import pytest

from smdt.standardizers.base import SourceInfo, Standardizer


def test_sourceinfo_defaults() -> None:
    """SourceInfo should store path and default None for optional fields."""
    result = SourceInfo("/some/path")
    assert result.path == "/some/path"
    assert result.member is None
    assert result.hints is None


def test_standardizer_standardize_raises() -> None:
    """Calling Standardizer.standardize should raise NotImplementedError by default."""
    s = Standardizer()
    with pytest.raises(NotImplementedError):
        s.standardize({})
