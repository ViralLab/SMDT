from smdt.standardizers.row import Record


def _make_record():
    keys = ["id", "text", "user"]
    index = {k: i for i, k in enumerate(keys)}
    values = (123, "hello", {"name": "alice"})
    return Record(values, index, keys)


def test_record_getitem() -> None:
    """Record.__getitem__ should return the correct value for an existing key."""
    rec = _make_record()
    result = rec["text"]
    assert result == "hello"


def test_record_get_default_for_missing() -> None:
    """Record.get should return the provided default when key is missing."""
    rec = _make_record()
    result = rec.get("missing", "def")
    assert result == "def"


def test_record_to_dict() -> None:
    """Record.to_dict should materialize a dict with all keys and values."""
    rec = _make_record()
    result = rec.to_dict()
    assert isinstance(result, dict)
    assert result["id"] == 123


def test_record_repr_contains_keys() -> None:
    """The repr of Record should include its keys and their repr'ed values."""
    rec = _make_record()
    result = repr(rec)
    assert '"id":' in result
    assert '"text":' in result


def test_record_len_and_iter() -> None:
    """len(record) should reflect number of keys and iteration yields keys."""
    rec = _make_record()
    result = len(rec)
    assert result == 3
