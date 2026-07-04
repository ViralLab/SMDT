from datetime import datetime, timezone

import pytest

from smdt.store.models.accounts import Accounts
from smdt.store.models.posts import Posts
from smdt.store.utils.geo import normalize_point

_NOW = datetime.now(timezone.utc)


def test_normalize_point_none_stays_none():
    assert normalize_point(None) is None


def test_normalize_point_tuple_produces_ewkt():
    """Regression test: this used to produce '(-122.4,37.8)' -- Postgres's
    native `point` type syntax -- which PostGIS's geometry(Point, 4326)
    columns reject outright (verified against live Postgres+PostGIS)."""
    assert normalize_point((-122.4, 37.8)) == "SRID=4326;POINT(-122.4 37.8)"


def test_normalize_point_list_produces_ewkt():
    assert normalize_point([-122.4, 37.8]) == "SRID=4326;POINT(-122.4 37.8)"


def test_normalize_point_passes_through_ewkt_string():
    ewkt = "SRID=4326;POINT(-73.9 40.7)"
    assert normalize_point(ewkt) == ewkt


def test_normalize_point_strips_whitespace_from_string():
    assert normalize_point("  SRID=4326;POINT(1 2)  ") == "SRID=4326;POINT(1 2)"


def test_normalize_point_empty_string_becomes_none():
    assert normalize_point("") is None


def test_normalize_point_invalid_tuple_values_return_none():
    assert normalize_point(("not-a-number", "also-not")) is None


@pytest.mark.parametrize("model_cls, kwargs", [
    (Posts, {"account_id": "a1"}),
    (Accounts, {}),
])
def test_model_normalizes_tuple_location_on_construction(model_cls, kwargs):
    instance = model_cls(created_at=_NOW, location=(-122.4, 37.8), **kwargs)
    assert instance.location == "SRID=4326;POINT(-122.4 37.8)"


@pytest.mark.parametrize("model_cls, kwargs", [
    (Posts, {"account_id": "a1"}),
    (Accounts, {}),
])
def test_model_passes_through_ewkt_string_location(model_cls, kwargs):
    ewkt = "SRID=4326;POINT(-73.9 40.7)"
    instance = model_cls(created_at=_NOW, location=ewkt, **kwargs)
    assert instance.location == ewkt


@pytest.mark.parametrize("model_cls, kwargs", [
    (Posts, {"account_id": "a1"}),
    (Accounts, {}),
])
def test_model_location_none_stays_none(model_cls, kwargs):
    instance = model_cls(created_at=_NOW, location=None, **kwargs)
    assert instance.location is None
