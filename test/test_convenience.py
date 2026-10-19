from urllib import parse

import pytest

import mincepy
import mincepy.archive_factory
import mincepy.testing
from mincepy.testing import Car


def test_load(historian: mincepy.Historian):
    car = Car()
    car_id = historian.save(car)

    assert mincepy.load(car_id) is car

    # Check loading from 'cold'
    del car
    car = mincepy.load(car_id)
    assert car._historian is historian


def test_save(historian: mincepy.Historian):
    car = Car()
    mincepy.save(car)
    assert car._historian is historian


def test_invalid_connect():
    """Check we get the right error when attempting to connect to invalid archive"""
    with pytest.raises(mincepy.ConnectionError):
        mincepy.connect("mongodb://unknown-server/db", timeout=5)

    with pytest.raises(ValueError):
        mincepy.connect("unknown-protocol://nowhere", timeout=5)


@pytest.mark.parametrize("empty_uri", ["", parse.urlparse("")])
def test_connect_default_uri(empty_uri, monkeypatch, archive_uri):
    """Without a URI, connect() should use the default (MINCEPY_ARCHIVE), also when given an empty
    parsed URI"""
    monkeypatch.setenv(mincepy.archive_factory.ENV_ARCHIVE_URI, archive_uri)
    historian = mincepy.connect(empty_uri)
    car = mincepy.testing.Car()
    historian.save(car)
    assert mincepy.connect(archive_uri).load(car.obj_id) == car
