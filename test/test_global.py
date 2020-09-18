"""Test global functions in mincepy"""
import mincepy
from mincepy import testing


def test_archive_uri(monkeypatch):
    monkeypatch.delenv(mincepy.ENV_ARCHIVE_URI, raising=False)
    assert mincepy.archive_uri() == mincepy.DEFAULT_ARCHIVE_URI
    monkeypatch.setenv(mincepy.ENV_ARCHIVE_URI, "mongodb://example.com")
    assert mincepy.archive_uri() == "mongodb://example.com"


def test_default_archive_uri(monkeypatch):
    monkeypatch.delenv(mincepy.ENV_ARCHIVE_URI, raising=False)
    assert mincepy.default_archive_uri() == mincepy.DEFAULT_ARCHIVE_URI
    monkeypatch.setenv(mincepy.ENV_ARCHIVE_URI, "mongodb://example.com")
    assert mincepy.default_archive_uri() == "mongodb://example.com"


def test_save_load():
    car = testing.Car()
    car_id = mincepy.save(car)
    del car
    # Now try loading
    mincepy.load(car_id)


def test_find():
    car = testing.Car()
    car_id = car.save()
    assert list(mincepy.find(obj_id=car_id))[0] is car


def test_delete():
    car = testing.Car()
    car_id = car.save()
    mincepy.delete(car)
    assert not list(mincepy.find(obj_id=car_id))
