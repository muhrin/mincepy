"""Test global functions in mincepy"""
import mincepy


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
    car = mincepy.testing.Car()
    car_id = mincepy.save(car)
    del car
    # Now try loading
    mincepy.load(car_id)


def test_find():
    car = mincepy.testing.Car()
    car_id = car.save()
    assert list(mincepy.find(obj_id=car_id))[0] is car
