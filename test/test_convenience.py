import mincepy
from mincepy.testing import Car


def test_load(historian: mincepy.Historian):
    car = Car()
    car_id = historian.save(car)

    assert mincepy.load(car_id) is car

    # Check loading from 'cold'
    del car
    car = mincepy.load(car_id)
    assert car._historian == historian


def test_save(historian: mincepy.Historian):
    car = Car()
    mincepy.save(car)
    assert car._historian is historian
