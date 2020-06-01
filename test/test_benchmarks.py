try:
    from contextlib import nullcontext
except ImportError:
    from contextlib2 import nullcontext

import pytest

import mincepy
from mincepy.testing import Car
import mincepy.testing
from . import utils


def insert_cars(historian: mincepy.Historian, num=100, in_transaction=False):
    """Insert a number of cars into the database, optionally in a transaction so they all get
    inserted in one go."""
    if in_transaction:
        ctx = historian.transaction()
    else:
        ctx = nullcontext()

    with ctx:
        for _ in range(num):
            historian.save(mincepy.testing.Car())


def find(historian, **kwargs):
    # Have to wrap the find method like this because it returns a generator and won't necessarily
    # fetch from the db unless we iterate it
    return tuple(historian.find(**kwargs))


def test_benchmark_insertions_individual(historian: mincepy.Historian, benchmark):
    benchmark(insert_cars, historian, in_transaction=False)


def test_benchmark_insertions_transaction(historian: mincepy.Historian, benchmark):
    benchmark(insert_cars, historian, in_transaction=True)


@pytest.mark.parametrize("num", [10**i for i in range(5)])
def test_find_cars(historian: mincepy.Historian, benchmark, num):
    """Test finding a car as a function of the number of entries in the database"""
    # Put in the one we want to find
    historian.save(Car('honda', 'green'))

    # Put in the correct number of random other entries
    for _ in range(num):
        historian.save(Car(utils.random_str(10), utils.random_str(5)))

    result = benchmark(find, historian, state=dict(make='honda', colour='green'))
    assert len(result) == 1


@pytest.mark.parametrize("num", [5**i for i in range(1, 4)])
def test_load_cars(historian: mincepy.Historian, benchmark, num):
    """Test finding a car as a function of the number of entries in the database"""
    # Put in the correct number of random other entries
    car_ids = []
    for _ in range(num):
        car_ids.append(historian.save(Car(utils.random_str(10), utils.random_str(5))))

    result = benchmark(historian.load, *car_ids)
    assert len(result) == len(car_ids)
