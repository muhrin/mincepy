# -*- coding: utf-8 -*-
import pytest

import mincepy
from mincepy.testing import Car


def test_load(historian: mincepy.Historian):
    car = Car()
    car_id = historian.save(car)

    assert mincepy.load(car_id) is car

    # Check loading from 'cold'
    del car
    car = mincepy.load(car_id)
    assert car._historian is historian  # pylint: disable=protected-access


def test_save(historian: mincepy.Historian):
    car = Car()
    mincepy.save(car)
    assert car._historian is historian  # pylint: disable=protected-access


def test_invalid_connect():
    """Check we get the right error when attempting to connect to invalid archive"""
    with pytest.raises(mincepy.ConnectionError):
        mincepy.connect("mongodb://unknown-server/db", timeout=5)

    with pytest.raises(ValueError):
        mincepy.connect("unknown-protocol://nowhere", timeout=5)
