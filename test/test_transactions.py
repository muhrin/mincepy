"""Module for testing saved snapshots"""

import pytest

import mincepy
from mincepy import testing


def test_snapshot_id_in_transaction(historian: mincepy.Historian):
    """An object saved during a transaction should already have its new snapshot id available during
    the transaction."""
    car = testing.Car("ferrari", "red")
    car.save()
    sid = historian.get_snapshot_id(car)

    with historian.transaction():
        car.make = "honda"
        car.save()
        assert historian.get_snapshot_id(car) != sid


def test_nested_delete_hides_parent_object(historian: mincepy.Historian):
    """An object deleted in a nested transaction must not be found via the parent transaction"""
    car = testing.Car()
    car_id = car.save()
    with historian.transaction():
        car.colour = "blue"
        car.save()  # Now live in the outer transaction
        historian.meta.set(car, {"reg": "AB12"})
        with historian.transaction() as nested:
            historian.delete(car)
            with pytest.raises(mincepy.ObjectDeleted):
                nested.get_live_object(car_id)
            with pytest.raises(mincepy.ObjectDeleted):
                nested.get_meta(car_id)
            with pytest.raises(mincepy.ObjectDeleted):
                historian.load(car_id)

    with pytest.raises(mincepy.NotFound):
        historian.load(car_id)
