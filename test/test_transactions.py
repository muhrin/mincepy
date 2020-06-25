"""Module for testing saved snapshots"""

import mincepy
from mincepy import testing


def test_snapshot_id_in_transaction(historian: mincepy.Historian):
    """An object saved during a transaction should already have its new snapshot id available during
    the transaction."""
    car = testing.Car('ferrari', 'red')
    car.save()
    sid = historian.get_snapshot_id(car)

    with historian.transaction():
        car.make = 'honda'
        car.save()
        assert historian.get_snapshot_id(car) != sid
