import time

import pytest

import mincepy
from mincepy.testing import Car, Cycle


def test_list_basics(historian: mincepy.Historian):
    parking_lot = mincepy.builtins.List()
    for i in range(1000):
        parking_lot.append(Car(str(i)))

    list_id = historian.save(parking_lot, return_sref=True)

    # Change one element
    parking_lot[0].make = 'ferrari'
    new_list_id = historian.save(parking_lot, return_sref=True)

    assert list_id != new_list_id

    old_list = historian.load_snapshot(list_id)
    assert old_list is not parking_lot

    assert old_list[0].make == str(0)


def test_save_snapshot_change_load(historian: mincepy.Historian):
    car = Car()

    # Saving twice without changing should produce the same snapshot id
    ferrari_ref = historian.save(car, return_sref=True)
    assert ferrari_ref == historian.save(car, return_sref=True)

    car.make = 'fiat'
    car.color = 'white'

    fiat_ref = historian.save(car, return_sref=True)

    assert fiat_ref != ferrari_ref

    ferrari = historian.load_snapshot(ferrari_ref)

    assert ferrari.make == 'ferrari'
    assert ferrari.colour == 'red'


def test_transaction_snapshots(historian: mincepy.Historian):
    ferrari = Car('ferrari')
    ferrari_id = historian.save(ferrari, return_sref=True)

    with historian.transaction():
        ferrari_snapshot_1 = historian.load_snapshot(ferrari_id)
        with historian.transaction():
            ferrari_snapshot_2 = historian.load_snapshot(ferrari_id)
            # Reference wise they should be unequal
            assert ferrari_snapshot_1 is not ferrari_snapshot_2
            assert ferrari is not ferrari_snapshot_1
            assert ferrari is not ferrari_snapshot_2

            # Value wise they should be equal
            assert ferrari == ferrari_snapshot_1
            assert ferrari == ferrari_snapshot_2

        # Now check within the same transaction the result is the same
        ferrari_snapshot_2 = historian.load_snapshot(ferrari_id)
        # Reference wise they should be unequal
        assert ferrari_snapshot_1 is not ferrari_snapshot_2
        assert ferrari is not ferrari_snapshot_1
        assert ferrari is not ferrari_snapshot_2

        # Value wise they should be equal
        assert ferrari == ferrari_snapshot_1
        assert ferrari == ferrari_snapshot_2


def test_record_times(historian: mincepy.Historian):
    car = Car('honda', 'red')
    historian.save(car)
    time.sleep(0.001)

    car.colour = 'white'
    historian.save(car)
    time.sleep(0.01)

    car.colour = 'yellow'
    historian.save(car)

    history = historian.history(car, as_objects=False)
    assert len(history) == 3
    for idx, record in zip(range(1, 2), history[1:]):
        assert record.creation_time == history[0].creation_time
        assert record.snapshot_time > history[0].creation_time
        assert record.snapshot_time > history[idx - 1].snapshot_time


def test_get_latest(historian: mincepy.Historian):
    # Save the car
    car = Car()
    car_id = historian.save(car)

    # Change it and save getting a snapshot
    car.make = 'fiat'
    car.colour = 'white'
    fiat_id = historian.save(car, return_sref=True)
    assert car_id != fiat_id

    # Change it again...
    car.make = 'honda'
    car.colour = 'wine red'
    honda_id = historian.save(car, return_sref=True)
    assert honda_id != fiat_id
    assert honda_id != car_id

    # Now delete and reload
    del car
    latest = historian.load(car_id)
    assert latest == historian.load_snapshot(honda_id)


def test_history(historian: mincepy.Historian):
    rainbow = ['red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet']

    car = Car()
    car_id = None
    for colour in rainbow:
        car.colour = colour
        car_id = historian.save(car)

    car_history = historian.history(car_id)
    assert len(car_history) == len(rainbow)
    for i, entry in enumerate(car_history):
        assert entry[1].colour == rainbow[i]

    # Test loading directly from snapshot id
    assert historian.load_snapshot(car_history[2].ref) == car_history[2].obj

    # Test slicing
    assert historian.history(car_id, -1)[0].obj.colour == rainbow[-1]

    # Try changing history
    old_version = car_history[2].obj
    old_version.colour = 'black'
    with pytest.raises(mincepy.ModificationError):
        historian.save(old_version)


def test_loading_snapshot(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    white_honda_ref = historian.save(honda, return_sref=True)
    honda.colour = 'red'
    historian.save(honda, return_sref=True)
    del honda

    with historian.transaction():
        white_honda = historian.load_snapshot(white_honda_ref)
        assert white_honda.colour == 'white'
        # Make sure that if we load it again we get a different object instance
        assert white_honda is not historian.load_snapshot(white_honda_ref)


def test_loading_snapshot_cycle(historian: mincepy.Historian):
    a = Cycle()
    a.ref = a  # Close the cycle
    a_ref = historian.save(a, return_sref=True)
    del a

    loaded = historian.load_snapshot(a_ref)
    assert loaded.ref is loaded
