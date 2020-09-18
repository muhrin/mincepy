import pytest

import mincepy
from mincepy import testing
from mincepy.testing import Car


def test_transaction_snapshots(historian: mincepy.Historian):
    ferrari = Car('ferrari')
    historian.save(ferrari)
    ferrari_sid = historian.get_snapshot_id(ferrari)

    with historian.transaction():
        ferrari_snapshot_1 = historian.load_snapshot(ferrari_sid)
        with historian.transaction():
            ferrari_snapshot_2 = historian.load_snapshot(ferrari_sid)
            # Reference wise they should be unequal
            assert ferrari_snapshot_1 is not ferrari_snapshot_2
            assert ferrari is not ferrari_snapshot_1
            assert ferrari is not ferrari_snapshot_2

            # Value wise they should be equal
            assert ferrari == ferrari_snapshot_1
            assert ferrari == ferrari_snapshot_2

        # Now check within the same transaction the result is the same
        ferrari_snapshot_2 = historian.load_snapshot(ferrari_sid)
        # Reference wise they should be unequal
        assert ferrari_snapshot_1 is not ferrari_snapshot_2
        assert ferrari is not ferrari_snapshot_1
        assert ferrari is not ferrari_snapshot_2

        # Value wise they should be equal
        assert ferrari == ferrari_snapshot_1
        assert ferrari == ferrari_snapshot_2


def test_transaction_records(historian: mincepy.Historian):
    """Make sure that records within a transaction are not recreated at each save"""
    with historian.transaction():
        ferrari = Car('ferrari')

        # Save and get the record for the ferrari
        ferrari_id = historian.save(ferrari)
        record = historian.get_current_record(ferrari)

        ferrari_id2 = historian.save(ferrari)
        assert ferrari_id == ferrari_id2
        assert historian.get_current_record(ferrari) is record

        loaded = historian.load(ferrari_id)
        assert loaded is ferrari


def test_find(historian: mincepy.Historian):
    honda_id = historian.save(Car('honda'))
    zonda_id = historian.save(Car('zonda'))
    porsche_id = historian.save(Car('porsche'))

    cars = list(historian.find(Car))
    assert len(cars) == 3

    makes = [car.make for car in cars]
    assert 'honda' in makes
    assert 'zonda' in makes
    assert 'porsche' in makes

    obj_ids = [car.obj_id for car in cars]
    assert honda_id in obj_ids
    assert zonda_id in obj_ids
    assert porsche_id in obj_ids


def test_update(historian: mincepy.Historian):
    car = Car('ferrari', 'red')
    historian.save_one(car)

    # Simulate saving the car from another connection
    honda = Car('honda', 'black')
    historian.save_one(honda)
    honda_record = historian.get_current_record(honda)

    archive = historian.archive
    record = historian.get_current_record(car)
    builder = record.child_builder(obj_id=historian.get_obj_id(car),
                                   snapshot_hash=honda_record.snapshot_hash,
                                   state=honda_record.state,
                                   state_types=honda_record.state_types)
    archive.save(builder.build())

    # Now update and check the state
    historian.sync(car)
    assert car.make == 'honda'
    assert car.colour == 'black'

    # Also, check the cached record
    car_record = historian.get_current_record(car)
    assert car_record.snapshot_hash == honda_record.snapshot_hash
    assert car_record.state == honda_record.state


def test_to_obj_id(historian: mincepy.Historian):
    car = Car()
    car_id = car.save()

    assert historian.to_obj_id(car_id) == car_id
    assert historian.to_obj_id(car) == car_id
    assert historian.to_obj_id(str(car_id)) == car_id
    assert historian.to_obj_id('carrot') is None


def test_copy(historian: mincepy.Historian):
    car = Car('zonda')

    historian.save(car)
    car_copy = mincepy.copy(car)
    assert car == car_copy
    assert car is not car_copy
    car_copy.save()

    record = historian.get_current_record(car)
    copy_record = historian.get_current_record(car_copy)

    assert record is not copy_record
    assert copy_record.get_copied_from() == record.snapshot_id


def test_copy_unsaved(historian: mincepy.Historian):
    car = Car('porsche', 'silver')
    car_copy = mincepy.copy(car)

    assert car_copy is not car
    assert car == car_copy

    # The cars should not be saved
    assert not historian.is_known(car)
    assert not historian.is_known(car_copy)


def test_save(historian: mincepy.Historian):
    car = Car('porsche', 'yellow')
    with pytest.raises(ValueError):
        historian.save((car, {'speed': 'fast'}, 124))


def test_is_trackable(historian: mincepy.Historian):
    assert historian.is_trackable(mincepy.testing.Car) is True
    assert historian.is_trackable(5) is False
    assert historian.is_trackable(5.6) is False
    assert historian.is_trackable('hello') is False
    assert historian.is_trackable(False) is False
    assert historian.is_trackable(b'byte me') is False


def test_delete_referenced(historian: mincepy.Historian):
    car = testing.Car()
    garage = testing.Garage(mincepy.ObjRef(car))
    garage.save()

    # Should be prevented from deleting the car as it is referenced by the garage
    try:
        historian.delete(car)
    except mincepy.ReferenceError as exc:
        assert exc.references == {car.obj_id}
    else:
        assert "Reference error should have been raised"

    historian.delete(garage)
    # Now safe to delete car
    historian.delete(car)

    car = testing.Car()
    garage = testing.Garage(mincepy.ObjRef(car))
    garage.save()

    # Now, check that deleting both together works
    historian.delete(car, garage)
