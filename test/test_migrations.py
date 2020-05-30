""""Tests of migrations"""
import logging

import mincepy
import mincepy.testing
from .common import CarV1, CarV2, CarV3, StoreByValue, StoreByRef


def test_simple_migration(historian: mincepy.Historian):
    car = CarV1('red', 'ferrari')
    car_id = historian.save(car)
    del car

    # Now change to version 2
    historian.register_type(CarV2)
    loaded_car = historian.load(car_id)
    assert loaded_car.colour == 'red'
    assert loaded_car.make == 'ferrari'


def test_multiple_migrations(historian: mincepy.Historian):
    car = CarV1('red', 'ferrari')
    car_id = historian.save(car)
    del car

    # Now change to version 3, skipping 2
    historian.register_type(CarV3)
    loaded_car = historian.load(car_id)
    assert loaded_car.colour == 'red'
    assert loaded_car.make == 'ferrari'
    assert hasattr(loaded_car, 'reg')
    assert loaded_car.reg == 'unknown'


def test_migrate_to_reference(historian: mincepy.Historian, caplog):
    caplog.set_level(logging.DEBUG)

    car = mincepy.testing.Car()
    by_val = StoreByValue(car)
    oid = historian.save(by_val)
    del by_val

    # Now, change my mind
    historian.register_type(StoreByRef)
    by_ref = historian.load(oid)
    assert isinstance(by_ref.ref, mincepy.testing.Car)
    assert by_ref.ref is not car
    loaded_car = by_ref.ref
    del by_ref

    # Reload and check that ref points to the same car
    loaded = historian.load(oid)
    assert loaded.ref is loaded_car


def test_dependent_migrations(historian: mincepy.Historian, caplog):
    """Test what happens when both a parent object and a contained object need migration"""
    caplog.set_level(logging.DEBUG)

    car = CarV1('red', 'honda')
    by_val = StoreByValue(car)

    oid = historian.save(by_val)
    del by_val

    loaded = historian.load(oid)
    assert isinstance(loaded.ref, CarV1)
    assert loaded.ref is not car
    del loaded

    # Now, change my mind
    historian.register_type(StoreByRef)  # Store by reference instead
    historian.register_type(CarV3)  # And update the car

    by_ref = historian.load(oid)
    assert isinstance(by_ref.ref, CarV3)
    loaded_car = by_ref.ref
    assert by_ref is not car

    # Now delete, load and check we have the same car
    del by_ref
    reloaded = historian.load(oid)
    assert reloaded.ref is loaded_car


def test_migrating_snapshot(historian: mincepy.Historian, caplog):
    """Test migrating an out of date snapshot"""
    caplog.set_level(logging.INFO)

    car = CarV1('yellow', 'bugatti')
    car_id = car.save()  # Version 0

    car.colour = 'brown'
    car.save()  # Version 1
    del car

    historian.register_type(CarV3)  # And update the car definition
    snapshot = historian.load_snapshot(mincepy.SnapshotId(car_id, 0))  # Load version 0

    assert snapshot.colour == 'yellow'
    assert snapshot.make == 'bugatti'

    # Now load the current version
    current = historian.load(car_id)
    assert current.colour == 'brown'
    assert current.make == 'bugatti'
