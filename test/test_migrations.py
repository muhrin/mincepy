""""Tests of migration"""
import gc
import logging
import uuid

import mincepy
import mincepy.testing


class CarV1(mincepy.SimpleSavable):
    TYPE_ID = uuid.UUID('297808e4-9bc7-4f0a-9f8d-850a5f558663')
    ATTRS = ('colour', 'make')

    def __init__(self, colour: str, make: str):
        super(CarV1, self).__init__()
        self.colour = colour
        self.make = make

    def save_instance_state(self, saver: mincepy.Saver):
        super(CarV1, self).save_instance_state(saver)
        # Here, I decide to store as an array
        return [self.colour, self.make]

    def load_instance_state(self, saved_state, loader: mincepy.Loader):
        self.colour = saved_state[0]
        self.make = saved_state[1]


class CarV2(CarV1):

    class V1toV2(mincepy.ObjectMigration):
        VERSION = 1

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader') -> dict:
            return dict(colour=saved_state[0], make=saved_state[1])

    # Set the migration
    LATEST_MIGRATION = V1toV2

    def save_instance_state(self, saver: mincepy.Saver):
        # I've changed my mind, I'd like to store it as a dict
        return dict(colour=self.colour, make=self.make)

    def load_instance_state(self, saved_state, loader: mincepy.Loader):
        self.colour = saved_state['colour']
        self.make = saved_state['make']


class CarV3(CarV2):

    class V2toV3(mincepy.ObjectMigration):
        VERSION = 1
        PREVIOUS = CarV2.V1toV2

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader') -> dict:
            # Augment the saved state
            saved_state['reg'] = 'unknown'
            return saved_state

    # Set the migration
    LATEST_MIGRATION = V2toV3

    def __init__(self, colour: str, make: str, reg=None):
        super().__init__(colour, make)
        self.reg = reg

    def save_instance_state(self, saver: mincepy.Saver):
        # I've changed my mind, I'd like to store it as a dict
        return dict(colour=self.colour, make=self.make, reg=self.reg)

    def load_instance_state(self, saved_state, loader: mincepy.Loader):
        self.colour = saved_state['colour']
        self.make = saved_state['make']
        self.reg = saved_state['reg']


class StoreByValue(mincepy.SimpleSavable):
    ATTRS = ('ref',)
    TYPE_ID = uuid.UUID('40377bfc-901c-48bb-a85c-1dd692cddcae')

    def __init__(self, ref):
        super().__init__()
        self.ref = ref


class StoreByRef(StoreByValue):

    class ToRefMigration(mincepy.ObjectMigration):
        VERSION = 1

        @classmethod
        def upgrade(cls, saved_state, loader: 'mincepy.Loader') -> dict:
            # Replace the value stored version with a reference
            saved_state['ref'] = mincepy.ObjRef(saved_state['ref'])
            return saved_state

    # Changed my mind, want to store by value now
    ATTRS = (mincepy.AsRef('ref'),)
    LATEST_MIGRATION = ToRefMigration


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


def test_migrate_to_reference(historian: mincepy.Historian):
    car = mincepy.testing.Car()
    by_val = StoreByValue(car)

    oid = historian.save(by_val)
    del by_val

    loaded = historian.load(oid)
    assert isinstance(loaded.ref, mincepy.testing.Car)
    assert loaded.ref is not car
    del loaded
    gc.collect()  # Force garbage collection

    # Now, change my mind
    historian.register_type(StoreByRef)
    by_ref = historian.load(oid)
    assert isinstance(by_ref.ref, mincepy.ObjRef)
    assert isinstance(by_ref.ref(), mincepy.testing.Car)
    assert by_ref.ref() is not car


def test_dependent_migrations(historian: mincepy.Historian):
    """Test what happens when both a parent object and a contained object need migration"""
    car = CarV1('red', 'honda')
    by_val = StoreByValue(car)

    oid = historian.save(by_val)
    del by_val

    loaded = historian.load(oid)
    assert isinstance(loaded.ref, CarV1)
    assert loaded.ref is not car
    del loaded
    gc.collect()  # Force garbage collection

    # Now, change my mind
    historian.register_type(StoreByRef)  # Store by reference instead
    historian.register_type(CarV3)  # And update the car

    by_ref = historian.load(oid)
    assert isinstance(by_ref.ref, mincepy.ObjRef)
    assert isinstance(by_ref.ref(), CarV3)
    assert by_ref.ref() is not car


def test_migrating_snapshot(historian: mincepy.Historian, caplog):
    """Test migrating an out of date snapshot"""
    caplog.set_level(logging.INFO)

    car = CarV1('yellow', 'bugatti')
    car_id = car.save()  # Version 0

    car.colour = 'brown'
    car.save()  # Version 1
    del car

    historian.register_type(CarV3)  # And update the car definition
    snapshot = historian.load_snapshot(mincepy.SnapshotRef(car_id, 0))  # Load version 0

    assert snapshot.colour == 'yellow'
    assert snapshot.make == 'bugatti'

    # Now load the current version
    current = historian.load(car_id)
    assert current.colour == 'brown'
    assert current.make == 'bugatti'
