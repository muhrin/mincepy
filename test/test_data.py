import time
import uuid

import pytest

import mincepy
from mincepy.testing import Car, Garage


def test_basic_save_load(historian: mincepy.Historian):
    car = Car('nissan', 'white')

    car_id = historian.save(car)
    del car
    loaded_car = historian.load(car_id)

    assert loaded_car.make == 'nissan'
    assert loaded_car.colour == 'white'


def test_save_snapshot_change_load(historian: mincepy.Historian):
    car = Car()

    # Saving twice without changing should produce the same snapshot id
    ferrari_id = historian.save_snapshot(car)
    assert ferrari_id == historian.save_snapshot(car)

    car.make = 'fiat'
    car.color = 'white'

    fiat_id = historian.save_snapshot(car)

    assert fiat_id != ferrari_id

    ferrari = historian.load_snapshot(ferrari_id)

    assert ferrari.make == 'ferrari'
    assert ferrari.colour == 'red'


def test_nested_references(historian: mincepy.Historian):
    car = Car()
    garage = Garage(car)

    car_id = historian.save(car)
    garage_id = historian.save(garage)

    # Now change the car
    car.make = 'fiat'
    car.colour = 'white'

    historian.save(car)
    # Try loading while the object is still alive
    loaded_garage = historian.load(garage_id)
    assert loaded_garage is garage

    # Now delete and load
    del garage
    del car
    loaded_garage2 = historian.load(garage_id)
    # Should be the last version fo the car
    assert loaded_garage2.car.make == 'fiat'

    assert len(historian.history(car_id)) == 2
    # The following may seem counter intuitive that we only have one history
    # entry for garage.  But above we only saved it once.  It's just that when
    # we load the garage again we get the 'latest' version it's contents i.e.
    # the newer version of the car
    assert len(historian.history(garage_id)) == 1


def test_create_delete_load(historian: mincepy.Historian):
    car = Car('honda', 'red')
    car_id = historian.save(car)
    del car

    loaded_car = historian.load(car_id)
    assert loaded_car.make == 'honda'
    assert loaded_car.colour == 'red'


def test_list_basics(historian: mincepy.Historian):
    parking_lot = mincepy.builtins.List()
    for i in range(1000):
        parking_lot.append(Car(str(i)))

    list_id = historian.save_snapshot(parking_lot)

    # Change one element
    parking_lot[0].make = 'ferrari'
    new_list_id = historian.save_snapshot(parking_lot)

    assert list_id != new_list_id

    old_list = historian.load_snapshot(list_id)
    assert old_list is not parking_lot

    assert old_list[0].make == str(0)


def test_track(historian: mincepy.Historian):

    @mincepy.track
    def put_car_in_garage(car: Car, garage: Garage):
        garage.car = car
        return garage

    mincepy.set_historian(historian)

    ferrari = Car('ferrari', 'red')
    garage = Garage()
    put_car_in_garage(ferrari, garage)
    assert garage.car is ferrari


def test_track_method(historian: mincepy.Historian):

    class CarFactory(mincepy.Archivable):
        TYPE_ID = uuid.UUID('166a9446-c04e-4fbe-a3da-6f36c2f8292d')
        ATTRS = ('_make',)

        def __init__(self, make):
            super(CarFactory, self).__init__()
            self._make = make

        @mincepy.track
        def build(self):
            return Car(self._make)

    mincepy.set_historian(historian)

    car_factory = CarFactory('zonda')
    car = car_factory.build()

    build_call = next(historian.find(mincepy.FunctionCall, limit=1))
    assert build_call.args[0] is car_factory
    assert build_call.result() is car


def test_get_latest(historian: mincepy.Historian):
    # Save the car
    car = Car()
    car_id = historian.save(car)

    # Change it and save getting a snapshot
    car.make = 'fiat'
    car.colour = 'white'
    fiat_id = historian.save_snapshot(car)
    assert car_id != fiat_id

    # Change it again...
    car.make = 'honda'
    car.colour = 'wine red'
    honda_id = historian.save_snapshot(car)
    assert honda_id != fiat_id
    assert honda_id != car_id

    # Now delete and reload
    del car
    latest = historian.load(car_id)
    assert latest == historian.load_snapshot(honda_id)


# def test_save_as(historian: mincepy.Historian):
#     """Check the save_as functionality in historian"""
#     car = Car('ferrari')
#
#     car_id = historian.save(car)
#     assert historian.get_current_record(car) is not None
#
#     # Now create a new car and save that over the old one
#     new_car = Car('honda')
#
#     new_car_id = historian.save_as(new_car, car_id)
#     assert car_id == new_car_id
#
#     with pytest.raises(mincepy.NotFound):
#         # Now that 'old' car should not be known to the historian
#         historian.get_current_record(car)
#
#     assert historian.get_current_record(new_car) is not None


def test_type_helper(historian: mincepy.Historian):
    """Check that a type helper can be used to make a non-historian compatible type compatible"""

    class Bird:

        def __init__(self, specie='hoopoe'):
            self.specie = specie

    class BirdHelper(mincepy.TypeHelper):
        TYPE = Bird
        TYPE_ID = uuid.UUID('5cc59e03-ea5d-43ff-8814-3b6f2e22cd76')

        def yield_hashables(self, obj, hasher):
            yield from hasher.yield_hashables(obj.specie)

        def eq(self, one, other) -> bool:
            return one.specie == other.specie

        def save_instance_state(self, obj, _depositor):
            return obj.specie

        def load_instance_state(self, obj, saved_state, _depositor):
            obj.specie = saved_state

    bird = Bird()
    with pytest.raises(TypeError):
        historian.save(bird)

    # Now register the helper...
    historian.register_type(BirdHelper())
    # ...and we should be able to save
    assert historian.save(bird) is not None


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


def test_storing_internal_object(historian: mincepy.Historian):

    class Person(mincepy.SavableObject):
        TYPE_ID = uuid.UUID('f6f83595-6375-4bc4-89f2-d8f31a1286b0')

        def __init__(self, car):
            super(Person, self).__init__()
            self.car = car  # This person 'owns' the car

        def __eq__(self, other):
            return self.car == other.car

        def yield_hashables(self, hasher):
            yield from hasher.yield_hashables(self.car)

        def save_instance_state(self, _depositor):
            return {'car': self.car}

        def load_instance_state(self, saved_state, _depositor):
            self.car = saved_state['car']

    ferrari = Car('ferrari')
    mike = Person(ferrari)

    mike_id = historian.save(mike)
    del mike

    loaded_mike = historian.load(mike_id)
    assert loaded_mike.car.make == 'ferrari'
    # Default is to save by reference so two cars should be the same
    assert loaded_mike.car is ferrari


def test_copy(historian: mincepy.Historian):
    car = Car('zonda')

    historian.save(car)
    car_copy = historian.copy(car)
    assert car == car_copy
    assert car is not car_copy

    record = historian.get_current_record(car)
    copy_record = historian.get_current_record(car_copy)

    assert record is not copy_record
    assert copy_record.get_copied_from() == record.get_reference()


def test_delete(historian: mincepy.Historian):
    """Test deleting and then attempting to load an object"""
    car = Car('lada')
    car_id = historian.save(car)
    historian.delete(car)
    with pytest.raises(mincepy.ObjectDeleted):
        historian.load(car_id)

    records = historian.history(car_id, as_objects=False)
    assert len(records) == 2, "There should be two record, the initial and the delete"
    assert records[-1].is_deleted_record()


def test_load_unknown_object(mongodb_archive, historian: mincepy.Historian):
    """Make up an ID an try to load it"""
    obj_id = mongodb_archive.create_archive_id()
    with pytest.raises(mincepy.NotFound):
        historian.load(obj_id)


class Cycle(mincepy.Archivable):
    TYPE_ID = uuid.UUID('600fb6ae-684c-4f8e-bed3-47ae06739d29')
    ATTRS = ('ref',)

    def __init__(self, ref=None):
        super(Cycle, self).__init__()
        self.ref = ref

    def __eq__(self, other):
        return self.ref is other.ref

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables(id(self.ref))


def test_cyclic_ref_simple(historian: mincepy.Historian):
    a = Cycle()
    a.ref = a  # Cycle complete

    a_id = historian.save(a)
    del a
    loaded_a = historian.load(a_id)
    assert loaded_a.ref is loaded_a


def test_cyclic_ref_complex(historian: mincepy.Historian):
    a = Cycle()
    b = Cycle(a)
    a.ref = b  # Cycle complete

    a_id = historian.save(a)
    del a
    loaded_a = historian.load(a_id)
    assert loaded_a.ref is b
    assert b.ref is loaded_a


def test_transaction_rollback(historian: mincepy.Historian):
    ferrari = Car('ferrari')
    with historian.transaction() as trans:
        # Within the transaction should be able to save and load
        ferrari_id = historian.save(ferrari)
        loaded = historian.load(ferrari_id)
        assert loaded is ferrari
        trans.rollback()

    # But now that I rolled back the object should not be available
    with pytest.raises(mincepy.NotFound):
        historian.load(ferrari_id)

    ferrari_id = historian.save(ferrari)
    assert historian.load(ferrari_id) is ferrari


def test_transaction_snapshots(historian: mincepy.Historian):
    ferrari = Car('ferrari')
    ferrari_id = historian.save_snapshot(ferrari)

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


def test_replace_simple(historian: mincepy.Historian):

    def paint_shop(car, colour):
        """An imaginary function that modifies an object but returns a copy rather than an in
        place modification"""
        return Car(car.make, colour)

    honda = Car('honda', 'yellow')
    honda_id = historian.save(honda)

    # Now paint the honda
    new_honda = paint_shop(honda, 'green')
    assert historian.get_obj_id(honda) == honda_id

    # Now we know that this is a 'continuation' of the history of the original honda, so replace
    historian.replace(honda, new_honda)
    with pytest.raises(mincepy.NotFound):
        historian.get_obj_id(honda)

    assert historian.get_obj_id(new_honda) == honda_id
    historian.save(new_honda)
    del honda, new_honda

    loaded = historian.load(honda_id)
    assert loaded.make == 'honda'
    assert loaded.colour == 'green'


def test_replace_invalid(historian: mincepy.Historian):
    honda = Car('honda', 'yellow')
    historian.save(honda)
    with pytest.raises(AssertionError):
        historian.replace(honda, Garage())


def test_find(historian: mincepy.Historian):
    honda = Car('honda', 'green')
    porsche = Car('porsche', 'black')
    red_honda = Car('honda', 'red')
    fiat = Car('fiat', 'green')

    historian.save(honda, porsche, red_honda, fiat)
    hondas = list(historian.find(Car, criteria={'make': 'honda'}))
    assert len(hondas) == 2
    assert honda in hondas
    assert red_honda in hondas

    # Try without type
    greens = list(historian.find(criteria={'colour': 'green'}))
    assert len(greens) == 2
    assert honda in greens
    assert fiat in greens
