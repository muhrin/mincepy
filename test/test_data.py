import uuid

import pytest

import mincepy
from .common import Car, Garage


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

    build_call = historian.find(mincepy.FunctionCall, limit=1)[0]
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


def test_metadata(historian: mincepy.Historian):
    car = Car()
    ferrari_id = historian.save_snapshot(car, with_meta={'reg': 'VD395'})
    # Check that we get back what we just set
    assert historian.get_meta(ferrari_id) == {'reg': 'VD395'}

    car.make = 'fiat'
    red_fiat_id = historian.save_snapshot(car)
    # Check that the metadata was inherited
    assert historian.get_meta(red_fiat_id) == {'reg': 'VD395'}

    historian.set_meta(ferrari_id, {'reg': 'N317'})
    # Check that this saves the metadata on the object level i.e. both are changed
    assert historian.get_meta(ferrari_id) == {'reg': 'N317'}
    assert historian.get_meta(red_fiat_id) == {'reg': 'N317'}


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

    class Person(mincepy.SavableComparable):
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
