import gc
import uuid

import pytest

import mincepy
import mincepy.builtins
from mincepy.testing import Car, Garage, Cycle


def test_basic_save_load(historian: mincepy.Historian):
    car = Car('nissan', 'white')

    car_id = historian.save(car)
    del car
    loaded_car = historian.load(car_id)

    assert loaded_car.make == 'nissan'
    assert loaded_car.colour == 'white'


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

    # Default is to save by value so two cars should not be the same
    assert loaded_mike.car is not ferrari
    assert loaded_mike.car == ferrari  # But values should match


def test_copy(historian: mincepy.Historian):
    car = Car('zonda')

    historian.save(car)
    car_copy = historian.copy(car)
    assert car == car_copy
    assert car is not car_copy

    record = historian.get_current_record(car)
    copy_record = historian.get_current_record(car_copy)

    assert record is not copy_record
    assert copy_record.get_copied_from() == record.snapshot_id


def test_load_unknown_object(mongodb_archive, historian: mincepy.Historian):
    """Make up an ID an try to load it"""
    obj_id = mongodb_archive.create_archive_id()
    with pytest.raises(mincepy.NotFound):
        historian.load(obj_id)


def test_cyclic_ref_simple(historian: mincepy.Historian):
    a = Cycle()
    a.ref = a  # Cycle complete
    a_id = historian.save(a)
    del a
    gc.collect()  # Force a garbage collection

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

    del loaded_a, b
    gc.collect()  # Force a garbage collection
    reloaded_a = historian.load(a_id)
    assert isinstance(reloaded_a.ref, Cycle)


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
    assert historian.get_obj_id(honda) is None

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


def test_user_info(historian: mincepy.Historian):
    user_info = historian.get_user_info()
    assert user_info[mincepy.ExtraKeys.USER]
    assert user_info[mincepy.ExtraKeys.HOSTNAME]

    car = Car()
    historian.save(car)
    record = historian.get_current_record(car)

    assert record.extras[mincepy.ExtraKeys.USER] == user_info[mincepy.ExtraKeys.USER]
    assert record.extras[mincepy.ExtraKeys.HOSTNAME] == user_info[mincepy.ExtraKeys.HOSTNAME]


def test_save_as_ref(historian: mincepy.Historian):

    class Person(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('692429b6-a08b-489a-aa09-6eb3174b6405')
        ATTRS = (mincepy.AsRef('car'),)  # Save the car by reference

        def __init__(self, car):
            super().__init__()
            self.car = car

    car = Car()
    # Both martin and sonia have the same car
    martin = Person(car)
    sonia = Person(car)
    martin_id, sonia_id = historian.save(martin, sonia)
    del martin, sonia, car

    # No reload and check they still have the same car
    martin, sonia = historian.load(martin_id, sonia_id)
    assert martin.car is sonia.car


def test_encode_nested(historian: mincepy.Historian):

    class CarDelegate(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('c0148a43-c0c0-4d2b-9262-ed1c8c6ab2fc')
        ATTRS = ('car',)

        def __init__(self, car):
            super(CarDelegate, self).__init__()
            self.car = car

        def save_instance_state(self, saver):
            return {'car': self.car}

        def load_instance_state(self, saved_state, _loader):
            self.car = saved_state['car']

    historian.register_type(CarDelegate)

    car = Car()
    delegate = CarDelegate(car)
    garage = Garage(delegate)

    garage_id = historian.save(garage)
    del garage

    historian.load(garage_id)
