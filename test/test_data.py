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
    ferrari_id = historian.save_get_snapshot_id(car)
    assert ferrari_id == historian.save_get_snapshot_id(car)

    car.make = 'fiat'
    car.color = 'white'

    fiat_id = historian.save_get_snapshot_id(car)

    assert fiat_id != ferrari_id

    ferrari = historian.load(ferrari_id)

    assert ferrari.make == 'ferrari'
    assert ferrari.colour == 'red'


def test_nested_references(historian: mincepy.Historian):
    car = Car()
    garage = Garage(car)

    ferrari_id = historian.save_get_snapshot_id(car)
    ferrari_garage_id = historian.save_get_snapshot_id(garage)

    # Now change the car
    car.make = 'fiat'
    car.colour = 'white'

    fiat_garage_id = historian.save_get_snapshot_id(garage)
    ferrari = historian.load(ferrari_id)

    assert ferrari.make == 'ferrari'
    assert ferrari.colour == 'red'

    ferrari_garage = historian.load(ferrari_garage_id)

    assert ferrari_garage.car is ferrari

    fiat_garage = historian.load(fiat_garage_id)
    assert fiat_garage is garage


def test_create_delete_load(historian: mincepy.Historian):
    car = Car('honda', 'red')
    car_id = historian.save_get_snapshot_id(car)
    del car

    loaded_car = historian.load(car_id)
    assert loaded_car.make == 'honda'
    assert loaded_car.colour == 'red'


def test_list_basics(historian: mincepy.Historian):
    parking_lot = mincepy.builtins.List()
    for i in range(100):
        parking_lot.append(Car(str(i)))

    list_id = historian.save_get_snapshot_id(parking_lot)

    # Change one element
    parking_lot[0].make = 'ferrari'
    new_list_id = historian.save_get_snapshot_id(parking_lot)

    assert list_id != new_list_id

    old_list = historian.load(list_id)
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

    class CarFactory:
        TYPE_ID = uuid.UUID('166a9446-c04e-4fbe-a3da-6f36c2f8292d')

        def __init__(self, make):
            self._make = make

        def __eq__(self, other):
            if type(other) != CarFactory:
                return False

            return self._make == other._make

        def yield_hashables(self, hasher):
            yield from hasher.yield_hashables(self._make)

        def save_instance_state(self, _: mincepy.Referencer):
            return {'make': self._make}

        def load_instance_state(self, encoded_value, _: mincepy.Referencer):
            self.__init__(encoded_value['make'])

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
    fiat_id = historian.save_get_snapshot_id(car)
    assert car_id != fiat_id

    # Change it again...
    car.make = 'honda'
    car.colour = 'wine red'
    honda_id = historian.save_get_snapshot_id(car)
    assert honda_id != fiat_id
    assert honda_id != car_id

    # Now delete and reload
    del car
    latest = historian.load(car_id)
    assert latest == historian.load(honda_id)


def test_metadata(historian: mincepy.Historian):
    car = Car()
    ferrari_id = historian.save_get_snapshot_id(car, with_meta={'reg': 'VD395'})
    # Check that we get back what we just set
    assert historian.get_meta(ferrari_id) == {'reg': 'VD395'}

    car.make = 'fiat'
    red_fiat_id = historian.save_get_snapshot_id(car)
    # Check that the metadata was inherited
    assert historian.get_meta(red_fiat_id) == {'reg': 'VD395'}

    historian.set_meta(ferrari_id, {'reg': 'N317'})
    # Check the ferrari is now changed but the fiat retains the original value
    assert historian.get_meta(ferrari_id) == {'reg': 'N317'}
    assert historian.get_meta(red_fiat_id) == {'reg': 'VD395'}


def test_save_as(historian: mincepy.Historian):
    """Check the save_as functionality in historian"""
    car = Car('ferrari')

    car_id = historian.save(car)
    assert historian.get_current_record(car) is not None

    # Now create a new car and save that over the old one
    new_car = Car('honda')

    new_car_id = historian.save_as(new_car, car_id)
    assert car_id == new_car_id

    with pytest.raises(mincepy.NotFound):
        # Now that 'old' car should not be known to the historian
        historian.get_current_record(car)

    assert historian.get_current_record(new_car) is not None
