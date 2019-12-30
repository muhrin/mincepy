import bson
import pytest
import pymongo

import mincepy


class Car:
    def __init__(self, make='ferrari', colour='red'):
        self.make = make
        self.colour = colour

    def __eq__(self, other):
        if type(other) != Car:
            return False

        return self.make == other.make and self.colour == other.colour

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables(self.make)
        yield from hasher.yield_hashables(self.colour)


class Garage:
    def __init__(self, car=None):
        self.car = car

    def __eq__(self, other):
        if type(other) != Garage:
            return False

        return self.car == other.car

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables(self.car)


class CarCodec(mincepy.TypeCodec):
    TYPE = Car
    TYPE_ID = bson.ObjectId('5e075d6244572f823ed93274')

    def encode(self, value: Car, lookup: mincepy.Referencer):
        return {'make': value.make, 'colour': value.colour}

    def decode(self, encoded_value, obj: Car, lookup: mincepy.Referencer):
        obj.__init__()
        obj.make = encoded_value['make']
        obj.colour = encoded_value['colour']


class GarageCodec(mincepy.mongo.MongoTypeCodec):
    TYPE = Garage
    TYPE_ID = bson.ObjectId('5e07b40a44572f823ed9327b')

    def encode(self, value, lookup: mincepy.Referencer):
        return {'car': self.enc(value.car, lookup)}

    def decode(self, encoded_value, obj, lookup: mincepy.Referencer):
        obj.__init__()
        obj.car = self.dec(encoded_value['car'], lookup)


class ListCodec(mincepy.TypeCodec):
    TYPE = mincepy.builtins.List
    TYPE_ID = bson.ObjectId('5e07d7de44572f823ed9327c')

    def encode(self, value, lookup: mincepy.Referencer):
        return [lookup.ref(e) for e in value]

    def decode(self, encoded_value, obj, lookup: mincepy.Referencer):
        obj.__init__([lookup.deref(entry) for entry in encoded_value])


class FunctionCallCodec(mincepy.mongo.MongoTypeCodec):
    TYPE = mincepy.FunctionCall
    TYPE_ID = bson.ObjectId('5e08974b44572f823ed9327d')

    def encode(self, value, lookup: mincepy.Referencer):
        return {
            'function': self.enc(value.function, lookup),
            'args': self.enc(list(value.args), lookup),
            'kwargs': self.enc(value.kwargs, lookup),
            'result': self.enc(value._result, lookup),
            'exception': self.enc(value._exception, lookup),
            'done': self.enc(value._done, lookup)
        }

    def decode(self, encoded_value, obj, lookup: mincepy.Referencer):
        obj._function = self.dec(encoded_value['function'], lookup)
        obj._args = tuple(self.dec(encoded_value['args'], lookup)),
        obj._kwargs = self.dec(encoded_value['kwargs'], lookup),
        obj._result = self.dec(encoded_value['result'], lookup),
        obj._exception = self.dec(encoded_value['exception'], lookup),
        obj._done = self.dec(encoded_value['done'], lookup)


@pytest.fixture
def mongodb_archive():
    client = pymongo.MongoClient()
    db = client.test_database
    codecs = (CarCodec(), GarageCodec(), ListCodec(), FunctionCallCodec())
    mongo_archive = mincepy.mongo.MongoArchive(db, codecs)
    yield mongo_archive
    client.drop_database(db)


@pytest.fixture
def historian(mongodb_archive):
    hist = mincepy.Historian(mongodb_archive)
    return hist


def test_save_change_load(historian: mincepy.Historian):
    car = Car()

    ferrari_id = historian.save(car)
    assert ferrari_id == historian.save(car)

    car.make = 'fiat'
    car.color = 'white'

    fiat_id = historian.save(car)

    assert fiat_id != ferrari_id

    ferrari = historian.load(ferrari_id)

    assert ferrari.make == 'ferrari'
    assert ferrari.colour == 'red'


def test_nested_references(historian: mincepy.Historian):
    car = Car()
    garage = Garage(car)

    ferrari_id = historian.save(car)
    ferrari_garage_id = historian.save(garage)

    # Now change the car
    car.make = 'fiat'
    car.colour = 'white'

    fiat_garage_id = historian.save(garage)
    ferrari = historian.load(ferrari_id)

    assert ferrari.make == 'ferrari'
    assert ferrari.colour == 'red'

    ferrari_garage = historian.load(ferrari_garage_id)

    assert ferrari_garage.car is ferrari

    fiat_garage = historian.load(fiat_garage_id)
    assert fiat_garage is garage


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

    list_id = historian.save(parking_lot)

    # Change one element
    parking_lot[0].make = 'ferrari'
    new_list_id = historian.save(parking_lot)

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
