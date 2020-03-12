"""Classes and function useful for trying out mincepy functionality"""
import random
import uuid

import bson
import pymongo
import pytest

import mincepy


@pytest.fixture
def mongodb_archive():
    client = pymongo.MongoClient()
    db = client.test_database
    mongo_archive = mincepy.mongo.MongoArchive(db)
    yield mongo_archive
    client.drop_database(db)


@pytest.fixture(autouse=True)
def historian(mongodb_archive):
    hist = mincepy.Historian(mongodb_archive)
    hist.register_types(mincepy.testing.HISTORIAN_TYPES)
    mincepy.set_historian(hist)
    yield hist
    mincepy.set_historian(None)


class Car(mincepy.BaseSavableObject):
    TYPE_ID = bson.ObjectId('5e075d6244572f823ed93274')
    ATTRS = 'colour', 'make'

    def __init__(self, make='ferrari', colour='red'):
        super(Car, self).__init__()
        self.make = make
        self.colour = colour


class Garage(mincepy.BaseSavableObject):
    TYPE_ID = bson.ObjectId('5e07b40a44572f823ed9327b')
    ATTRS = ('car',)

    def __init__(self, car=None):
        super(Garage, self).__init__()
        self.car = car


class Person(mincepy.BaseSavableObject):
    TYPE_ID = uuid.UUID('d60ca740-9fa6-4002-83f6-e4c91403e41b')
    ATTRS = 'name', 'age'

    def __init__(self, name, age):
        super(Person, self).__init__()
        self.name = name
        self.age = age

    def __eq__(self, other):
        return self.name == other.name and self.age == other.age


class Cycle(mincepy.BaseSavableObject):
    TYPE_ID = uuid.UUID('600fb6ae-684c-4f8e-bed3-47ae06739d29')
    ATTRS = ('_ref',)

    def __init__(self, ref=None):
        super(Cycle, self).__init__()
        self._ref = mincepy.ObjRef(ref)

    @property
    def ref(self):
        return self._ref()

    @ref.setter
    def ref(self, value):
        self._ref = mincepy.ObjRef(value)

    def __eq__(self, other):
        return self.ref is other.ref

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables(id(self.ref))


def populate(historian=None):
    historian = historian or mincepy.get_historian()

    colours = ('red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet')
    makes = ('honda', 'ferrari', 'zonda', 'fiat')

    cars = []

    for make in makes:
        for colour in colours:
            # Make some cars
            car = Car(make, colour)
            historian.save(car)
            cars.append(car)

    # Now randomly change some of them
    for _ in range(int(len(cars) / 4)):
        car = random.choice(cars)
        car.colour = random.choice(colours)
        car.save()

    # Now change one a number of times
    car = random.choice(cars)
    for colour in colours:
        car.colour = colour
        car.save()

    people = mincepy.RefList()
    for name in ('martin', 'sonia', 'gavin', 'upul', 'sebastiaan', 'irene'):
        person = Person(name, random.randint(20, 40))
        historian.save(person)
        people.append(person)
    historian.save(people)


HISTORIAN_TYPES = Car, Garage, Person, Cycle
