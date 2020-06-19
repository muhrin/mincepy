"""Classes and function useful for trying out mincepy functionality"""
import os
import random
import uuid

import bson
import pymongo
import pytest

import mincepy

ENV_ARCHIVE_URI = 'MINCEPY_TEST_URI'
DEFAULT_ARCHIVE_URI = 'mongodb://localhost/mincepy-tests'

# pylint: disable=redefined-outer-name


@pytest.fixture
def archive_uri():
    return os.environ.get(ENV_ARCHIVE_URI, DEFAULT_ARCHIVE_URI)


@pytest.fixture
def mongodb_archive(archive_uri):
    client = pymongo.MongoClient(archive_uri)
    db = client.get_default_database()  # pylint: disable=invalid-name
    client.drop_database(db)
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


class Car(mincepy.SimpleSavable):
    TYPE_ID = bson.ObjectId('5e075d6244572f823ed93274')
    ATTRS = 'colour', 'make'

    def __init__(self, make='ferrari', colour='red'):
        super(Car, self).__init__()
        self.make = make
        self.colour = colour


class Garage(mincepy.SimpleSavable):
    TYPE_ID = bson.ObjectId('5e07b40a44572f823ed9327b')
    ATTRS = ('car',)

    def __init__(self, car=None):
        super(Garage, self).__init__()
        self.car = car

    def save_instance_state(self, saver: mincepy.Saver):
        state = super().save_instance_state(saver)
        return state


class Person(mincepy.SimpleSavable):
    TYPE_ID = uuid.UUID('d60ca740-9fa6-4002-83f6-e4c91403e41b')
    ATTRS = 'name', 'age', mincepy.AsRef('car')

    def __init__(self, name, age, car=None):
        super(Person, self).__init__()
        self.name = name
        self.age = age
        self.car = car


class Cycle(mincepy.SimpleSavable):
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
