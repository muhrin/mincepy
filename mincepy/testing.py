# -*- coding: utf-8 -*-
"""Classes and function useful for trying out mincepy functionality"""
# pylint: disable=cyclic-import
import contextlib
import logging
import os
import string
import random
from typing import Iterator
import uuid

import bson

import mincepy

logger = logging.getLogger(__name__)

ENV_ARCHIVE_URI = 'MINCEPY_TEST_URI'
ENV_ARCHIVE_BASE_URI = 'MINCEPY_TEST_BASE_URI'
DEFAULT_ARCHIVE_URI = 'mongodb://localhost/mincepy-tests'
# DEFAULT_ARCHIVE_URI = 'mongomock://localhost/mincepy-tests'
DEFAULT_ARCHIVE_BASE_URI = 'mongodb://127.0.0.1'

# pylint: disable=redefined-outer-name, invalid-name


def get_base_uri() -> str:
    """Get a base URI for an archive that can be used for testing.  This will not contain the database name as multiple
    databases can be used during a test session."""
    return os.environ.get(ENV_ARCHIVE_BASE_URI, DEFAULT_ARCHIVE_BASE_URI)


def create_archive_uri(base_uri='', db_name=''):
    """Get an archive URI based on the current archive base URI plus the passed database name.

    If the database name is missing a random one will be used"""
    if not db_name:
        letters = string.ascii_lowercase
        db_name = 'mincepy-' + ''.join(random.choice(letters) for _ in range(5))
    base_uri = base_uri or get_base_uri()
    return base_uri + '/' + db_name


@contextlib.contextmanager
# @mongomock.patch(servers=(('localhost', 27017),))
def temporary_archive(archive_uri: str) -> Iterator[mincepy.Archive]:
    """Create a temporary archive.  The associated database will be dropped on exiting the context"""
    archive = mincepy.mongo.connect(archive_uri)
    db = archive.database
    client = db.client
    yield archive
    client.drop_database(db)


@contextlib.contextmanager
def temporary_historian(archive_uri: str = '') -> Iterator[mincepy.Archive]:
    """Create a temporary historian.  The associated database will be dropped on exiting the context."""
    with temporary_archive(archive_uri) as archive:
        yield mincepy.Historian(archive)


try:
    import pytest

    # Optional pytest fixtures

    @pytest.fixture
    def archive_uri() -> str:
        return os.environ.get(ENV_ARCHIVE_URI, DEFAULT_ARCHIVE_URI)

    @pytest.fixture
    def archive_base_uri() -> str:
        return os.environ.get(ENV_ARCHIVE_BASE_URI, DEFAULT_ARCHIVE_BASE_URI)

    @pytest.fixture
    def mongodb_archive(archive_uri):
        with temporary_archive(archive_uri) as mongo_archive:
            yield mongo_archive

    @pytest.fixture(autouse=True)
    def historian(mongodb_archive):
        hist = mincepy.Historian(mongodb_archive)
        hist.register_types(mincepy.testing.HISTORIAN_TYPES)
        mincepy.set_historian(hist)
        yield hist
        mincepy.set_historian(None)

except ImportError:
    logger.debug("pytest fixtures missing because pytest isn't installed")


class Car(mincepy.ConvenientSavable):
    TYPE_ID = bson.ObjectId('5e075d6244572f823ed93274')
    colour = mincepy.field()
    make = mincepy.field()

    def __init__(self, make='ferrari', colour='red'):
        super().__init__()
        self.make = make
        self.colour = colour

    def __str__(self) -> str:
        return f'{self.colour} {self.make}'

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self.make)}, {repr(self.colour)})'


class Garage(mincepy.ConvenientSavable):
    TYPE_ID = bson.ObjectId('5e07b40a44572f823ed9327b')
    car = mincepy.field()

    def __init__(self, car=None):
        super().__init__()
        self.car = car

    def save_instance_state(self, saver: mincepy.Saver):
        state = super().save_instance_state(saver)
        return state

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self.car)})'


class Person(mincepy.ConvenientSavable):
    TYPE_ID = uuid.UUID('d60ca740-9fa6-4002-83f6-e4c91403e41b')
    name = mincepy.field()
    age = mincepy.field()
    car = mincepy.field(ref=True)

    def __init__(self, name, age, car=None):
        super().__init__()
        self.name = name
        self.age = age
        self.car = car

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self.name)}, {repr(self.age)},  {repr(self.car)})'


class Cycle(mincepy.ConvenientSavable):
    TYPE_ID = uuid.UUID('600fb6ae-684c-4f8e-bed3-47ae06739d29')
    _ref = mincepy.field()

    def __init__(self, ref=None):
        super().__init__()
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
    for name in ('martin', 'sonia', 'gavin', 'upul', 'martin', 'sebastiaan', 'irene'):
        person = Person(name, random.randint(20, 40))
        historian.save(person)
        people.append(person)
    historian.save(people)


HISTORIAN_TYPES = Car, Garage, Person, Cycle
