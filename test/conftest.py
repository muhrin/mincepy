# pylint: disable=unused-import
import random

import pytest

import mincepy
from mincepy.testing import archive_uri, mongodb_archive, historian, Car, Person, Garage
from . import utils


@pytest.fixture
def large_dataset(historian: mincepy.Historian):
    with historian.transaction():
        # Put in some cars
        Car('ferrari', 'red').save()
        Car('honda', 'white').save()
        Car('fiat', 'green').save()

        # Put in some yellow cars
        for _ in range(100):
            Car(make='renault', colour='yellow').save()

        # Put in some purely random
        for _ in range(100):
            Car(make=utils.random_str(5), colour=utils.random_str(3)).save()

        # Put in some people
        Person('sonia', 30).save()
        Person('martin', 35).save()
        Person('gavin', 34).save()
        Person('upul', 35).save()

        # Put in some with the same age
        for _ in range(100):
            Person(name=utils.random_str(5), age=54).save()

        # Put in some with the same name
        for _ in range(100):
            Person(name='mike', age=random.randint(0, 100)).save()

        # Put in some purely random
        for _ in range(100):
            Person(name=utils.random_str(5), age=random.randint(0, 100)).save()
