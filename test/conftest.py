# -*- coding: utf-8 -*-
# pylint: disable=unused-import, redefined-outer-name
import random

import pytest

import mincepy
from mincepy import testing
from mincepy.testing import archive_uri, mongodb_archive, historian, archive_base_uri
from . import utils


@pytest.fixture
def standard_dataset(historian: mincepy.Historian):
    with historian.transaction():
        # Put in some cars
        ferrari = testing.Car('ferrari', 'red')
        ferrari.save()
        honda = testing.Car('honda', 'white')
        honda.save()
        fiat = testing.Car('fiat', 'green')
        fiat.save()

        # Put in some identical yellow cars
        for _ in range(4):
            testing.Car(make='renault', colour='yellow').save()

        # Put in some people
        testing.Person('sonia', 30, ferrari).save()
        testing.Person('martin', 35, honda).save()
        testing.Person('gavin', 34).save()
        testing.Person('upul', 35, fiat).save()

        # Put in some people with the same name and different age
        for _ in range(100):
            testing.Person(name='mike', age=random.randint(0, 4)).save()


@pytest.fixture
def large_dataset(historian: mincepy.Historian):
    with historian.transaction():
        # Put in some cars
        ferrari = testing.Car('ferrari', 'red')
        ferrari.save()
        honda = testing.Car('honda', 'white')
        honda.save()
        fiat = testing.Car('fiat', 'green')
        fiat.save()

        # Put in some yellow cars
        for _ in range(100):
            testing.Car(make='renault', colour='yellow').save()

        # Put in some purely random
        for _ in range(100):
            testing.Car(make=utils.random_str(5), colour=utils.random_str(3)).save()

        # Put in some people
        testing.Person('sonia', 30, ferrari).save()
        testing.Person('martin', 35, honda).save()
        testing.Person('gavin', 34).save()
        testing.Person('upul', 35, fiat).save()

        # Put in some with the same age
        for _ in range(100):
            testing.Person(name=utils.random_str(5), age=54).save()

        # Put in some with the same name
        for _ in range(100):
            testing.Person(name='mike', age=random.randint(0, 100)).save()

        # Put in some purely random
        for _ in range(100):
            testing.Person(name=utils.random_str(5), age=random.randint(0, 100)).save()
