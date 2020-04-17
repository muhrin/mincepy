from argparse import Namespace
import pathlib
import uuid

import mincepy
import mincepy.builtins

from mincepy.testing import Car, Person


def test_path_helper(historian: mincepy.Historian):
    historian.register_type(mincepy.common_helpers.PathHelper())

    class File(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('8d645bb8-4657-455b-8b61-8613bc8a0acf')
        ATTRS = ('path',)

        def __init__(self, path):
            super(File, self).__init__()
            self.path = path

    file = File(pathlib.Path('some_path'))
    file_id = historian.save(file)
    del file
    loaded = historian.load(file_id)
    assert loaded.path == pathlib.Path('some_path')


def test_tuple_helper(historian: mincepy.Historian):
    historian.register_type(mincepy.common_helpers.TupleHelper())

    container = mincepy.builtins.List()
    container.append((Car('ferrari'),))

    container_id = historian.save(container)
    del container
    loaded = historian.load(container_id)
    assert loaded[0][0] == Car('ferrari')


def test_namespace_helper(historian: mincepy.Historian):
    historian.register_type(mincepy.common_helpers.NamespaceHelper())

    car = Car('fiat', '500')
    dinner = Namespace(food='pizza', drink='wine', cost=10.94, car=car, host=Person('Martin', 34))
    dinner_id = historian.save(dinner)
    del dinner

    loaded = historian.load(dinner_id)
    assert loaded.car == car
    assert loaded.host == Person('Martin', 34)
    assert loaded.food == 'pizza'
    assert loaded.cost == 10.94

    loaded.guest = Person('Sonia', 30)
    historian.save(loaded)
    del loaded

    reloaded = historian.load(dinner_id)
    assert reloaded.guest == Person('Sonia', 30)
