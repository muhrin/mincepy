"""Classes and function useful for trying out mincepy functionality"""

import uuid

import bson
import mincepy


class Car(mincepy.Archivable):
    TYPE_ID = bson.ObjectId('5e075d6244572f823ed93274')
    ATTRS = 'colour', 'make'

    def __init__(self, make='ferrari', colour='red'):
        super(Car, self).__init__()
        self.make = make
        self.colour = colour


class Garage(mincepy.Archivable):
    TYPE_ID = bson.ObjectId('5e07b40a44572f823ed9327b')
    ATTRS = ('car',)

    def __init__(self, car=None):
        super(Garage, self).__init__()
        self.car = car


class Person(mincepy.Archivable):
    TYPE_ID = uuid.UUID('d60ca740-9fa6-4002-83f6-e4c91403e41b')
    ATTRS = 'name', 'age'

    def __init__(self, name, age):
        super(Person, self).__init__()
        self.name = name
        self.age = age

    def __eq__(self, other):
        return self.name == other.name and self.age == other.age


HISTORIAN_TYPES = Car, Garage, Person
