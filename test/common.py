import bson

import mincepy


class Car(mincepy.Archivable):
    TYPE_ID = bson.ObjectId('5e075d6244572f823ed93274')
    TEST = 'In Car'
    ATTRS = ('colour', 'make')

    #
    # @classmethod
    # def define(cls, spec):
    #     super().define(spec)
    #     spec.attrs('colour', 'make')

    def __init__(self, make='ferrari', colour='red'):
        super(Car, self).__init__()
        self.make = make
        self.colour = colour


class Garage(mincepy.Archivable):
    TYPE_ID = bson.ObjectId('5e07b40a44572f823ed9327b')
    ATTRS = ('car',)

    #
    # @classmethod
    # def define(cls, spec):
    #     super().define(spec)
    #     spec.attrs('car')

    def __init__(self, car=None):
        super(Garage, self).__init__()
        self.car = car
