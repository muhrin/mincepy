import bson

import mincepy


class Car:
    TYPE_ID = bson.ObjectId('5e075d6244572f823ed93274')

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

    def save_instance_state(self, _: mincepy.Referencer):
        return {'make': self.make, 'colour': self.colour}

    def load_instance_state(self, encoded_value, _: mincepy.Referencer):
        self.__init__(encoded_value['make'], encoded_value['colour'])


class Garage:
    TYPE_ID = bson.ObjectId('5e07b40a44572f823ed9327b')

    def __init__(self, car=None):
        self.car = car

    def __eq__(self, other):
        if type(other) != Garage:
            return False

        return self.car == other.car

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables(self.car)

    def save_instance_state(self, referencer: mincepy.Referencer):
        return {'car': referencer.ref(self.car)}

    def load_instance_state(self, encoded_value, referencer: mincepy.Referencer):
        self.__init__(referencer.deref(encoded_value['car']))