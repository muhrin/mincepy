from abc import ABCMeta, abstractmethod

import pyhash

__all__ = ('TypeEquator', 'Equator')


def eq_attributes(one, other, attributes) -> bool:
    return all(one.__getattribute__(attr) == other.__getattribute__(attr) for attr in attributes)


def yield_hashable_attributes(obj, attributes, hasher):
    for attr in attributes:
        yield from hasher.yield_hashables(obj.__getattribute__(attr))


class TypeEquator(metaclass=ABCMeta):
    """Responsible for generating a hash and checking equality of objects"""
    TYPE = None  # The type that this equator can compare

    def __init__(self):
        assert self.TYPE is not None, "Must set the equator TYPE to a type of or a tuple of types"

    @abstractmethod
    def yield_hashables(self, value, hasher):
        """Produce a hash representing the value"""

    @abstractmethod
    def eq(self, one, other) -> bool:
        """Return True of the two objects are equal, False otherwise"""


class Equator:
    def __init__(self, equators=tuple()):
        self._equators = list(equators)

        def do_hash(*args):
            hasher = pyhash.xx_64()
            return hex(hasher(*args))[2:]

        self._hasher = do_hash

    def add_equator(self, equator: TypeEquator):
        self._equators.append(equator)

    def remove_equator(self, equator):
        self._equators.reverse()
        try:
            self._equators.remove(equator)
        except ValueError:
            raise ValueError("Unknown equator '{}'".format(equator))
        finally:
            self._equators.reverse()

    def get_equator(self, obj):
        for equator in self._equators:
            if isinstance(obj, equator.TYPE):
                return equator
        raise TypeError("Don't know how to compare '{}' types, no type equator set".format(type(obj)))

    def yield_hashables(self, obj):
        try:
            equator = self.get_equator(obj)
        except TypeError:
            # Try the objects's method
            yield from obj.yield_hashables(self)
        else:
            yield from equator.yield_hashables(obj, self)

    def hash(self, obj):
        return self._hasher(*self.yield_hashables(obj))

    def eq(self, obj1, obj2) -> bool:
        if not type(obj1) == type(obj2):
            return False

        try:
            equator = self.get_equator(obj1)
        except TypeError:
            # Fallback to python eq
            return obj1 == obj2
        else:
            return equator.eq(obj1, obj2)

    def float_to_str(self, value, sig=14):
        """
        Convert float to text string for computing hash.
        Preserve up to N significant number given by sig.

        :param value: the float value to convert
        :param sig: choose how many digits after the comma should be output
        """
        fmt = u'{{:.{}g}}'.format(sig)
        return fmt.format(value)
