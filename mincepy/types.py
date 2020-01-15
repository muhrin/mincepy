from abc import ABCMeta, abstractmethod
import contextlib
import uuid

import pyhash

__all__ = ('TypeHelper', 'Equator', 'SavableComparable')

BASE_TYPES = (bool, int, float, str, dict, list, type(None), bytes, uuid.UUID)


def eq_attributes(one, other, attributes) -> bool:
    return all(one.__getattribute__(attr) == other.__getattribute__(attr) for attr in attributes)


def yield_hashable_attributes(obj, attributes, hasher):
    for attr in attributes:
        yield from hasher.yield_hashables(obj.__getattribute__(attr))


class TypeHelper(metaclass=ABCMeta):
    """Responsible for generating a hash and checking equality of objects"""
    TYPE = None  # The type that this equator can compare

    def __init__(self):
        assert self.TYPE is not None, "Must set the equator TYPE to a type of or a tuple of types"

    @abstractmethod
    def yield_hashables(self, obj, hasher):
        """Produce a hash representing the value"""

    @abstractmethod
    def eq(self, one, other) -> bool:  # pylint: disable=invalid-name
        """Determine if two objects are equal"""

    @abstractmethod
    def save_instance_state(self, obj, referencer):
        """Save the instance state of an object, should return a saved instance"""

    @contextlib.contextmanager
    def load(self, saved_state, referencer):
        """
        Loading of an object takes place in two steps, analogously to the way python
        creates objects.  First a 'blank' object is created and and yielded by this
        context manager.  Then loading is finished in load_instance_state.  Naturally,
        the state of the object should not be relied upon until the context exits.
        """
        new_obj = self.new(saved_state)
        try:
            yield new_obj
        finally:
            self.load_instance_state(new_obj, saved_state, referencer)

    def new(self, saved_state):  # pylint: disable=unused-argument
        """Create a new blank object of this type"""
        cls = self.TYPE
        return cls.__new__(cls)

    @abstractmethod
    def load_instance_state(self, obj, saved_state, referencer):
        """Take the given blank object and load the instance state into it"""


class Savable(metaclass=ABCMeta):
    """An object that can save an load its instance state"""
    TYPE_ID = None

    def __init__(self):
        assert self.TYPE_ID is not None, "Must set the TYPE_ID for an object to be savable"

    @abstractmethod
    def save_instance_state(self, referencer):
        """Save the instance state of an object, should return a saved instance"""

    @abstractmethod
    def load_instance_state(self, saved_state, referencer):
        """Take the given object and load the instance state into it"""


class Comparable(metaclass=ABCMeta):
    """An object that can be compared and hashed"""

    @abstractmethod
    def yield_hashables(self, hasher):
        """Produce a hash representing the value"""

    @abstractmethod
    def __eq__(self, other) -> bool:
        """Determine if two objects are equal"""


class SavableComparable(Savable, Comparable, metaclass=ABCMeta):
    """A class that is both savable and comparable"""


class Equator:

    def __init__(self, equators=tuple()):
        self._equators = list(equators)

        def do_hash(*args):
            hasher = pyhash.xx_64()
            return hex(hasher(*args))[2:]

        self._hasher = do_hash

    def add_equator(self, equator: TypeHelper):
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

    def eq(self, obj1, obj2) -> bool:  # pylint: disable=invalid-name
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
