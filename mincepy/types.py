from abc import ABCMeta, abstractmethod
import datetime
import uuid

try:  # Python3
    from hashlib import blake2b
except ImportError:  # Python < 3.6
    from pyblake2 import blake2b

__all__ = ('Equator', 'SavableObject', 'Archivable')

# The primitives that all archive types must support
PRIMITIVE_TYPES = (bool, int, float, str, dict, list, type(None), bytes, uuid.UUID, datetime.datetime)


class Savable(metaclass=ABCMeta):
    """An object that can save an load its instance state"""
    TYPE_ID = None

    def __init__(self):
        assert self.TYPE_ID is not None, "Must set the TYPE_ID for an object to be savable"

    @abstractmethod
    def save_instance_state(self, depositor):
        """Save the instance state of an object, should return a saved instance"""

    @abstractmethod
    def load_instance_state(self, saved_state, depositor):
        """Take the given object and load the instance state into it"""


class Comparable(metaclass=ABCMeta):
    """An object that can be compared and hashed"""

    @abstractmethod
    def __eq__(self, other) -> bool:
        """Determine if two objects are equal"""

    @abstractmethod
    def yield_hashables(self, hasher):
        """Produce a hash representing the value"""


class Object(Comparable, metaclass=ABCMeta):

    def __init__(self):
        from . import history
        # Tell the historian that we've been created
        historian = history.get_historian()
        if historian is not None:
            historian.created(self)


class Primitive(Object, metaclass=ABCMeta):
    """Primitives are types that are comparable but not encodable through save_instance_state.
    They must be accepted directly by the archive"""


class SavableObject(Object, Savable, metaclass=ABCMeta):
    """A class that is both savable and comparable"""


class Archivable(SavableObject):
    """A helper class that makes a class compatible with the historian by flagging certain
    attributes which will be saved/loaded/hashed and compared in __eq__.  This should be an
    exhaustive list of all the attributes that define this class.  If more complex functionality
    is needed then the standard SavableComparable interface methods should be overwritten."""
    ATTRS = tuple()

    def __new__(cls, *_args, **_kwargs):
        new_instance = super(Archivable, cls).__new__(cls)
        attrs = []
        for entry in cls.__mro__:
            try:
                local = getattr(entry, 'ATTRS')
                attrs.extend(local)
            except AttributeError:
                pass
        setattr(new_instance, '__attrs', attrs)
        return new_instance

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return all(getattr(self, name) == getattr(other, name) for name in self.__get_attrs())

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables([getattr(self, name) for name in self.__get_attrs()])

    def save_instance_state(self, depositor):
        return {name: getattr(self, name) for name in self.__get_attrs()}

    def load_instance_state(self, saved_state, depositor):
        for name in self.__get_attrs():
            setattr(self, name, saved_state[name])

    def __get_attrs(self):
        return getattr(self, '__attrs')


class Equator:

    def __init__(self, equators=tuple()):
        self._equators = list(equators)

        def do_hash(*args):
            hasher = blake2b(digest_size=32)
            for arg in args:
                hasher.update(arg)

            return hasher.hexdigest()

        self._hasher = do_hash

    def add_equator(self, equator):
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
            try:
                yield from obj.yield_hashables(self)
            except AttributeError:
                raise TypeError("No helper registered and no yield_hashabled method on '{}'".format(type(obj)))
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

    def float_to_str(self, value, sig=14):  # pylint: disable=no-self-use
        """
        Convert float to text string for computing hash.
        Preserve up to N significant number given by sig.

        :param value: the float value to convert
        :param sig: choose how many digits after the comma should be output
        """
        fmt = u'{{:.{}g}}'.format(sig)
        return fmt.format(value)
