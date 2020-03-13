from abc import ABCMeta, abstractmethod
import datetime
import typing
import uuid

try:  # Python3
    from hashlib import blake2b
except ImportError:  # Python < 3.6
    from pyblake2 import blake2b

from . import exceptions

__all__ = 'Savable', 'Comparable', 'Object', 'SavableObject', 'PRIMITIVE_TYPES'

# The primitives that all archive types must support
PRIMITIVE_TYPES = (bool, int, float, str, dict, list, type(None), bytes, uuid.UUID,
                   datetime.datetime)


class Savable(metaclass=ABCMeta):
    """Interface for an object that can save an load its instance state"""
    TYPE_ID = None

    def __init__(self):
        assert self.TYPE_ID is not None, "Must set the TYPE_ID for an object to be savable"

    @abstractmethod
    def save_instance_state(self, saver):
        """Save the instance state of an object, should return a saved instance"""

    @abstractmethod
    def load_instance_state(self, saved_state, loader):
        """Take the given object and load the instance state into it"""


class Comparable(metaclass=ABCMeta):
    """Interface for an object that can be compared and hashed"""

    @abstractmethod
    def __eq__(self, other) -> bool:
        """Determine if two objects are equal"""

    @abstractmethod
    def yield_hashables(self, hasher):
        """Produce a hash representing the value"""


class Object(Comparable, metaclass=ABCMeta):
    """A simple object that is comparable"""


class SavableObject(Object, Savable, metaclass=ABCMeta):
    """A class that is both savable and comparable"""

    def __init__(self, historian=None):
        super().__init__()
        from . import history
        # Tell the historian that we've been created
        self._historian = historian or history.get_historian()
        assert self._historian is not None, \
            "Must provide a valid historian or set one globally using mincepy.set_historian()"

        if self._historian is not None:
            self._historian.created(self)

    @property
    def obj_id(self):
        return self._historian.get_obj_id(self)

    def get_meta(self) -> dict:
        """Get the metadata dictionary for this object"""
        return self._historian.meta.get(self)

    def set_meta(self, meta: typing.Optional[dict]):
        """Set the metadata dictionary for this object"""
        self._historian.meta.set(self, meta)

    def update_meta(self, meta: dict):
        """Update the metadata dictionary for this object"""
        self._historian.meta.update(self, meta)

    def is_saved(self) -> bool:
        return self.obj_id is not None

    def save(self, with_meta=None, return_sref=False):
        """Save the object"""
        return self._historian.save(self, with_meta=with_meta, return_sref=return_sref)

    def sync(self):
        """Update the state of this object by loading the latest version from the historian"""
        return self._historian.sync(self)

    def load_instance_state(self, saved_state, loader):
        """Take the given object and load the instance state into it"""
        super().load_instance_state(saved_state, loader)
        self._historian = loader.get_historian()


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
        # Iterate in reversed order i.e. the latest added should be used preferentially
        for equator in reversed(self._equators):
            if isinstance(obj, equator.TYPE):
                return equator
        raise TypeError("Don't know how to compare '{}' types, no type equator set".format(
            type(obj)))

    def yield_hashables(self, obj):
        try:
            equator = self.get_equator(obj)
        except TypeError:
            # Try the objects's method
            try:
                yield from obj.yield_hashables(self)
            except AttributeError:
                raise TypeError("No helper registered and no yield_hashabled method on '{}'".format(
                    type(obj)))
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
