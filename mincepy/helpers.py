from abc import ABCMeta, abstractmethod
from typing import Type

from . import types

__all__ = 'TypeHelper', 'WrapperHelper', 'BaseHelper'


class TypeHelper(metaclass=ABCMeta):
    """Responsible for generating a hash and checking equality of objects"""
    TYPE = None  # The type this helper corresponds to
    TYPE_ID = None  # The unique id for this type of objects
    IMMUTABLE = False

    def __init__(self):
        assert self.TYPE is not None, "Must set the TYPE to a type of or a tuple of types"

    def new(self, encoded_saved_state):  # pylint: disable=unused-argument
        """Create a new blank object of this type"""
        cls = self.TYPE
        return cls.__new__(cls)

    @abstractmethod
    def yield_hashables(self, obj, hasher):
        """Produce a hash representing the value"""

    @abstractmethod
    def eq(self, one, other) -> bool:  # pylint: disable=invalid-name
        """Determine if two objects are equal"""

    @abstractmethod
    def save_instance_state(self, obj, depositor):
        """Save the instance state of an object, should return a saved instance"""

    @abstractmethod
    def load_instance_state(self, obj, saved_state, depositor):
        """Take the given blank object and load the instance state into it"""


class BaseHelper(TypeHelper, metaclass=ABCMeta):
    """A base helper that defaults to yielding hashables directly on the object
    and testing for equality using == given two objects.  This behaviour is fairly
    standard and therefor more type helpers will want to subclass from this class."""

    def yield_hashables(self, obj, hasher):
        yield from hasher.yield_hashables(obj)

    def eq(self, one, other) -> bool:
        return one == other


class WrapperHelper(TypeHelper):
    """Wraps up an object type to perform the necessary Historian actions"""

    # pylint: disable=invalid-name

    def __init__(self, obj_type: Type[types.SavableObject]):
        self.TYPE = obj_type
        self.TYPE_ID = obj_type.TYPE_ID
        super(WrapperHelper, self).__init__()

    def yield_hashables(self, obj, hasher):
        yield from self.TYPE.yield_hashables(obj, hasher)

    def eq(self, one, other) -> bool:
        return self.TYPE.__eq__(one, other)

    def save_instance_state(self, obj: types.Savable, depositor):
        return self.TYPE.save_instance_state(obj, depositor)

    def load_instance_state(self, obj, saved_state: types.Savable, depositor):
        return self.TYPE.load_instance_state(obj, saved_state, depositor)
