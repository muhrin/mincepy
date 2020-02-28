from abc import ABCMeta, abstractmethod
from typing import Type
import uuid

import mincepy
from . import records
from . import types

__all__ = 'TypeHelper', 'WrapperHelper', 'BaseHelper'


def inject_creation_tracking(cls):
    # Check to make sure we don't do this twice!
    if not hasattr(cls, '__orig_new'):
        cls.__orig_new = cls.__new__

        def new(_cls, *_args, **_kwargs):
            inst = cls.__orig_new(_cls)
            hist = mincepy.get_historian()
            if hist is not None:
                hist.created(inst)
            return inst

        cls.__new__ = new


def remove_creation_tracking(cls):
    try:
        cls.__new__ = cls.__orig_new
    except AttributeError:
        pass


class TypeHelper(metaclass=ABCMeta):
    """This interface provides the basic methods necessary to enable a type
    to be compatible with the historian."""
    TYPE = None  # The type this helper corresponds to
    TYPE_ID = None  # The unique id for this type of objects
    IMMUTABLE = False  # If set to true then the object is decoded straight away
    INJECT_CREATION_TRACKING = False

    def __init__(self):
        assert self.TYPE is not None, "Must set the TYPE to a type of or a tuple of types"
        if self.INJECT_CREATION_TRACKING:
            inject_creation_tracking(self.TYPE)

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
    def save_instance_state(self, obj, saver):
        """Save the instance state of an object, should return a saved instance"""

    @abstractmethod
    def load_instance_state(self, obj, saved_state, loader):
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

    def save_instance_state(self, obj: types.Savable, saver):
        return self.TYPE.save_instance_state(obj, saver)

    def load_instance_state(self, obj, saved_state: types.Savable, loader):
        self.TYPE.load_instance_state(obj, saved_state, loader)


class SnapshotRefHelper(TypeHelper):
    """Add ability to store references"""
    TYPE = records.Ref
    TYPE_ID = uuid.UUID('05fe092b-07b3-4ffc-8cf2-cee27aa37e81')

    def eq(self, one, other):
        if not (isinstance(one, records.Ref) and isinstance(other, records.Ref)):
            return False

        return one.obj_id == other.obj_id and one.version == other.version

    def yield_hashables(self, obj, hasher):
        yield from hasher.yield_hashables(obj.obj_id)
        yield from hasher.yield_hashables(obj.version)

    def save_instance_state(self, obj, saver):
        return obj.to_list()

    def load_instance_state(self, obj, saved_state, loader):
        obj.__init__(*saved_state)
