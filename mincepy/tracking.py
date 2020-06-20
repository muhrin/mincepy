import copy as python_copy
import functools
from typing import Callable

from . import records
from . import staging

__all__ = 'track', 'copy', 'deepcopy', 'mark_as_copy'


class TrackStack:
    """Stack to keep track of functions being called"""

    _stack = []

    def __init__(self):
        raise RuntimeError("Cannot be instantiated")

    @classmethod
    def peek(cls):
        if not cls._stack:
            return None
        return cls._stack[-1]

    @classmethod
    def push(cls, obj):
        """Push an object onto the stack"""
        cls._stack.append(obj)

    @classmethod
    def pop(cls, obj):
        """Pop an object off of the stack"""
        if cls._stack[-1] != obj:
            raise RuntimeError("Someone has corrupted the process stack!\n"
                               "Expected to find '{}' on top but found '{}'".format(
                                   obj, cls._stack[-1]))

        cls._stack.pop()


class TrackContext:
    """Context manager that pushes and pops the object to/from the track stack"""

    def __init__(self, obj):
        self._obj = obj

    def __enter__(self):
        TrackStack.push(self._obj)

    def __exit__(self, exc_type, exc_val, exc_tb):
        TrackStack.pop(self._obj)


def track(obj_or_fn):
    """Allows object creation to be tracked.  When an object is created within this context, the
    creator of the object will be saved in the database record.

    This can be used either as a decorator to a class method, in which case the object instance will
    be the creator.  Or it can be used as a context in which case the creator should be passed as
    the argument.
    """
    if isinstance(obj_or_fn, Callable):
        # We're acting as a decorator
        @functools.wraps(obj_or_fn)
        def wrapper(self, *args, **kwargs):
            with TrackContext(self):
                return obj_or_fn(self, *args, **kwargs)

        return wrapper

    # We're acting as a context
    return TrackContext(obj_or_fn)


def obj_created(obj):
    creator = TrackStack.peek()
    if creator is not None:
        staging.get_info(obj)[records.ExtraKeys.CREATED_BY] = creator


def mark_as_copy(obj, copied_from):
    staging.get_info(obj)[records.ExtraKeys.COPIED_FROM] = copied_from


def copy(obj):
    """Create a shallow copy of the object.  Using this method allows the historian to inject
    information about where the object was copied from into the record if saved."""
    obj_copy = python_copy.copy(obj)
    mark_as_copy(obj_copy, obj)
    return obj_copy


def deepcopy(obj):
    """Create a shallow copy of the object.  Using this method allows the historian to inject
    information about where the object was copied from into the record if saved."""
    obj_copy = python_copy.deepcopy(obj)
    mark_as_copy(obj_copy, obj)
    return obj_copy
