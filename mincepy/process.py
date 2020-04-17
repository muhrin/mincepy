import contextlib
import functools
from typing import MutableMapping, Any
import uuid

from . import base_savable
from . import utils

__all__ = 'Process', 'track'


def track(func):

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.running():
            return func(self, *args, **kwargs)

    return wrapper


class Process(base_savable.SimpleSavable):
    TYPE_ID = uuid.UUID('bcf03171-a1f1-49c7-b890-b7f9d9f9e5a2')
    STACK = []
    ATTRS = '_name', '_running'

    @classmethod
    def current_process(cls):
        if not cls.STACK:
            return None
        return cls.STACK[-1]

    def __init__(self, name: str):
        super(Process, self).__init__()
        self._name = name
        self._running = 0

    def __eq__(self, other):
        if not isinstance(other, Process):
            return False

        return self.name == other.name

    @property
    def is_running(self):
        return self._running != 0

    @property
    def name(self) -> str:
        return self._name

    @contextlib.contextmanager
    def running(self):
        self.STACK.append(self)
        self._running += 1
        try:
            yield
        finally:
            if self.STACK[-1] != self:
                raise RuntimeError("Someone has corrupted the process stack!\n"
                                   "Expected to find '{}' on top but found:{}".format(
                                       self, self.STACK))
            self._running -= 1
            self.STACK.pop()


class CreatorsRegistry:
    """Global registry of the creator of each object"""
    _creators = utils.WeakObjectIdDict()  # type: MutableMapping[Any, Any]

    def __init__(self):
        raise RuntimeError("Cannot be instantiated")

    @classmethod
    def created(cls, obj):
        """Called when an object is created.  The historian tracks the creator for saving when
        the object is saved

        :param obj: the object that was created
        :type obj: mincepy.SavableObject
        """
        creator = Process.current_process()
        if creator is not None:
            cls._creators[obj] = creator

    @classmethod
    def get_creator(cls, obj):
        """
        Get the creator of the passed object

        :param obj: the object to get the creator of
        :type obj: mincepy.SavableObject
        :return: Optional[mincepy.SavableObject]
        """
        return cls._creators.get(obj, None)

    @classmethod
    def set_creator(cls, obj, creator):
        """
        Set the creator of an object.  This should no typically be called by user code.

        :param obj: the object to set the creator of
        :type obj: mincepy.SavableObject
        :param creator: the creating object
        :type creator: mincepy.SavableObject
        """
        cls._creators[obj] = creator
