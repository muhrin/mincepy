import contextlib
import functools
import uuid

import deprecation

from . import base_savable
from . import tracking
from . import version

__all__ = ('Process',)


@deprecation.deprecated(deprecated_in="0.14.5",
                        removed_in="0.16",
                        current_version=version.__version__,
                        details="Use mincepy.track() instead")
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
        try:
            self._running += 1
            with tracking.track(self):
                yield
        finally:
            self._running -= 1
