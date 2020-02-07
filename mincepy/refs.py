"""References module"""
from abc import ABCMeta, abstractmethod

from . import history
from . import types


class Ref(types.Object, metaclass=ABCMeta):
    @abstractmethod
    def __call__(self):
        """Get the object referred to by this reference"""


class SnapshotRef(types.Savable, Ref):
    def __init__(self, obj_id, version, obj=None, historian=None):
        super().__init__()
        self._obj_id = obj_id
        self._version = version
        self._obj = obj
        self._historian = historian or history.get_historian()

    def __call__(self):
        if self._obj is None:
            # We need to cache the snapshot because the historian does not
            self._obj = self._historian.load_snapshot(self._obj_id, self._version)

        return self._obj

    def __eq__(self, other):
        if not isinstance(other, SnapshotRef):
            return False

        return self._obj_id == other._obj_id and self._version == other._version

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables((self._obj_id, self._version))

    def save_instance_state(self, depositor):
        return [self._obj_id, self._version]

    def load_instance_state(self, saved_state, depositor):
        self._obj = None
        self._obj_id, self._version = saved_state
        self._historian = depositor.get_historian()


class LiveRef(Ref):
    def __init__(self, obj_id, historian=None):
        super(LiveRef, self).__init__()
        self._obj_id = obj_id
        self._historian = historian or history.get_historian()

    def __call__(self):
        # No need to do any caching here, the historian caches the
        # values if they're in use anyway
        return self._historian.load(self._obj_id)

    def __eq__(self, other):
        if not isinstance(other, LiveRef):
            return False

        return self._obj_id == other._obj_id

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables(self._obj_id)
