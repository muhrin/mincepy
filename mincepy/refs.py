"""References module"""
import uuid

from . import archive
from . import types

__all__ = ('ObjRef', 'auto_deref')


def auto_deref(obj):
    if isinstance(obj, ObjRef):
        return obj()
    if isinstance(obj, dict):
        return {key: auto_deref(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [auto_deref(item) for item in obj]
    return obj


class ObjRef(types.SavableObject):
    TYPE_ID = uuid.UUID('633c7035-64fe-4d87-a91e-3b7abd8a6a28')

    def __init__(self, obj, auto=False):
        super().__init__()
        self._obj = obj
        self._auto = auto
        self._ref = None
        self._loader = None

    def __call__(self):
        if self._obj is None:
            # Cache the object
            self._obj = self._loader.load(self._ref)
            self._ref = None
            self._loader = None

        return self._obj

    def __eq__(self, other):
        if not isinstance(other, ObjRef):
            return False

        return self._obj == other._obj

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables(self._ref)

    def save_instance_state(self, saver):
        ref = saver.ref(self._obj)
        return {'ref': [ref.obj_id, ref.version], 'auto': self._auto}

    def load_instance_state(self, saved_state, loader):
        self._ref = archive.Ref(*saved_state['ref'])
        self._obj = None
        self._loader = loader
        self._auto = saved_state['auto']
