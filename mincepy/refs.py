"""References module"""
import uuid

from . import exceptions
from . import types
from . import records

__all__ = ('ObjRef',)


class ObjRef(types.SavableObject):
    """A reference to an object instance"""
    TYPE_ID = uuid.UUID('633c7035-64fe-4d87-a91e-3b7abd8a6a28')

    _obj = None
    _ref = None
    _loader = None

    def __init__(self, obj):
        super().__init__()
        self._obj = obj

    def __str__(self):
        desc = "ref: "
        if self._obj is not None:
            desc += str(self._obj)
        else:
            desc += str(self._ref)
        return desc

    def __call__(self, update=False):
        """Get the object being referenced.  If update is called then the latest version
        will be loaded from the historian"""
        if self._obj is None:
            # This means we were loaded and need to load the object
            if self._ref is None:
                raise RuntimeError("Cannot dereference a None reference")
            # Cache the object
            self._obj = self._loader.load(self._ref)
            assert self._obj is not None, "Loader did not load object"
            self._ref = None
            self._loader = None
        elif update:
            try:
                self._historian.sync(self._obj)
            except exceptions.NotFound:
                pass  # Object must never have been saved and is therefore up to date

        return self._obj

    def __eq__(self, other):
        if not isinstance(other, ObjRef):
            return False

        if self._obj is not None:
            return id(self._obj) == id(other._obj)

        return self._ref == other._ref

    def assign(self, obj):
        """Assign this reference to point to the given object"""
        self._obj = obj
        self._ref = None
        self._loader = None

    def yield_hashables(self, hasher):
        if self._obj is not None:
            yield from hasher.yield_hashables(id(self._obj))
        else:
            # This will also work if ref is None
            yield from hasher.yield_hashables(self._ref)

    def save_instance_state(self, saver):
        ref = self._ref
        if ref is None:
            # This should mean that we have loaded the object via the 'call' method previously
            assert self._obj is not None
            ref = saver.ref(self._obj)

        return ref.to_list()

    def load_instance_state(self, saved_state, loader):
        self._ref = records.Ref(*saved_state)
        self._loader = loader


HISTORIAN_TYPES = (ObjRef,)
