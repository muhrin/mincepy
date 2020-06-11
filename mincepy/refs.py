"""References module"""
from typing import Optional

from . import exceptions
from . import types
from . import records

__all__ = ('ObjRef',)


class ObjRef(types.SavableObject):
    """A reference to an object instance"""
    TYPE_ID = records.SnapshotId.TYPE_ID
    IMMUTABLE = True

    _obj = None
    _sid = None  # type: Optional[records.SnapshotId]
    _loader = None

    def __init__(self, obj=None):
        super().__init__()
        assert not (obj is not None and types.is_primitive(obj)), \
            "Can't create a reference to a primitive type"
        self._obj = obj

    def __bool__(self) -> bool:
        """Test if this is a null reference"""
        return self._obj is not None or self._sid is not None

    def __str__(self) -> str:
        desc = ["ObjRef('"]
        if self._obj is not None:
            desc.append(str(self._obj))
        else:
            desc.append(str(self._sid))
        desc.append("')")
        return "".join(desc)

    def __repr__(self) -> str:
        return "ObjRef({})".format(self._obj if self._obj is not None else self._sid)

    def __call__(self, update=False):
        """Get the object being referenced.  If update is called then the latest version
        will be loaded from the historian"""
        if self._obj is None:
            # This means we were loaded and need to load the object
            if self._sid is None:
                raise RuntimeError("Cannot dereference a None reference")
            # Cache the object
            self._obj = self._loader.load(self._sid)
            assert self._obj is not None, "Loader did not load object"
            self._sid = None
            self._loader = None
        elif update:
            try:
                self._historian.sync(self._obj)
            except exceptions.NotFound:
                pass  # Object must never have been saved and is therefore up to date

        return self._obj

    def __eq__(self, other) -> bool:
        if not isinstance(other, ObjRef):
            return False

        if self._obj is not None:
            return id(self._obj) == id(other._obj)

        return self._sid == other._sid

    def yield_hashables(self, hasher):
        if self._obj is not None:
            yield from hasher.yield_hashables(id(self._obj))
        else:
            # This will also work if ref is None
            yield from hasher.yield_hashables(self._sid)

    def save_instance_state(self, saver):
        if self._obj is not None:
            sid = saver.get_snapshot_id(self._obj)
        else:
            sid = self._sid

        if sid is not None:
            return sid.to_dict()

        return None

    def load_instance_state(self, saved_state, loader):
        super(ObjRef, self).load_instance_state(saved_state, loader)
        # Rely on class default values for members
        if saved_state is not None:
            if isinstance(saved_state, list):
                # Legacy version
                self._sid = records.SnapshotId(*saved_state)
            else:
                # New version is dict
                self._sid = records.SnapshotId(**saved_state)
            self._loader = loader


HISTORIAN_TYPES = (ObjRef,)
