import collections
import collections.abc
import typing
from typing import TypeVar, Generic
import weakref


class WeakObjectIdDict(collections.MutableMapping):
    def __init__(self):
        self._refs: collections.abc.MutableMapping[int, weakref.ReferenceType] = {}
        self._values: collections.abc.MutableMapping[int, typing.Any] = {}

    def __getitem__(self, item):
        try:
            return self._values[id(item)]
        except KeyError:
            raise KeyError(str(item))

    def __setitem__(self, key, value):
        persistent_id = id(key)
        wref = weakref.ref(key, self._finalised)
        self._refs[persistent_id] = wref
        self._values[persistent_id] = value

    def __delitem__(self, key):
        persistent_id = id(key)
        del self._values[persistent_id]
        del self._refs[persistent_id]

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        for ref in self._refs.values():
            yield ref()

    def _finalised(self, wref):
        found_id = None
        for persistent_id, ref in self._refs.items():
            if ref == wref:
                found_id = persistent_id
                break
        # Delete both the object values and the reference itself
        del self._values[found_id]
        del self._refs[found_id]
