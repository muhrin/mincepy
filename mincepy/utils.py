import collections
import collections.abc
import typing
from typing import TypeVar, Generic
import weakref


class WeakObjectIdDict(collections.MutableMapping):
    """
    Like weakref.WeakKeyDict but internally uses object ids instead of the object reference
    itself thereby avoiding the need for the object to be hashable (and therefore immutable).
    """

    def __init__(self, seq=None, **kwargs):
        self._refs = {}  # type: collections.abc.MutableMapping[int, weakref.ReferenceType]
        self._values = {}  # type: collections.abc.MutableMapping[int, typing.Any]
        if seq:
            if isinstance(seq, collections.abc.Mapping):
                for key, value in seq.items():
                    self[key] = value
            elif isinstance(seq, collections.Iterable):
                for key, value in seq:
                    self[key] = value
        if kwargs:
            for key, value in kwargs.items():
                self[key] = value

    def __copy__(self):
        return WeakObjectIdDict(self)

    def __getitem__(self, item):
        try:
            return self._values[id(item)]
        except KeyError:
            raise KeyError(str(item))

    def __setitem__(self, key, value):
        obj_id = id(key)
        wref = weakref.ref(key, self._finalised)
        self._refs[obj_id] = wref
        self._values[obj_id] = value

    def __delitem__(self, key):
        obj_id = id(key)
        del self._values[obj_id]
        del self._refs[obj_id]

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        for ref in self._refs.values():
            yield ref()

    def _finalised(self, wref):
        found_id = None
        for obj_id, ref in self._refs.items():
            if ref == wref:
                found_id = obj_id
                break
        # Delete both the object values and the reference itself
        del self._values[found_id]
        del self._refs[found_id]


T = TypeVar('T')  # Declare type variable pylint: disable=invalid-name


class NamedTupleBuilder(Generic[T]):
    """A builder that allows namedtuples to be build step by step"""

    def __init__(self, tuple_type: typing.Type[T], defaults={}):
        # Have to do it this way because we overwrite __setattr__
        object.__setattr__(self, '_tuple_type', tuple_type)
        diff = set(defaults.keys()) - set(tuple_type._fields)
        assert not diff, "Can't supply defaults that are not in the namedtuple: '{}'".format(diff)
        object.__setattr__(self, '_values', defaults)

    def __repr__(self):
        """Representation of the object."""
        return '%s(%s)' % (self.__class__.__name__, dict.__repr__(self._values))

    def __getattr__(self, attr):
        """Read a key as an attribute.

        :raises AttributeError: if the attribute does not correspond to an existing key.
        """
        try:
            return self._values[attr]
        except KeyError:
            errmsg = "'{}' object has no attribute '{}'".format(self.__class__.__name__, attr)
            raise AttributeError(errmsg)

    def __setattr__(self, attr, value):
        """Set a key as an attribute."""
        if attr not in self._tuple_type._fields:
            raise AttributeError("AttributeError: '{}' is not a valid attribute of the object "
                                 "'{}'".format(attr, self.__class__.__name__))

        self._values[attr] = value

    def __dir__(self):
        return self._tuple_type._fields

    def update(self, new_values: dict):
        for key, value in new_values.items():
            setattr(self, key, value)

    def build(self) -> T:
        return self._tuple_type(**self._values)
