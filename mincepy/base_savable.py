import collections
import typing

from . import types

__all__ = 'BaseSavableObject', 'AsRef'

AsRef = collections.namedtuple('AsRef', 'attr')

AttrSpec = collections.namedtuple('AttrSpec', 'name as_ref')


class BaseSavableObject(types.SavableObject):
    """A helper class that makes a class compatible with the historian by flagging certain
    attributes which will be saved/loaded/hashed and compared in __eq__.  This should be an
    exhaustive list of all the attributes that define this class.  If more complex functionality
    is needed then the standard SavableComparable interface methods should be overwritten."""
    ATTRS = tuple()
    IGNORE_MISSING = True  # When loading ignore attributes that are missing in the record

    def __new__(cls, *_args, **_kwargs):
        new_instance = super(BaseSavableObject, cls).__new__(cls)
        attrs = {}
        for entry in cls.__mro__:
            try:
                for name in getattr(entry, 'ATTRS'):
                    if name not in attrs:
                        if isinstance(name, AsRef):
                            spec = AttrSpec(name.attr, True)
                        else:
                            spec = AttrSpec(name, False)
                        attrs[name] = spec

            except AttributeError:
                pass
        setattr(new_instance, '__attrs', attrs.values())
        return new_instance

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return all(
            getattr(self, attr.name) == getattr(other, attr.name) for attr in self.__get_attrs())

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables([getattr(self, attr.name) for attr in self.__get_attrs()])

    def save_instance_state(self, _saver) -> dict:
        from . import refs

        saved_state = {}
        for attr in self.__get_attrs():
            item = getattr(self, attr.name)
            if attr.as_ref:
                item = refs.ObjRef(item)
            saved_state[attr.name] = item
        return saved_state

    def load_instance_state(self, saved_state, _loader):
        super(BaseSavableObject, self).load_instance_state(saved_state, _loader)
        for attr in self.__get_attrs():
            try:
                obj = saved_state[attr.name]
            except KeyError:
                if self.IGNORE_MISSING:
                    # Set any missing attributes to None
                    setattr(self, attr.name, None)
                else:
                    raise
            else:
                if attr.as_ref:
                    obj = obj()
                setattr(self, attr.name, obj)

    def __get_attrs(self) -> typing.Sequence[AttrSpec]:
        return getattr(self, '__attrs')
