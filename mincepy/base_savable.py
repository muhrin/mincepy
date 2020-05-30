import collections
from typing import Optional
import typing

import mincepy
from . import depositors
from . import refs
from . import types

__all__ = 'BaseSavableObject', 'ConvenienceMixin', 'SimpleSavable', 'AsRef'

AttrSpec = collections.namedtuple('AttrSpec', 'name as_ref')


def AsRef(name: str) -> AttrSpec:  # pylint: disable=invalid-name
    """Create an attribute specification for an attribute that should be stored by reference"""
    return AttrSpec(name, True)


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
                class_attrs = getattr(entry, 'ATTRS')
            except AttributeError:
                pass
            else:
                for attr_spec in class_attrs:
                    if isinstance(attr_spec, str):
                        # If it's just a string then default to store by value
                        attr_spec = AttrSpec(attr_spec, False)

                    # Check that it's not already there so higher up in the MRO is always kept
                    if attr_spec.name not in attrs:
                        attrs[attr_spec.name] = attr_spec

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
        saved_state = {}
        for attr in self.__get_attrs():
            item = getattr(self, attr.name)
            if attr.as_ref:
                item = refs.ObjRef(item)
            saved_state[attr.name] = item

        super().save_instance_state(_saver)
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


class ConvenienceMixin:
    """A mixin that adds convenience methods to your savable object"""

    @property
    def obj_id(self):
        if self._historian is None:
            return None
        return self._historian.get_obj_id(self)

    def get_meta(self) -> Optional[dict]:
        """Get the metadata dictionary for this object"""
        if self._historian is None:
            return None
        return self._historian.meta.get(self)

    def set_meta(self, meta: Optional[dict]):
        """Set the metadata dictionary for this object"""
        if self._historian is None:
            raise RuntimeError("Object must be saved before the metadata can be set")

        self._historian.meta.set(self, meta)

    def update_meta(self, meta: dict):
        """Update the metadata dictionary for this object"""
        if self._historian is None:
            raise RuntimeError("Object must be saved before the metadata can be updated")

        self._historian.meta.update(self, meta)

    def is_saved(self) -> bool:
        """Returns True if this object is saved with a historian"""
        if self._historian is not None:
            return self._historian.is_saved(self)

        return False

    def save(self, meta: dict = None):
        """Save the object"""
        historian = self._historian or mincepy.get_historian()
        return historian.save_one(self, meta=meta)

    def sync(self):
        """Update the state of this object by loading the latest version from the historian"""
        if self._historian is not None:
            self._historian.sync(self)

    def save_instance_state(self, saver: depositors.Saver):
        self._on_save(saver)
        return super().save_instance_state(saver)

    def load_instance_state(self, saved_state, loader: 'mincepy.Loader'):
        """Take the given object and load the instance state into it"""
        super().load_instance_state(saved_state, loader)
        self._on_load(loader)

    def _on_save(self, saver: depositors.Saver):
        # Check if we've been assigned an object id, otherwise we're just being saved by value
        if saver.get_historian().get_obj_id(self) is not None:
            self._historian = saver.get_historian()

    def _on_load(self, loader: depositors.Loader):
        if loader.get_historian().get_obj_id(self) is not None:
            self._historian = loader.get_historian()


class SimpleSavable(ConvenienceMixin, BaseSavableObject):
    """A BaseSavableObject with convenience methods mixed in"""
