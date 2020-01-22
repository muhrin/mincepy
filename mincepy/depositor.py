from abc import ABCMeta, abstractmethod
import collections.abc

from . import archive
from . import types

__all__ = ('Referencer',)

TYPE_KEY = '!!type'
STATE_KEY = '!!state'


class Referencer(metaclass=ABCMeta):
    """
    A class capable of looking up objects form archive ids and archive ids from objects
    """

    def __init__(self, historian):
        self._historian = historian

    @abstractmethod
    def ref(self, obj) -> archive.Ref:
        """Get a persistent reference for the given object"""

    def ref_many(self, objs: collections.abc.Iterable):
        """Create a persistent reference for a number of objects"""
        return [self.ref(obj) for obj in objs]

    def autoref(self, obj):
        """
        Automatically create a reference for an object.  If the type is one of the base
        types it will be returned as is.  If it is a list or a dictionary the datastructure
        will be traversed with entries also being autoreffed
        """
        if type(obj) in types.BASE_TYPES:
            if isinstance(obj, list):
                return [self.autoref(entry) for entry in obj]
            if isinstance(obj, dict):
                return {key: self.autoref(val) for key, val in obj.items()}
            return obj

        # Assume we should create a reference for the object
        return self.ref(obj)

    @abstractmethod
    def deref(self, reference: archive.Ref):
        """Retrieve an object given a persistent reference"""

    def deref_many(self, obj_ids: collections.abc.Iterable):
        """Dereference a number of objects given their persistent ids"""
        return [self.deref(obj_id) for obj_id in obj_ids]

    def autoderef(self, obj):
        """
        Automatically dereference a passed object.  If a list or dict is encountered these will
        be traversed autodereffing entries.
        """
        if type(obj) in types.BASE_TYPES:
            if isinstance(obj, list):
                return [self.autoderef(entry) for entry in obj]
            if isinstance(obj, dict):
                return {key: self.autoderef(val) for key, val in obj.items()}
            return obj

        return self.deref(obj)

    # def to_dict(self, savable: types.Savable) -> dict:
    #     helper = self._historian.get_helper_from_obj_type(type(savable))
    #     return {
    #         TYPE_KEY: helper.TYPE_ID,
    #         STATE_KEY: self.encode(savable, self)
    #     }
    #
    # def from_dict(self, state_dict: dict, referencer: depositor.Referencer):
    #     if not isinstance(state_dict, dict):
    #         raise TypeError("State dict is of type '{}', should be dictionary!".format(type(state_dict)))
    #     if not (TYPE_KEY in state_dict and STATE_KEY in state_dict):
    #         raise ValueError("Passed non-state-dictionary: '{}'".format(state_dict))
    #
    #     with self.create_from(state_dict[STATE_KEY], self.get_helper(state_dict[TYPE_KEY]), referencer) as obj:
    #         return obj
