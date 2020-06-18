import collections
from typing import Type, MutableMapping, Iterable, Any, Union

from . import helpers
from . import types

SavableObjectType = Type[types.SavableObject]


class TypeRegistry:
    """The type registry.  This contains helpers that furnish mincepy with the necessary methods
    to store and track objects in the archive"""

    def __init__(self):
        self._helpers = {}  # type: MutableMapping[SavableObjectType, helpers.TypeHelper]
        self._type_ids = {}  # type: MutableMapping[Any, SavableObjectType]

    def __contains__(self, item: SavableObjectType) -> bool:
        return item in self._helpers

    @property
    def type_helpers(self) -> MutableMapping[Type, helpers.TypeHelper]:
        """Get the mapping of registered type helpers"""
        return self._helpers

    def register_type(
            self, obj_class_or_helper: Union[helpers.TypeHelper, SavableObjectType]) \
            -> helpers.WrapperHelper:
        """Register a type new type"""
        helper = self._register(obj_class_or_helper)

        # Now, put in any ancestors
        for ancestor in reversed(types.savable_mro(helper.TYPE)[1:]):
            self._register(ancestor)

        return helper

    def get_type_id(self, obj_type: SavableObjectType):
        try:
            # Try a direct lookup first
            return self._helpers[obj_type].TYPE_ID
        except KeyError:
            # Try an issubclass lookup as a backup
            for type_id, known_type in self._type_ids.items():
                if issubclass(obj_type, known_type):
                    return type_id
            raise ValueError("Type '{}' is not known".format(obj_type))

    def get_helper(self, type_id_or_type) -> helpers.TypeHelper:
        if isinstance(type_id_or_type, type):
            return self.get_helper_from_obj_type(type_id_or_type)

        return self.get_helper_from_type_id(type_id_or_type)

    def get_helper_from_type_id(self, type_id) -> helpers.TypeHelper:
        try:
            return self.get_helper_from_obj_type(self._type_ids[type_id])
        except KeyError:
            raise TypeError("Type id '{}' not known".format(type_id))

    def get_helper_from_obj_type(self, obj_type: SavableObjectType) -> helpers.TypeHelper:
        try:
            # Try the direct lookup
            return self._helpers[obj_type]
        except KeyError:
            # Do the full issubclass lookup
            for known_type, helper in self._helpers.items():
                if issubclass(obj_type, known_type):
                    return helper
            raise ValueError("Type '{}' has not been registered".format(obj_type))

    def get_version_info(self, type_id_or_type) -> collections.OrderedDict:
        """Get version information about a type.  This will return a reverse mro ordered dictionary
        where the key is the type id and the value is the version.  Only registered entries will
        appear."""
        helper = self.get_helper(type_id_or_type)
        type_info = collections.OrderedDict()

        # Get information for myself
        type_info[helper.TYPE_ID] = helper.get_version()

        # Get information for everything in the mro
        for type_id in reversed(helper.TYPE.mro()):
            try:
                helper = self.get_helper(type_id)
            except ValueError:
                pass
            else:
                type_info[helper.TYPE_ID] = helper.get_version()

        return type_info

    def _register(
            self, obj_class_or_helper: Union[helpers.TypeHelper, SavableObjectType]) \
            -> helpers.WrapperHelper:
        """Register a type and return the associated helper"""
        if isinstance(obj_class_or_helper, helpers.TypeHelper):
            helper = obj_class_or_helper
        else:
            if not issubclass(obj_class_or_helper, types.Object):
                raise TypeError("Type '{}' is nether a TypeHelper nor a SavableObject".format(
                    obj_class_or_helper))
            helper = helpers.WrapperHelper(obj_class_or_helper)

        self._insert_helper(helper)
        return helper

    def _insert_helper(self, helper: helpers.TypeHelper):
        """Insert a helper into the registry for all the types that it supports"""
        obj_types = helper.TYPE if isinstance(helper.TYPE, Iterable) else (helper.TYPE,)

        for obj_type in obj_types:
            self._helpers[obj_type] = helper
            self._type_ids[helper.TYPE_ID] = obj_type
