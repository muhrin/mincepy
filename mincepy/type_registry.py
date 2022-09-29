# -*- coding: utf-8 -*-
import collections
from typing import Type, MutableMapping, Any, Union

from . import helpers
from . import types

SavableObjectType = Type[types.SavableObject]
RegisterableType = Union[
    helpers.TypeHelper, Type[helpers.TypeHelper], SavableObjectType
]


class TypeRegistry:
    """The type registry.  This contains helpers that furnish mincepy with the necessary methods
    to store and track objects in the archive"""

    def __init__(self):
        self._helpers = (
            {}
        )  # type: MutableMapping[SavableObjectType, helpers.TypeHelper]
        self._type_ids = {}  # type: MutableMapping[Any, SavableObjectType]

    def __contains__(self, item: SavableObjectType) -> bool:
        return item in self._helpers

    @property
    def type_helpers(self) -> MutableMapping[Type, helpers.TypeHelper]:
        """Get the mapping of registered type helpers"""
        return self._helpers

    def register_type(
        self,
        obj_class_or_helper: RegisterableType,
        replace=False,
    ) -> helpers.WrapperHelper:
        """Register a type new type

        :param obj_class_or_helper: the type helper of savable object to register
        :param replace: if True, will silently replace an entry that has the same type id, otherwise raises a
            ValueError the id is already registered
        """
        helper = self._register(obj_class_or_helper, replace)

        # Now, put in any ancestors
        for ancestor in reversed(types.savable_mro(helper.TYPE)[1:]):
            self._register(ancestor, replace)

        return helper

    def unregister_type(self, item: Union[helpers.TypeHelper, SavableObjectType, Any]):
        """Un-register a type helper.  If the type is not registered, this method will return with no effect.

        :param item: either a `TypeHelper` (for mapped type), a `SavableObjectType` or a type id.  The checks will be
            performed in this order.
        """
        try:
            self._remove_using_type_id(item.TYPE_ID)
        except AttributeError:
            # Maybe it is a type id
            self._remove_using_type_id(item)

    def get_type_id(self, obj_type: SavableObjectType):
        """Given a type return the corresponding type id if it registered with this registry"""
        if obj_type in self._type_ids:
            # We've been passed a known type id
            return obj_type

        try:
            # Try a direct lookup first
            return self._helpers[obj_type].TYPE_ID
        except KeyError:
            # Try an issubclass lookup as a backup
            for type_id, known_type in self._type_ids.items():
                if issubclass(obj_type, known_type):
                    return type_id

        raise ValueError(f"Type '{obj_type}' is not known")

    def get_helper(self, type_id_or_type) -> helpers.TypeHelper:
        if isinstance(type_id_or_type, type):
            return self.get_helper_from_obj_type(type_id_or_type)

        return self.get_helper_from_type_id(type_id_or_type)

    def get_helper_from_type_id(self, type_id) -> helpers.TypeHelper:
        try:
            return self.get_helper_from_obj_type(self._type_ids[type_id])
        except KeyError:
            raise TypeError(f"Type id '{type_id}' not known") from None

    def get_helper_from_obj_type(
        self, obj_type: SavableObjectType
    ) -> helpers.TypeHelper:
        try:
            # Try the direct lookup
            return self._helpers[obj_type]
        except KeyError:
            # Do the full issubclass lookup
            for known_type, helper in self._helpers.items():
                if issubclass(obj_type, known_type):
                    return helper
            raise ValueError(f"Type '{obj_type}' has not been registered") from None

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
        self,
        obj_class_or_helper: RegisterableType,
        replace: bool,
    ) -> helpers.WrapperHelper:
        """Register a type and return the associated helper"""
        if obj_class_or_helper is None:
            raise ValueError("Must supply object type or helper, got None")

        if isinstance(obj_class_or_helper, type) and issubclass(
            obj_class_or_helper, helpers.TypeHelper
        ):
            # Try automatically constructing the helper
            # relies on 0-argument constructor being present
            obj_class_or_helper = obj_class_or_helper()

        if isinstance(obj_class_or_helper, helpers.TypeHelper):
            helper = obj_class_or_helper
        else:
            if not issubclass(obj_class_or_helper, types.Object):
                raise TypeError(
                    f"Type '{obj_class_or_helper}' is nether a TypeHelper nor a SavableObject"
                )
            helper = helpers.WrapperHelper(obj_class_or_helper)

        self._insert_helper(helper, replace=replace)
        return helper

    def _insert_helper(self, helper: helpers.TypeHelper, replace=False):
        """Insert a helper into the registry for all the types that it supports"""
        obj_types = (
            helper.TYPE if isinstance(helper.TYPE, tuple) else (helper.TYPE,)
        )  # pylint: disable=isinstance-second-argument-not-valid-type

        for obj_type in obj_types:
            type_id = helper.TYPE_ID

            if (
                not replace
                and type_id in self._type_ids
                and self._type_ids[type_id] is not obj_type
            ):
                raise ValueError(
                    f"Helper for type id '{helper.TYPE_ID}' already exists for type '{self._type_ids[type_id]}' but "
                    f"it is attempting to be replace by '{obj_type.__name__}'.  "
                    f"Call with replace=True if this is intentional."
                )

            self._helpers[obj_type] = helper
            self._type_ids[helper.TYPE_ID] = obj_type

    def _remove_using_type_id(self, type_id: Any):
        obj_type = self._type_ids.pop(type_id, None)
        if obj_type is not None:
            self._helpers.pop(obj_type)
