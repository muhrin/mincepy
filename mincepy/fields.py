"""Module that contains methods and classes for dealing with database storable attributes of
objects"""

import abc
from typing import Dict, Type

from . import expr

__all__ = ('field',)

_UNSET = ()


class FieldProperties:
    __slots__ = ('store_as', 'attr_name', 'ref', 'dynamic', 'field_type', 'default', 'extras',
                 'db_class')

    # pylint: disable=too-many-arguments
    def __init__(self,
                 store_as: str = None,
                 attr: str = None,
                 ref=False,
                 dynamic=True,
                 field_type: Type = None,
                 default=_UNSET,
                 extras=None):
        """
        Fixed properties of a field

        :param store_as: the name to use for this field when storing in the database
        :param attr: the name of the class attribute
        """
        if store_as and '.' in store_as:
            raise ValueError("store_as cannot contain a dot, got '{}'".format(store_as))

        self.store_as = store_as
        self.attr_name = attr
        self.ref = ref
        self.dynamic = dynamic
        self.field_type = field_type
        self.default = default
        self.extras = extras or {}
        self.db_class = None

    def class_created(self, the_class: type, attr: str):
        """Called by the metaclass when the owning class is created, should only be done once"""
        assert self.db_class is None, "Cannot call class_created more than once"
        self.db_class = the_class
        self.attr_name = attr
        if self.store_as is None:
            self.store_as = attr


class Field(expr.WithQueryContext, expr.Queryable):
    """Database field class.  Provides information about how to store object attributes in the
    database"""
    __doc__ = ''

    def __init__(self, properties: FieldProperties, path_prefix=''):
        """
        Create a attribute field that will be stored in the database
        """
        super().__init__()
        self._properties = properties
        self.path_prefix = path_prefix

    def __getattribute__(self, item: str):
        try:
            return object.__getattribute__(self, item)
        except AttributeError as exc:
            # Dynamically create a new field
            if item != "__isabstractmethod__":
                if self._properties.field_type is not None and \
                        issubclass(self._properties.field_type, WithFields):
                    properties = get_field_properties(self._properties.field_type)
                    try:
                        child_field = type(self)(properties[item], path_prefix=self._get_path())
                    except KeyError:
                        raise exc from None
                    else:
                        child_field.set_query_context(self._query_context)
                        return child_field

                if self._dynamic:
                    new_field = type(self)(self._properties, path_prefix=self._get_path())
                    new_field.set_query_context(self._query_context)
                    return new_field

            raise

    def __call__(self, fget=None, fset=None, fdel=None, doc=None, prop_kwargs=None):
        """This method allows the field to become a property"""
        self.getter(fget)
        self.setter(fset)
        self.deleter(fdel)
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc
        return self

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        return self._getter(obj)

    def __set__(self, obj, value):
        if self._setter is None:
            raise AttributeError("can't set attribute")
        self._setter(obj, value)

    def __delete__(self, obj):
        if self._deleter is None:
            raise AttributeError("can't delete attribute")
        self._deleter(obj)

    def getter(self, fget):
        setattr(self, '_getter', fget)
        return self

    def setter(self, fset):
        setattr(self, '_setter', fset)
        return self

    def deleter(self, fdel):
        setattr(self, '_deleter', fdel)
        return self

    def _getter(self, obj):
        """Default getter"""
        try:
            return obj.__dict__[self._properties.attr_name]
        except KeyError:
            raise AttributeError("unreadable attribute '{}'".format(
                self._properties.attr_name)) from None

    def _setter(self, obj, value):
        """Default setter"""
        obj.__dict__[self._properties.attr_name] = value

    def _delete(self, obj):
        """Default deleter"""
        del obj.__dict__[self._properties.attr_name]

    def _get_path(self) -> str:
        if self.path_prefix:
            return self.path_prefix + '.' + self._properties.store_as

        return self._properties.store_as


def field(store_as: str = None, ref=False, default=_UNSET, type=None):  # pylint: disable=redefined-builtin
    properties = FieldProperties(ref=ref, store_as=store_as, default=default, field_type=type)
    return Field(properties)


class WithFieldMeta(abc.ABCMeta):
    """Metaclass for database types"""

    def __init__(cls, name, bases, namespace, *args, **kwargs):
        super().__init__(name, bases, namespace, *args, **kwargs)
        for key, value in namespace.items():
            if isinstance(value, Field):
                cls.init_field(value, key)


class WithFields(metaclass=WithFieldMeta):
    """Base class for types that describe how to save objects in the database using db fields"""

    @classmethod
    def init_field(cls, obj_field, attr_name: str):
        obj_field._properties.class_created(cls, attr_name)  # pylint: disable=protected-access

    def __init__(self, **kwargs):
        for name, field_properties in get_field_properties(type(self)).items():
            try:
                passed_value = kwargs.pop(name)
            except KeyError:
                # Let's see if there's a default
                if field_properties.default is not _UNSET:
                    setattr(self, name, field_properties.default)
            else:
                setattr(self, name, passed_value)

        if kwargs:
            raise ValueError("Got unexpected keyword argument(s) '{}'".format(kwargs))


def get_field_properties(db_type: Type[WithFields]) -> Dict[str, FieldProperties]:
    """Given a WithField type this will return all the database attributes as a dictionary where the
    key is the attribute name"""
    db_attrs = {}
    for entry in reversed(db_type.__mro__):
        if entry is object:
            continue
        for name, class_attr in entry.__dict__.items():
            if isinstance(class_attr, Field):
                db_attrs[name] = class_attr._properties  # pylint: disable=protected-access

    return db_attrs
