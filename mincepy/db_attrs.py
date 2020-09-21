"""Module that contains methods and classes for dealing with database storable attributes of
objects"""

import abc
from typing import Union, Dict, Type

from . import refs

__all__ = ('db_attr',)


class DbAttr:
    """Database attribute class.  Provides information about how to store object attributes in the
    database"""

    def __init__(self, ref=False, store_as: str = None, attr: str = None):
        """
        Create a database attribute.

        :param ref: if True will store the object by reference
        param store_as: the name to use when storing this attribute in the state dictionary
        :param attr: the name of the class attribute
        """
        self._ref = ref
        self._store_as = store_as
        self._attr = attr
        self._db_class = None

        self._kwargs = {'ref': ref, 'store_as': store_as, 'attr': attr}

    @property
    def ref(self):
        return self._ref

    @property
    def db_class(self):
        return self._db_class

    @property
    def attr(self) -> str:
        return self._attr

    @property
    def store_as(self) -> str:
        return self._store_as

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self

        try:
            return obj.__dict__[self._attr]
        except KeyError:
            return self

    def __gt__(self, other):
        type_id = self.db_class.TYPE_ID
        return {'type_id': type_id, "state.{}".format(self._store_as): {'$gt': other}}

    def __call__(self, *args, **kwargs):
        """Means of promoting a database property to be a python property on a class"""
        return DbProperty(*args, **kwargs, prop_kwargs=self._kwargs)

    def class_created(self, the_class: type, attr: str):
        """Called by the metaclass when the owning class is created, should only be done once"""
        assert self._db_class is None, "Cannot call class_created more than once"

        self._db_class = the_class
        self._attr = attr
        if self._store_as is None:
            self._store_as = attr


class DbProperty(DbAttr):

    def __init__(self, fget=None, fset=None, fdel=None, doc=None, prop_kwargs=None):
        prop_kwargs = prop_kwargs or {}
        if prop_kwargs.get('store_as', None) is None:
            prop_kwargs['store_as'] = fget.__name__

        super(DbProperty, self).__init__(**prop_kwargs)
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(obj)

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        self.fset(obj, value)

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")
        self.fdel(obj)

    def getter(self, fget):
        return type(self)(fget, self.fset, self.fdel, self.__doc__, prop_kwargs=self._kwargs)

    def setter(self, fset):
        return type(self)(self.fget, fset, self.fdel, self.__doc__, prop_kwargs=self._kwargs)

    def deleter(self, fdel):
        return type(self)(self.fget, self.fset, fdel, self.__doc__, prop_kwargs=self._kwargs)


def db_attr(ref=False, store_as: str = None):
    return DbAttr(ref=ref, store_as=store_as)


class DbTypeMeta(abc.ABCMeta):
    """Metaclass for database types"""

    def __new__(cls, name, bases, namespace, **kwargs):  # pylint: disable=bad-mcs-classmethod-argument
        new_type = super().__new__(cls, name, bases, namespace, **kwargs)
        for key, value in namespace.items():
            if isinstance(value, DbAttr):
                value.class_created(new_type, key)
        return new_type


class DbType(metaclass=DbTypeMeta):  # pylint: disable=too-few-public-methods
    """Base class for types that describe how to save objects in the database using db attributes"""


def get_db_attrs(db_type: Type[DbType]) -> Dict[str, DbAttr]:
    """Given an object this will return all the database attributes as a dictionary where the key is
    the attribute name"""
    db_attrs = {}
    for entry in reversed(db_type.__mro__):
        if entry is object:
            continue
        for name, class_attr in entry.__dict__.items():
            if isinstance(class_attr, DbAttr):
                db_attrs[name] = class_attr

    return db_attrs


def save_instance_state(obj, db_type: Type[DbType] = None):
    """Save the instance state of an object.

    Given an object this function takes a DbType specifying the attributes to be saved and will used
    these to return a saved sate.  Note, that for regular Savable objects, the db_type is the object
    itself in which case this argument can be omitted.
    """
    if db_type is None:
        assert issubclass(type(obj), DbType), \
            "A DbType wasn't passed and obj isn't a DbType instance other"
        db_type = type(obj)

    to_check = get_db_attrs(db_type)
    state = {}

    for name, class_attr in to_check.items():
        attr_val = getattr(obj, name)
        if class_attr.ref:
            attr_val = refs.ObjRef(attr_val)

        # Check if it's still a field because otherwise it hasn't been set yet
        if attr_val is not class_attr:
            state[class_attr.store_as] = attr_val

    return state


def load_instance_state(obj,
                        state: Union[list, dict],
                        db_type: Type[DbType] = None,
                        ignore_missing=True):
    if db_type is None:
        assert issubclass(type(obj), DbType), \
            "A DbType wasn't passed and obj isn't a DbType instance other"
        db_type = type(obj)

    if isinstance(state, dict):
        db_attrs = {attr.store_as: attr for attr in get_db_attrs(db_type).values()}

        for store_as, attr in db_attrs.items():
            try:
                value = state[store_as]
            except KeyError:
                if ignore_missing:
                    value = None
                else:
                    raise

            if attr.ref:
                assert isinstance(value, refs.ObjRef), \
                    "Expected to see a reference in the bundle for key " \
                    "'{}' but got '{}'".format(store_as, value)
                if value:
                    value = value()  # Dereference it

            setattr(obj, attr.attr, value)
