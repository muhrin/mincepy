# -*- coding: utf-8 -*-
"""Module for methods related to saving and loading objects to/from records"""

from typing import Type, Union

from . import fields
from . import refs


def save_instance_state(obj, db_type: Type[fields.WithFields] = None):
    """Save the instance state of an object.

    Given an object this function takes a DbType specifying the attributes to be saved and will used
    these to return a saved sate.  Note, that for regular Savable objects, the db_type is the object
    itself in which case this argument can be omitted.
    """
    if db_type is None:
        assert issubclass(type(obj), fields.WithFields), \
            "A DbType wasn't passed and obj isn't a DbType instance other"
        db_type = type(obj)

    field_properties = fields.get_field_properties(db_type).values()
    state = {}

    for properties in field_properties:
        attr_val = getattr(obj, properties.attr_name)
        if properties.ref and attr_val is not None:
            attr_val = refs.ObjRef(attr_val)

        # Check if it's still a field because otherwise it hasn't been set yet
        if attr_val is not properties:
            state[properties.store_as] = attr_val

    return state


def load_instance_state(obj,
                        state: Union[list, dict],
                        db_type: Type[fields.WithFields] = None,
                        ignore_missing=True):
    if db_type is None:
        assert issubclass(type(obj), fields.WithFields), \
            "A DbType wasn't passed and obj isn't a DbType instance other"
        db_type = type(obj)

    to_set = {}
    if isinstance(state, dict):
        for properties in fields.get_field_properties(db_type).values():
            try:
                value = state[properties.store_as]
            except KeyError:
                if ignore_missing:
                    value = None
                else:
                    raise ValueError("Saved state missing '{}'".format(
                        properties.store_as)) from None

            if properties.ref and value is not None:
                assert isinstance(value, refs.ObjRef), \
                    'Expected to see a reference in the saved state for key ' \
                    "'{}' but got '{}'".format(properties.store_as, value)
                value = value()  # Dereference it

            to_set[properties.attr_name] = value

    for attr_name, value in to_set.items():
        setattr(obj, attr_name, value)
