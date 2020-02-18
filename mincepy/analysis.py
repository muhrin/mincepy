"""Tools to analyse the state of the historian and archive"""

from typing import Iterable

import mincepy

__all__ = 'get_type_name', 'get_table'

SCALAR_VALUE = '[value]'
UNSET = '[unset]'


def get_type_name(obj_type) -> str:
    """Get a simplified type name for the type, say, given by a helper"""
    try:
        return "{}.{}".format(obj_type.__module__, obj_type.__name__)
    except AttributeError:
        return str(obj_type)


def get_table(records: Iterable, progress_callback=None):
    dicts = []
    obj_ids = []

    for idx, record in enumerate(records):
        # First get all the columns in this record
        paths = list(get_paths(record.state))
        state_values = {}

        for path in paths:
            if isinstance(path, list):
                str_path = ".".join(path)
            else:
                str_path = path
            value = get_value(path, record.state)
            state_values[str_path] = str(value)

        obj_ids.append(record.obj_id)
        dicts.append(state_values)
        if progress_callback is not None:
            progress_callback(idx)

    # Now we've collected all the information we need let's gather it all together
    # Get all the unique keys
    all_keys = set()
    for state_values in dicts:
        all_keys.update(state_values.keys())

    # 0th row always contains column names
    rows = [['Obj id'] + list(all_keys)]
    # Now fill in all the values
    for obj_id, state_values in zip(obj_ids, dicts):
        row = [str(obj_id)]
        for key in rows[0][1:]:
            row.append(state_values.get(key, UNSET))
        rows.append(row)

    return rows


def get_paths(state):
    if isinstance(state, dict):
        for key, value in state.items():
            key_path = [key]
            if isinstance(value, dict):
                for path in get_paths(value):
                    yield key_path + path
            else:
                yield key_path
    elif isinstance(state, list):
        yield from [(idx,) for idx in range(len(state))]
    else:
        yield SCALAR_VALUE


def get_value(path, state):
    if isinstance(state, (dict, list)):
        idx = path[0]
        value = state[idx]
        # Check for references
        if isinstance(value, dict) and set(value.keys()) == {'type_id', 'state'}:
            return str(mincepy.Ref(*value['state']))

        if len(path) > 1:
            return get_value(path[1:], value)

        try:
            return state[idx]
        except (KeyError, IndexError):
            return UNSET

    if path == SCALAR_VALUE:
        return state

    return UNSET
