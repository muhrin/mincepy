"""Module containing functions to generate query operations.  To prevent clashes with python
builtins we append underscores to the function names.  This also makes it safer to import this
module as a wildcard import.
"""

__all__ = ('and_', 'or_', 'eq_', 'exists_', 'elem_match_', 'gt_', 'gte_', 'in_', 'lt_', 'lte_',
           'ne_', 'nin_')

# region Logical operators


def and_(*conditions) -> dict:
    """Helper that produces query dict for AND of multiple conditions"""
    if len(conditions) == 1:
        return conditions[0]

    return {'$and': list(conditions)}


def or_(*conditions) -> dict:
    """Helper that produces query dict for OR of multiple conditions"""
    if len(conditions) == 1:
        return conditions[0]

    return {'$or': list(conditions)}


# endregion


def eq_(one, other) -> dict:
    """Helper that produces mongo query dict for two items being equal"""
    return {'$eq': [one, other]}


# region Element operators


def exists_(key, value: bool = True) -> dict:
    """Return condition for the existence of a key.  If True, matches if key exists, if False
    matches if it does not."""
    return {key: {'$exists': value}}


# endregion

# region Array operators


def elem_match_(**conditions) -> dict:
    """Match an element that is an array and has at least one member that matches all the specified
    conditions"""
    return {'$elemMatch': conditions}


# endregion

# region Comparison operators


def gt_(quantity) -> dict:
    """Match values greater than quantity"""
    return {'$gt': quantity}


def gte_(quantity) -> dict:
    """Match values greater than or equal to quantity"""
    return {'$gte': quantity}


def in_(*possibilities) -> dict:
    """Match values that are equal to any of the possibilities"""
    if len(possibilities) == 1:
        return possibilities[0]

    return {'$in': list(possibilities)}


def lt_(quantity) -> dict:
    """Match values less than quantity"""
    return {'$lt': quantity}


def lte_(quantity) -> dict:
    """Match values less than or equal to quantity"""
    return {'$lte': quantity}


def ne_(value) -> dict:
    """Match values not equal to to value"""
    return {'$ne': value}


def nin_(*possibilities) -> dict:
    """Match values that are not equal to any of the possibilities"""
    if len(possibilities) == 1:
        return possibilities[0]

    return {'$nin': list(possibilities)}


# endregion
