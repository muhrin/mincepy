"""Module containing functions to generate query operations.  To prevent clashes with python
builtins we append underscores to the function names.  This also makes it safer to import this
module as a wildcard import.
"""

__all__ = 'and_', 'eq_', 'in_', 'ne_', 'exists_', 'or_', 'gt_', 'lt_'


def and_(*conditions) -> dict:
    """Helper that produces mongo query dict for AND of multiple conditions"""
    if len(conditions) == 1:
        return conditions[0]

    return {'$and': list(conditions)}


def or_(*conditions) -> dict:
    """Helper that produces mongo query dict for OR of multiple conditions"""
    if len(conditions) == 1:
        return conditions[0]

    return {'$or': list(conditions)}


def eq_(one, other) -> dict:
    """Helper that produces mongo query dict for to items being equal"""
    return {'$eq': [one, other]}


def in_(*possibilities) -> dict:
    """Helper that produces mongo query dict for items being one of"""
    if len(possibilities) == 1:
        return possibilities[0]

    return {'$in': list(possibilities)}


def ne_(value) -> dict:
    """Not equal to value"""
    return {'$ne': value}


def exists_(key) -> dict:
    """Return condition for the existence of a key"""
    return {key: {'$exists': True}}


def gt_(quantity) -> dict:
    """Match values greater than quantity"""
    return {'$gt': quantity}


def lt_(quantity) -> dict:
    """Match values less than quantity"""
    return {'$lt': quantity}


def elem_match_(**conditions) -> dict:
    """Match an element that is an array and has at least one member that matches all the specified
    conditions"""
    return {'$elemMatch': conditions}
