"""Module containing functions to generate query operations.  Where the name would clash with a
python keyword an underscore has been appended.
"""

__all__ = 'and_', 'eq_', 'in_', 'ne', 'exists'

# We use shorter than normal names here, but that's ok. pylint: disable=invalid-name


def and_(*conditions) -> dict:
    """Helper that produces mongo query dict for AND of multiple conditions"""
    if len(conditions) == 1:
        return conditions[0]

    return {'$and': list(conditions)}


def eq_(one, other) -> dict:
    """Helper that produces mongo query dict for to items being equal"""
    return {'$eq': [one, other]}


def in_(*possibilities) -> dict:
    """Helper that produces mongo query dict for items being one of"""
    if len(possibilities) == 1:
        return possibilities[0]

    return {'$in': list(possibilities)}


def ne(value) -> dict:
    """Not equal to value"""
    return {'$ne': value}


ne_ = ne


def exists(key) -> dict:
    """Return condition for the existence of a key"""
    return {key: {'$exists': True}}


def gt(quantity) -> dict:
    return {'$gt': quantity}


gt_ = gt

exists_ = exists
