"""Module that contains aggregation operations"""


def and_(*conditions) -> dict:
    """Helper that produces query dict for AND of multiple conditions"""
    if len(conditions) == 1:
        return conditions[0]

    return {'$and': list(conditions)}


def eq_(one, other) -> dict:
    """Helper that produces mongo aggregation dict for two items being equal"""
    return {'$eq': [one, other]}


def in_(*possibilities) -> dict:
    """Tests if a value is one of the possibilities"""
    return {'$in': possibilities}
