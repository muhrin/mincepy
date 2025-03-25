from . import testing


def get_types():
    """Provide a list of all historian types"""
    types = []
    types.extend(testing.HISTORIAN_TYPES)

    return types
