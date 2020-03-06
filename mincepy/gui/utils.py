import inspect


def obj_dict(obj):
    """Given an object return a dictionary that represents it"""
    repr_dict = {}
    for name in dir(obj):
        if not name.startswith('_'):
            try:
                value = getattr(obj, name)
                if not inspect.isroutine(value):
                    repr_dict[name] = value
            except Exception as exc:
                repr_dict[name] = '{}: {}'.format(type(exc).__name__, exc)

    return repr_dict
