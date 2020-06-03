from . import history

__all__ = 'load', 'save'


def load(*obj_ids_or_refs):
    """Load one or more objects using the current global historian"""
    return history.get_historian().load(*obj_ids_or_refs)


def save(*objs):
    """Save one or more objects using the current global historian"""
    return history.get_historian().save(*objs)
