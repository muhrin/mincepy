from . import history

__all__ = 'load', 'save'


def load(*obj_ids_or_refs):
    return history.get_historian().load(*obj_ids_or_refs)


def save(*objs):
    return history.get_historian().save(*objs)
