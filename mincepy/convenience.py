from . import history

__all__ = 'load', 'save'


def load(*obj_ids_or_refs):
    return history.get_historian().load(*obj_ids_or_refs)


def save(*objs, with_meta=None, return_sref=False):
    return history.get_historian().save(*objs, with_meta=with_meta, return_sref=return_sref)
