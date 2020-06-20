from typing import MutableMapping, Any, Optional

from . import utils

__all__ = 'get_info', 'remove'


class StagingArea:
    """This global singleton stores information about objects before they have been saved by the
    historian.  This is useful when there is global information that may or may not be used if the
    object is indeed saved.  If not, the information is simply discarded when the object is
    destructed"""

    # pylint: disable=too-few-public-methods

    staged_obj_info = utils.WeakObjectIdDict()  # type: MutableMapping[Any, dict]

    def __init__(self):
        raise RuntimeError("Cannot be instantiated")


def get_info(obj, create=True) -> Optional[dict]:
    """Get the information dictionary for a given staged object.  If create is True, the
    dictionary will be created if it doesn't already exist.  If False and the object is not
    staged then None will be returned.
    """
    if create:
        return StagingArea.staged_obj_info.setdefault(obj, {})

    try:
        return StagingArea.staged_obj_info[obj]
    except KeyError:
        return None


def remove(obj):
    """Remove the information dictionary for a staged object.  If the object is not staged
    then this function does nothing"""
    StagingArea.staged_obj_info.pop(obj)


def replace(old, new):
    """Move over the staging information from object 'old' to object 'new'"""
    try:
        staging_info = StagingArea.staged_obj_info.pop(old)
    except KeyError:
        # Delete any information present for new
        StagingArea.staged_obj_info.pop(new, None)
    else:
        StagingArea.staged_obj_info[new] = staging_info
