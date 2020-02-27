import os
from typing import Optional

from . import archive_factory
from . import builtins
from . import common_helpers
from . import historian
from . import refs

__all__ = 'get_historian', 'set_historian'

MINCEPY_URI = 'MINCEPY_ARCHIVE'
CURRENT_HISTORIAN = None


def get_archive_uri():
    return os.environ.get(MINCEPY_URI, '')


def create_default_historian():
    archive_uri = get_archive_uri()
    if archive_uri:
        return archive_factory.create_historian(archive_uri)

    return None


def get_historian() -> historian.Historian:
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    if CURRENT_HISTORIAN is None:
        # Try creating a new one
        set_historian(create_default_historian())

    return CURRENT_HISTORIAN


def set_historian(new_historian: Optional[historian.Historian], register_common_types=True):
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    CURRENT_HISTORIAN = new_historian
    if new_historian is not None and register_common_types:
        new_historian.register_types(common_helpers.HISTORIAN_TYPES)
        new_historian.register_types(builtins.HISTORIAN_TYPES)
        new_historian.register_types(refs.HISTORIAN_TYPES)
