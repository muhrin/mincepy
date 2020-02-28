import os
from typing import Optional

from . import archive_factory
from . import historian
from . import plugins

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


def set_historian(new_historian: Optional[historian.Historian], apply_plugins=True):
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    CURRENT_HISTORIAN = new_historian
    if new_historian is not None and apply_plugins:
        new_historian.register_types(plugins.get_types())
