import os
from typing import Optional

from . import archive_factory
from . import historians
from . import plugins

__all__ = 'get_historian', 'set_historian', 'DEFAULT_ARCHIVE_URI', 'ENV_ARCHIVE_URI', 'archive_uri'

DEFAULT_ARCHIVE_URI = 'mongodb://localhost/mincepy'
ENV_ARCHIVE_URI = 'MINCEPY_ARCHIVE'
CURRENT_HISTORIAN = None


def archive_uri():
    """Returns the currently set archive URI to use by default"""
    return os.environ.get(ENV_ARCHIVE_URI, DEFAULT_ARCHIVE_URI)


def create_default_historian():
    uri = archive_uri()
    if uri:
        return archive_factory.historian(uri)

    return None


def get_historian() -> historians.Historian:
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    if CURRENT_HISTORIAN is None:
        # Try creating a new one
        set_historian(create_default_historian())

    return CURRENT_HISTORIAN


def set_historian(new_historian: Optional[historians.Historian], apply_plugins=True):
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    CURRENT_HISTORIAN = new_historian
    if new_historian is not None and apply_plugins:
        new_historian.register_types(plugins.get_types())
