import os
from typing import Optional

import deprecation

from . import archive_factory
from . import historians
from . import plugins
from . import version

__all__ = ('connect', 'get_historian', 'set_historian', 'DEFAULT_ARCHIVE_URI', 'ENV_ARCHIVE_URI', \
           'archive_uri', 'load', 'save')

DEFAULT_ARCHIVE_URI = 'mongodb://localhost/mincepy'
ENV_ARCHIVE_URI = 'MINCEPY_ARCHIVE'
CURRENT_HISTORIAN = None


@deprecation.deprecated(deprecated_in="0.15.3",
                        removed_in="0.16.0",
                        current_version=version.__version__,
                        details="Use default_archive_uri() instead")
def archive_uri() -> Optional[str]:
    """Returns the default archive URI.  This is currently being taken from the environmental
    MINCEPY_ARCHIVE, however it may chance to include a config file in the future."""
    return os.environ.get(ENV_ARCHIVE_URI, DEFAULT_ARCHIVE_URI)


@deprecation.deprecated(deprecated_in="0.15.3",
                        removed_in="0.16.0",
                        current_version=version.__version__,
                        details="Use connect(set_global=False) instead")
def create_default_historian():
    """Create a default historian using the current `archive_uri()`"""
    uri = default_archive_uri()
    if uri:
        return archive_factory.create_historian(uri)

    return None


def connect(uri: str = '', use_globally=False) -> historians.Historian:
    """Connect to an archive and return a corresponding historian

    :param uri: the URI of the archive to connect to
    :param use_globally: if True sets the newly create historian as the current global historian
    """
    uri = uri or default_archive_uri()
    hist = archive_factory.create_historian(uri, apply_plugins=True)
    if use_globally:
        set_historian(hist, apply_plugins=False)
    return hist


def default_archive_uri() -> Optional[str]:
    """Returns the default archive URI.  This is currently being taken from the environmental
    MINCEPY_ARCHIVE, however it may chance to include a config file in the future."""
    return os.environ.get(ENV_ARCHIVE_URI, DEFAULT_ARCHIVE_URI)


# region Globals


def get_historian(create=True) -> Optional[historians.Historian]:
    """Get the currently set global historian.  If one doesn't exist and create is True then this
    call will attempt to create a new default historian using connect()"""
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    if CURRENT_HISTORIAN is None and create:
        # Try creating a new one
        connect(use_globally=True)

    return CURRENT_HISTORIAN


def set_historian(new_historian: Optional[historians.Historian], apply_plugins=True):
    """Set the current global historian.  Optionally load all plugins.
    To reset the historian pass None.
    """
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    if new_historian is not None and apply_plugins:
        new_historian.register_types(plugins.get_types())
    CURRENT_HISTORIAN = new_historian


def load(*obj_ids_or_refs):
    """Load one or more objects using the current global historian"""
    return get_historian().load(*obj_ids_or_refs)


def save(*objs):
    """Save one or more objects using the current global historian"""
    return get_historian().save(*objs)


# endregion
