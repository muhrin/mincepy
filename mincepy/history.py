"""
This module exposes some global functionality for connecting to and interacting with the current
historian
"""

from typing import TYPE_CHECKING, Optional

import deprecation

from . import archive_factory, helpers, historians, plugins, version

if TYPE_CHECKING:
    import mincepy

__all__ = (
    "connect",
    "create_historian",
    "get_historian",
    "set_historian",
    "load",
    "save",
    "find",
    "delete",
    "db",
)

CURRENT_HISTORIAN = None


@deprecation.deprecated(
    deprecated_in="0.15.3",
    removed_in="0.16.0",
    current_version=version.__version__,
    details="Use connect(set_global=False) instead",
)
def create_default_historian():
    """Create a default historian using the current `archive_uri()`"""
    uri = archive_factory.default_archive_uri()
    if uri:
        return create_historian(uri)

    return None


def connect(uri: str = "", use_globally=False, timeout=30000) -> "mincepy.Historian":
    """Connect to an archive and return a corresponding historian

    :param uri: the URI of the archive to connect to
    :param use_globally: if True sets the newly create historian as the current global historian
    :param timeout: a connection timeout (in milliseconds)
    """
    uri = uri or archive_factory.default_archive_uri()
    hist = create_historian(uri, apply_plugins=True, connect_timeout=timeout)
    if use_globally:
        set_historian(hist, apply_plugins=False)
    return hist


def create_historian(
    archive_uri: str, apply_plugins=True, connect_timeout=30000
) -> "mincepy.Historian":
    """Convenience function to create a standard historian directly from an archive URI

    :param archive_uri: the specification of where to connect to
    :param apply_plugins: register the plugin types with the new historian
    :param connect_timeout: a connection timeout (in milliseconds)
    """
    historian = historians.Historian(
        archive_factory.create_archive(archive_uri, connect_timeout=connect_timeout)
    )
    if apply_plugins:
        historian.register_types(plugins.get_types())

    return historian


# region Globals


def get_historian(create=True) -> Optional["mincepy.Historian"]:
    """Get the currently set global historian.  If one doesn't exist and create is True then this
    call will attempt to create a new default historian using connect()"""
    global CURRENT_HISTORIAN  # pylint: disable=global-statement, global-variable-not-assigned
    if CURRENT_HISTORIAN is None and create:
        # Try creating a new one, use globally otherwise a new one will be created each time which
        # is unlikely to be what users want
        connect(use_globally=True)

    return CURRENT_HISTORIAN


def set_historian(new_historian: Optional["mincepy.Historian"], apply_plugins=True):
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
    """Save one or more objects.  See :py:meth:`mincepy.Historian.save`"""
    return get_historian().save(*objs)


def find(*args, **kwargs):
    """Find objects.  See :py:meth:`mincepy.Historian.find`"""
    yield from get_historian().find(*args, **kwargs)


def delete(*obj_or_identifier):
    """Delete an object.  See :py:meth:`mincepy.Historian.delete`"""
    return get_historian().delete(*obj_or_identifier)


def db(type_id_or_type) -> helpers.TypeHelper:  # pylint: disable=invalid-name
    """Get the database type helper for a type.  See :py:meth:`mincepy.Historian.get_helper`"""
    return get_historian().get_helper(type_id_or_type)


# endregion
