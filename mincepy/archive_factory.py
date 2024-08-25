import logging
import os
from typing import TYPE_CHECKING, Optional

import deprecation

from . import mongo, version

if TYPE_CHECKING:
    import mincepy

__all__ = (
    "create_archive",
    "DEFAULT_ARCHIVE_URI",
    "ENV_ARCHIVE_URI",
    "archive_uri",
    "default_archive_uri",
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_ARCHIVE_URI = "mongodb://localhost/mincepy"
ENV_ARCHIVE_URI = "MINCEPY_ARCHIVE"


@deprecation.deprecated(
    deprecated_in="0.15.3",
    removed_in="0.16.0",
    current_version=version.__version__,
    details="Use default_archive_uri() instead",
)
def archive_uri() -> Optional[str]:
    """Returns the default archive URI.  This is currently being taken from the environmental
    MINCEPY_ARCHIVE, however it may chance to include a config file in the future."""
    return os.environ.get(ENV_ARCHIVE_URI, DEFAULT_ARCHIVE_URI)


def default_archive_uri() -> Optional[str]:
    """Returns the default archive URI.  This is currently being taken from the environmental
    MINCEPY_ARCHIVE, however it may chance to include a config file in the future."""
    return os.environ.get(ENV_ARCHIVE_URI, DEFAULT_ARCHIVE_URI)


def create_archive(uri: str, connect_timeout=30000) -> "mincepy.Archive":
    """Create an archive type based on a URI string

    :param uri: the specification of where to connect to
    :param connect_timeout: a connection timeout (in milliseconds)
    """
    archive = mongo.connect(uri, timeout=connect_timeout)

    _LOGGER.info("Connected to archive with uri: %s", uri)
    return archive
