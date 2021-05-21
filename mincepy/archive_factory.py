# -*- coding: utf-8 -*-
import logging

from . import historians
from . import plugins

__all__ = 'create_archive', 'create_historian'

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def create_archive(uri: str, connect_timeout=30000):
    """Create an archive type based on a uri string

    :param uri: the specification of where to connect to
    :param connect_timeout: a connection timeout (in milliseconds)
    """
    if uri.startswith('mongodb'):
        from . import mongo  # pylint: disable=import-outside-toplevel
        archive = mongo.connect(uri, timeout=connect_timeout)
    else:
        raise ValueError(f'Unknown archive string: {uri}')

    logger.info('Connected to archive with uri: %s', uri)
    return archive


def create_historian(archive_uri: str,
                     apply_plugins=True,
                     connect_timeout=30000) -> historians.Historian:
    """Convenience function to create a standard historian directly from an archive URI

    :param archive_uri: the specification of where to connect to
    :param apply_plugins: register the plugin types with the new historian
    :param connect_timeout: a connection timeout (in milliseconds)
    """
    historian = historians.Historian(create_archive(archive_uri, connect_timeout=connect_timeout))
    if apply_plugins:
        historian.register_types(plugins.get_types())

    return historian
