import pymongo
import pymongo.uri_parser

from . import historians
from . import mongo

__all__ = 'archive', 'historian'

from . import plugins


def archive(uri: str):
    """Create an archive type based on a uri string"""
    if uri.startswith('mongodb'):
        # Format is:
        # mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[database][?options]]
        parsed = pymongo.uri_parser.parse_uri(uri)
        if not parsed.get('database', None):
            raise ValueError("Failed to supply database on MongoDB uri: {}".format(uri))
        client = pymongo.MongoClient(uri)
        database = client[parsed['database']]
        return mongo.MongoArchive(database)

    raise ValueError("Unknown archive string: {}".format(uri))


def historian(archive_uri: str, apply_plugins=True):
    """Convenience function to create a standard historian directly from an archive URI"""
    historian = historians.Historian(archive(archive_uri))
    if apply_plugins:
        historian.register_types(plugins.get_types())

    return historian
