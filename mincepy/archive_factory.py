import pymongo
import pymongo.uri_parser

from . import historian
from . import mongo

__all__ = 'create_archive', 'create_historian'


def create_archive(uri: str):
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


def create_historian(archive_uri: str):
    """Convenience function to create a standard historian directly from an archive URI"""
    return historian.Historian(create_archive(archive_uri))
