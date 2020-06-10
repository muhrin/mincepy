import pymongo.database

from mincepy import q
from . import migrate


class Initial(migrate.Migration):
    NAME = 'initial-setup'
    VERSION = 0

    def upgrade(self, database: pymongo.database.Database):
        collections = database.list_collection_names()
        if 'data' not in collections:
            database.create_collection('data')
        if 'meta' not in collections:
            database.create_collection('meta')
        super().upgrade(database)


class CollectionsSplit(migrate.Migration):
    """
    This migration makes the following changes wrt the initial database layout:

    Data collection
    ---------------

    `_id` is now the object id
    All historical and deleted records are removed.

    History collection
    ------------------

    Contains what data used to contain (including currently live objects).
    The _id field is the sref string i.e. <obj_id>#<version>

    References collection
    ---------------------

    Has been changed because of the format change of history ids.  This can
    simply be dropped as it will be lazily re-created if needed.

    Meta collection
    ---------------

    Unchanged.

    """
    NAME = 'split-data-collection'
    VERSION = 1
    PREVIOUS = Initial

    def upgrade(self, database: pymongo.database.Database):
        history_collection = 'history'
        data_collection = 'data'

        old_data = database[data_collection]
        history = database[history_collection]

        # Transform the entries to the new history format
        for entry in old_data.find():
            obj_id = entry['_id']['oid']
            version = entry['_id']['v']
            entry['_id'] = "{}#{}".format(obj_id, version)
            entry['obj_id'] = obj_id
            entry['ver'] = version
            history.insert_one(entry)

        # Ok, now rename, copy over what we need and drop
        old_data.rename('old_data')

        pipeline = self.pipeline_latest_version(history_collection)
        pipeline.append({'$match': {'state': {'$ne': '!!deleted'}}})

        new_data = database[data_collection]
        for entry in history.aggregate(pipeline):
            entry['_id'] = entry['obj_id']
            new_data.insert_one(entry)

        # Finally drop
        database.drop_collection('old_data')

        # This will be lazily re-created
        database.drop_collection('references')

        super().upgrade(database)

    @staticmethod
    def pipeline_latest_version(collection: str) -> list:
        """Returns a pipeline that will take the incoming data record documents and for each one
        find the latest version."""
        pipeline = []
        pipeline.extend([
            # Group by object id the maximum version
            {
                '$group': {
                    '_id': '$obj_id',
                    'max_ver': {
                        '$max': '$ver'
                    }
                }
            },
            # Then do a lookup against the same collection to get the records
            {
                '$lookup': {
                    'from': collection,
                    'let': {
                        'obj_id': '$_id',
                        'max_ver': '$max_ver'
                    },
                    'pipeline': [{
                        '$match': {
                            '$expr':
                                q.and_(
                                    q.eq_('$obj_id', '$$obj_id'),  # Match object id and version
                                    q.eq_('$ver', '$$max_ver')),
                        }
                    }],
                    'as': 'latest'
                }
            },
            # Now unwind and promote the 'latest' field
            {
                '$unwind': {
                    'path': '$latest'
                }
            },
            {
                '$replaceRoot': {
                    'newRoot': '$latest'
                }
            },
        ])

        return pipeline


class DropReferencesCache(migrate.Migration):
    """Here we reset the references cache (if present) as some of the code has changed"""

    NAME = 'drop-references-cache'
    VERSION = 2
    PREVIOUS = CollectionsSplit

    def upgrade(self, database: pymongo.database.Database):  # pylint: disable=no-self-use
        # This is an old one that was created by accident
        database.drop_collection("data.references")
        # And this will drop the current one
        database.drop_collection("references")


LATEST = CollectionsSplit
