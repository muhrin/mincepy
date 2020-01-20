import typing
from typing import Optional

from bidict import bidict
import bson
import pymongo
import pymongo.database
import pymongo.errors

from . import archive
from .archive import BaseArchive, DataRecord
from . import exceptions

__all__ = ('MongoArchive',)

OBJ_ID = archive.OBJ_ID
TYPE_ID = archive.TYPE_ID
CREATED_IN = archive.CREATED_IN
COPIED_FROM = archive.COPIED_FROM
VERSION = 'ver'
STATE = 'state'
SNAPSHOT_HASH = 'hash'


class MongoArchive(BaseArchive[bson.ObjectId]):
    ID_TYPE = bson.ObjectId

    DATA_COLLECTION = 'data'
    META_COLLECTION = 'meta'

    # Here we map the data record property names onto ones in our entry format.
    # If a record property doesn't appear here it means the name says the same
    KEY_MAP = bidict({
        archive.OBJ_ID: OBJ_ID,
        archive.TYPE_ID: TYPE_ID,
        archive.CREATED_IN: CREATED_IN,
        archive.COPIED_FROM: COPIED_FROM,
        archive.VERSION: VERSION,
        archive.STATE: STATE,
        archive.SNAPSHOT_HASH: SNAPSHOT_HASH
    })

    META = 'meta'

    def __init__(self, database: pymongo.database.Database):
        self._data_collection = database[self.DATA_COLLECTION]
        self._meta_collection = database[self.META_COLLECTION]
        self._create_indices()

    def _create_indices(self):
        # Make sure that no two entries can share the same object id and version
        self._data_collection.create_index([(self.KEY_MAP[archive.OBJ_ID], pymongo.ASCENDING),
                                            (self.KEY_MAP[archive.VERSION], pymongo.ASCENDING)],
                                           unique=True)

    def create_archive_id(self):  # pylint: disable=no-self-use
        return bson.ObjectId()

    def save(self, record: DataRecord):
        self.save_many([record])
        return record

    def save_many(self, records: typing.List[DataRecord]):
        # Generate the entries for our collection collecting the metadata that we gathered
        entries = [self._to_entry(record) for record in records]
        try:
            self._data_collection.insert_many(entries)
        except pymongo.errors.BulkWriteError as exc:
            write_errors = exc.details['writeErrors']
            if write_errors:
                raise exceptions.ModificationError("You're trying to rewrite history, that's not allowed!")
            raise  # Otherwise just raise what we got

    def load(self, reference) -> DataRecord:
        results = list(
            self._data_collection.find({
                self.KEY_MAP[archive.OBJ_ID]: reference.obj_id,
                self.KEY_MAP[archive.VERSION]: reference.version,
            }))
        if not results:
            raise exceptions.NotFound("Snapshot id '{}' not found".format(reference))
        return self._to_record(results[0])

    def get_snapshot_refs(self, obj_id):
        results = self._data_collection.find({OBJ_ID: obj_id}, sort=[(VERSION, pymongo.ASCENDING)])
        if not results:
            return []
        try:
            return [archive.Ref(obj_id, result[VERSION]) for result in results]
        except KeyError as err:
            print(err)
            raise

    def get_meta(self, obj_id):
        assert isinstance(obj_id, bson.ObjectId), "Must pass an ObjectId"

        result = self._meta_collection.find_one({'_id': obj_id}, projection={'_id': False})
        if not result:
            raise exceptions.NotFound("No record with object id '{}' found".format(obj_id))

        return result

    def set_meta(self, obj_id, meta):
        found = self._meta_collection.replace_one({'_id': obj_id}, meta, upsert=True)
        if not found:
            raise exceptions.NotFound("No record with snapshot id '{}' found".format(obj_id))

    def find(self,
             obj_id: Optional[bson.ObjectId] = None,
             type_id=None,
             created_in=None,
             copied_from=None,
             version=-1,
             state=None,
             snapshot_hash=None,
             limit=0,
             sort=None):
        mfilter = {}
        if obj_id is not None:
            mfilter['obj_id'] = obj_id
        if type_id is not None:
            mfilter['type_id'] = type_id
        if state is not None:
            mfilter[STATE] = state
        if snapshot_hash is not None:
            mfilter[self.KEY_MAP[archive.SNAPSHOT_HASH]] = snapshot_hash
        if version is not None and version != -1:
            mfilter[VERSION] = version

        pipeline = [{'$match': mfilter}]

        if version == -1:
            # Join with a collection that is grouped to get the maximum version for each object ID
            # then only take the the matching documents
            pipeline.append({
                '$lookup': {
                    'from': self.DATA_COLLECTION,
                    'let': {
                        'obj_id': '$obj_id',
                        'ver': '$ver'
                    },
                    'pipeline': [
                        # Get the maximum version
                        {
                            '$group': {
                                '_id': '$obj_id',
                                'ver': {
                                    '$max': '$ver'
                                }
                            }
                        },
                        # Then match these with the obj id and version in our collection
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': ['$_id', '$$obj_id']
                                        },
                                        {
                                            '$eq': ['$ver', '$$ver']
                                        },
                                    ]
                                }
                            }
                        }
                    ],
                    'as': "max_version"
                }
            })
            # Finally sepect those from our collection that have a 'max_version' array entry
            pipeline.append({"$match": {"max_version": {"$ne": []}}},)

        if limit:
            pipeline.append({'$limit': limit})

        results = self._data_collection.aggregate(pipeline)
        return [self._to_record(result) for result in results]

    def _to_record(self, entry) -> archive.DataRecord:
        """Convert a MongoDB data collection entry to a DataRecord"""
        record_dict = DataRecord.defaults()

        # Invert our mapping of keys back to the data record property names and update over any defaults
        record_dict.update({recordkey: entry[dbkey] for recordkey, dbkey in self.KEY_MAP.items() if dbkey in entry})

        return DataRecord(**record_dict)

    def _to_entry(self, record: DataRecord) -> dict:
        """Convert a DataRecord to a MongoDB data collection entry"""
        defaults = DataRecord.defaults()
        entry = {}
        for key, item in record._asdict().items():
            # Exclude entries that have the default value
            if not (key in defaults and defaults[key] == item):
                entry[self.KEY_MAP[key]] = item

        return entry
