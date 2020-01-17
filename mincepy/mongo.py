import typing

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
            self._data_collection.find(
                {
                    self.KEY_MAP[archive.OBJ_ID]: reference.obj_id,
                    self.KEY_MAP[archive.VERSION]: reference.version,
                },
                projection={'_id': False}))
        if not results:
            raise exceptions.NotFound("Snapshot id '{}' not found".format(reference))
        return self._to_record(results[0])

    def get_snapshot_refs(self, obj_id):
        results = self._data_collection.find({OBJ_ID: obj_id},
                                             projection={VERSION: 1},
                                             sort=[(VERSION, pymongo.ASCENDING)])
        if not results:
            return []
        return [archive.Ref(obj_id, result[VERSION]) for result in results]

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

    def find(self, obj_type_id=None, snapshot_hash=None, criteria=None, limit=0, sort=None):
        mfilter = {}
        if obj_type_id is not None:
            mfilter['type_id'] = obj_type_id
        if criteria is not None:
            mfilter['obj'] = criteria
        if snapshot_hash is not None:
            mfilter[self.KEY_MAP[archive.SNAPSHOT_HASH]] = snapshot_hash

        cursor = self._data_collection.find(filter=mfilter, limit=limit, sort=sort)
        return [self._to_record(result) for result in cursor]

    def _to_record(self, entry) -> archive.DataRecord:
        """Convert a MongoDB data collection entry to a DataRecord"""
        record_dict = dict(DataRecord.DEFAULTS)

        # Invert our mapping of keys back to the data record property names and update over any defaults
        record_dict.update({recordkey: entry[dbkey] for recordkey, dbkey in self.KEY_MAP.items() if dbkey in entry})

        return DataRecord(**record_dict)

    def _to_entry(self, record: DataRecord) -> dict:
        """Convert a DataRecord to a MongoDB data collection entry"""
        entry = {}
        for key, item in record._asdict().items():
            # Exclude entries that have the default value
            if not (key in DataRecord.DEFAULTS and DataRecord.DEFAULTS[key] == item):
                entry[self.KEY_MAP[key]] = item

        return entry
