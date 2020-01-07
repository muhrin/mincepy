import typing

import bson
import pymongo
import pymongo.database

from .archive import BaseArchive, DataRecord
from . import exceptions

__all__ = ('MongoArchive',)


class MongoArchive(BaseArchive):
    DATA_COLLECTION = 'data'

    OBJ_ID = 'obj_id'
    TYPE_ID = 'type_id'
    SNAPSHOT_ID = '_id'
    ANCESTOR_ID = 'ancestor_id'
    VERSION = 'version'
    STATE = 'state'
    SNAPSHOT_HASH = 'snapshot_hash'
    CREATED_IN = 'created_in'
    META = 'meta'

    def __init__(self, db: pymongo.database.Database):
        super(MongoArchive, self).__init__()
        self._data_collection = db[self.DATA_COLLECTION]
        self._create_indices()

    def _create_indices(self):
        # Make sure that no two entries can share the same ancestor
        self._data_collection.create_index(
            [(self.OBJ_ID, pymongo.ASCENDING),
             (self.ANCESTOR_ID, pymongo.ASCENDING)], unique=True)

    def create_archive_id(self):
        return bson.ObjectId()

    def save(self, record: DataRecord):
        self.save_many([record])
        return record

    def save_many(self, records: typing.List[DataRecord]):
        # Collect all the ones to where previous versions have to be accounted for
        ancestors = {record.ancestor_id for record in records if record.ancestor_id}

        # Now check that the ancestors are accounted for, either in the list passed or in the database
        ancestors -= {record.snapshot_id for record in records}

        metadatas = {}
        if ancestors:
            # Have to go look in the collection, collect the metadata while we're at it
            spec = {'$or': [{self.SNAPSHOT_ID: ancestor_id} for ancestor_id in ancestors]}
            results = list(self._data_collection.find(spec, projection={self.OBJ_ID: 1, self.META: 1}))

            for entry in results:
                metadatas[entry[self.SNAPSHOT_ID]] = entry[self.META]
            ancestors -= {entry[self.SNAPSHOT_ID] for entry in results}

        if ancestors:
            raise ValueError(
                "Records were passed that refer to ancestors not present in the passed list nor in the archive")

        # Generate the entries for our collection collecting the metadata that we gathered
        entries = [self._to_entry(record, metadatas.get(record.ancestor_id, None)) for record in records]
        self._data_collection.insert_many(entries)

    def _get_organised(self, records: typing.List[DataRecord]):
        # Organise the records into a dictionary where the key is the object id and the
        # value is another dictionary where the key is the version number
        organised = {}
        for record in records:
            organised.setdefault(record.obj_id, {})[record.version] = record
        return organised

    def load(self, snapshot_id) -> DataRecord:
        results = list(self._data_collection.find({self.SNAPSHOT_ID: snapshot_id}))
        if not results:
            raise exceptions.NotFound("Snapshot id '{}' not found".format(snapshot_id))
        return self._to_record(results[0])

    def get_snapshot_ids(self, obj_id):
        # Start with the first snapshot and do a graph traversal from there
        match_initial_document = {'$match': {self.OBJ_ID: obj_id, self.ANCESTOR_ID: None}}
        find_ancestors = {
            "$graphLookup": {
                "from": self._data_collection.name,
                "startWith": "${}".format(self.SNAPSHOT_ID),
                "connectFromField": self.SNAPSHOT_ID,
                "connectToField": self.ANCESTOR_ID,
                "as": "descendents",
                "depthField": "depth",
                "restrictSearchWithMatch": {self.OBJ_ID: obj_id}
            }
        }
        # unwind_descendents = {'$unwind': '$descendents'}
        # depth_sort = {'$sort': {'descendents.depth': pymongo.ASCENDING}}
        results = tuple(self._data_collection.aggregate([
            match_initial_document,
            find_ancestors,
            # unwind_descendents,
            # depth_sort,
        ]))
        if not results:
            return []

        entry = results[0]

        # Preallocate an array
        snapshot_ids = [None] * (len(entry['descendents']) + 1)
        snapshot_ids[0] = entry[self.SNAPSHOT_ID]
        for descendent in entry['descendents']:
            snapshot_ids[descendent['depth'] + 1] = descendent[self.SNAPSHOT_ID]

        return snapshot_ids

    def get_meta(self, snapshot_id):
        result = self._data_collection.find_one(snapshot_id, projection={self.META: 1})
        if not result:
            raise exceptions.NotFound("No record with snapshot id '{}' found".format(snapshot_id))

        return result['meta']

    def set_meta(self, snapshot_id, meta):
        found = self._data_collection.find_one_and_update({self.SNAPSHOT_ID: snapshot_id}, {'$set': {'meta': meta}})
        if not found:
            raise exceptions.NotFound("No record with snapshot id '{}' found".format(snapshot_id))

    def find(self, obj_type_id=None, hash=None, filter=None, limit=0, sort=None):
        mfilter = {}
        if obj_type_id is not None:
            mfilter['type_id'] = obj_type_id
        if filter is not None:
            mfilter['obj'] = filter
        if hash is not None:
            mfilter['hash'] = hash

        cursor = self._data_collection.find(filter=mfilter, limit=limit, sort=sort)
        return [self._to_record(result) for result in cursor]

    def _to_record(self, entry):
        return DataRecord(
            entry[self.OBJ_ID],
            entry[self.TYPE_ID],
            entry[self.CREATED_IN],

            entry[self.SNAPSHOT_ID],
            entry[self.ANCESTOR_ID],
            entry[self.STATE],
            entry[self.SNAPSHOT_HASH],
        )

    def _to_entry(self, record: DataRecord, meta=None):
        return {
            self.OBJ_ID: record.obj_id,
            self.TYPE_ID: record.type_id,
            self.CREATED_IN: record.created_in,

            self.SNAPSHOT_ID: record.snapshot_id,
            self.ANCESTOR_ID: record.ancestor_id,
            self.STATE: record.state,
            self.SNAPSHOT_HASH: record.snapshot_hash,
            self.META: meta
        }
