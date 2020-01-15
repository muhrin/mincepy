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


class MongoArchive(BaseArchive[bson.ObjectId]):
    ID_TYPE = bson.ObjectId

    DATA_COLLECTION = 'data'

    # Here we map the data record property names onto ones in our entry format.
    # If a record property doesn't appear here it means the name says the same
    KEY_MAP = bidict({
        archive.OBJ_ID: archive.OBJ_ID,
        archive.TYPE_ID: archive.TYPE_ID,
        archive.CREATED_IN: archive.CREATED_IN,
        archive.SNAPSHOT_ID: '_id',
        archive.ANCESTOR_ID: archive.ANCESTOR_ID,
        archive.STATE: archive.STATE,
        archive.SNAPSHOT_HASH: archive.SNAPSHOT_HASH
    })

    META = 'meta'

    def __init__(self, database: pymongo.database.Database):
        self._data_collection = database[self.DATA_COLLECTION]
        self._create_indices()

    def _create_indices(self):
        # Make sure that no two entries can share the same ancestor
        self._data_collection.create_index([(self.KEY_MAP[archive.OBJ_ID], pymongo.ASCENDING),
                                            (self.KEY_MAP[archive.ANCESTOR_ID], pymongo.ASCENDING)],
                                           unique=True)

    def create_archive_id(self):  # pylint: disable=no-self-use
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
            spec = {'$or': [{self.KEY_MAP[archive.SNAPSHOT_ID]: ancestor_id} for ancestor_id in ancestors]}
            results = list(self._data_collection.find(spec))

            for entry in results:
                metadatas[entry[self.KEY_MAP[archive.SNAPSHOT_ID]]] = entry[self.META]
            ancestors -= {entry[self.KEY_MAP[archive.SNAPSHOT_ID]] for entry in results}

        if ancestors:
            raise ValueError(
                "Records were passed that refer to ancestors not present in the passed list nor in the archive")

        # Generate the entries for our collection collecting the metadata that we gathered
        entries = [self._to_entry(record, metadatas.get(record.ancestor_id, None)) for record in records]
        try:
            self._data_collection.insert_many(entries)
        except pymongo.errors.BulkWriteError as exc:
            write_errors = exc.details['writeErrors']
            if write_errors:
                raise exceptions.ModificationError("You're trying to rewrite history, that's not allowed!")
            raise  # Otherwise just raise what we got

    def load(self, snapshot_id) -> DataRecord:
        results = list(self._data_collection.find({self.KEY_MAP[archive.SNAPSHOT_ID]: snapshot_id}))
        if not results:
            raise exceptions.NotFound("Snapshot id '{}' not found".format(snapshot_id))
        return self._to_record(results[0])

    def get_snapshot_ids(self, obj_id):
        # Start with the first snapshot and do a graph traversal from there
        match_initial_document = {
            '$match': {
                self.KEY_MAP[archive.OBJ_ID]: obj_id,
                self.KEY_MAP[archive.ANCESTOR_ID]: None
            }
        }
        find_ancestors = {
            "$graphLookup": {
                "from": self._data_collection.name,
                "startWith": "${}".format(self.KEY_MAP[archive.SNAPSHOT_ID]),
                "connectFromField": self.KEY_MAP[archive.SNAPSHOT_ID],
                "connectToField": self.KEY_MAP[archive.ANCESTOR_ID],
                "as": "descendents",
                "depthField": "depth",
                "restrictSearchWithMatch": {
                    self.KEY_MAP[archive.OBJ_ID]: obj_id
                }
            }
        }
        results = tuple(self._data_collection.aggregate([match_initial_document, find_ancestors]))
        if not results:
            return []

        entry = results[0]

        # Preallocate an array
        snapshot_ids = [None] * (len(entry['descendents']) + 1)
        snapshot_ids[0] = entry[self.KEY_MAP[archive.SNAPSHOT_ID]]
        for descendent in entry['descendents']:
            snapshot_ids[descendent['depth'] + 1] = descendent[self.KEY_MAP[archive.SNAPSHOT_ID]]

        return snapshot_ids

    def get_meta(self, snapshot_id):
        result = self._data_collection.find_one(snapshot_id, projection={self.META: 1})
        if not result:
            raise exceptions.NotFound("No record with snapshot id '{}' found".format(snapshot_id))

        return result['meta']

    def set_meta(self, snapshot_id, meta):
        found = self._data_collection.find_one_and_update({self.KEY_MAP[archive.SNAPSHOT_ID]: snapshot_id},
                                                          {'$set': {
                                                              'meta': meta
                                                          }})
        if not found:
            raise exceptions.NotFound("No record with snapshot id '{}' found".format(snapshot_id))

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

    def _to_record(self, entry):
        record_dict = dict(DataRecord.DEFAULTS)

        # Invert our mapping of keys back to the data record property names and update over any defaults
        record_dict.update(
            {self.KEY_MAP.inverse.get(key, key): value for key, value in entry.items() if key != self.META})

        return DataRecord(**record_dict)

    def _to_entry(self, record: DataRecord, meta=None):
        entry = {}
        for key, item in record._asdict().items():
            # Exclude defaults
            if not (key in DataRecord.DEFAULTS and DataRecord.DEFAULTS[key] == item):
                entry[self.KEY_MAP[key]] = item

        # Add the metadata
        entry[self.META] = meta
        return entry
