import tempfile
import typing
from typing import Optional, Sequence
import uuid

from bidict import bidict
import bson
import gridfs
import pymongo
import pymongo.database
import pymongo.errors

from . import archive
from .archive import BaseArchive, DataRecord
from . import builtins
from . import depositors
from . import exceptions
from . import helpers

__all__ = 'MongoArchive', 'GridFsFile'

OBJ_ID = archive.OBJ_ID
TYPE_ID = archive.TYPE_ID
CREATION_TIME = 'ctime'
CREATED_IN = archive.CREATED_BY
COPIED_FROM = archive.COPIED_FROM
VERSION = 'ver'
STATE = 'state'
SNAPSHOT_HASH = 'hash'
SNAPSHOT_TIME = 'stime'


class ObjectIdHelper(helpers.TypeHelper):
    TYPE = bson.ObjectId
    TYPE_ID = uuid.UUID('bdde0765-36d2-4f06-bb8b-536a429f32ab')

    def yield_hashables(self, obj, hasher):
        yield from hasher.yield_hashables(obj.binary)

    def eq(self, one, other) -> bool:  # pylint: disable=invalid-name
        return one.__eq__(other)

    def save_instance_state(self, obj, _depositor):
        return obj

    def load_instance_state(self, obj, saved_state, _depositor):
        return obj.__init__(saved_state)


class MongoArchive(BaseArchive[bson.ObjectId]):
    ID_TYPE = bson.ObjectId

    DATA_COLLECTION = 'data'
    META_COLLECTION = 'meta'

    # Here we map the data record property names onto ones in our entry format.
    # If a record property doesn't appear here it means the name says the same
    KEY_MAP = bidict({
        archive.OBJ_ID: OBJ_ID,
        archive.TYPE_ID: TYPE_ID,
        archive.CREATION_TIME: CREATION_TIME,
        archive.CREATED_BY: CREATED_IN,
        archive.COPIED_FROM: COPIED_FROM,
        archive.VERSION: VERSION,
        archive.STATE: STATE,
        archive.SNAPSHOT_HASH: SNAPSHOT_HASH,
        archive.SNAPSHOT_TIME: SNAPSHOT_TIME
    })

    META = 'meta'

    @classmethod
    def get_types(cls) -> Sequence:
        return ObjectIdHelper(), GridFsFile

    def __init__(self, database: pymongo.database.Database):
        self._data_collection = database[self.DATA_COLLECTION]
        self._meta_collection = database[self.META_COLLECTION]
        self._file_bucket = gridfs.GridFSBucket(database)
        self._create_indices()

    def _create_indices(self):
        # Make sure that no two entries can share the same object id and version
        self._data_collection.create_index([(self.KEY_MAP[archive.OBJ_ID], pymongo.ASCENDING),
                                            (self.KEY_MAP[archive.VERSION], pymongo.ASCENDING)],
                                           unique=True)

    def create_archive_id(self):  # pylint: disable=no-self-use
        return bson.ObjectId()

    def create_file(self, filename: str = None, encoding: str = None):
        return GridFsFile(self._file_bucket, filename, encoding)

    def get_gridfs_bucket(self) -> gridfs.GridFSBucket:
        return self._file_bucket

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

    def load(self, reference: archive.Ref) -> DataRecord:
        if not isinstance(reference, archive.Ref):
            raise TypeError(reference)

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
        return self._meta_collection.find_one({'_id': obj_id}, projection={'_id': False})

    def set_meta(self, obj_id, meta):
        found = self._meta_collection.replace_one({'_id': obj_id}, meta, upsert=True)
        if not found:
            raise exceptions.NotFound("No record with snapshot id '{}' found".format(obj_id))

    def update_meta(self, obj_id, meta):
        self._meta_collection.update_one({'_id': obj_id}, {'$set': meta}, upsert=True)

    # pylint: disable=too-many-arguments
    def find(self,
             obj_id: Optional[bson.ObjectId] = None,
             type_id=None,
             _created_by=None,
             _copied_from=None,
             version=-1,
             state=None,
             snapshot_hash=None,
             meta=None,
             limit=0,
             _sort=None):
        mfilter = {}
        if obj_id is not None:
            mfilter['obj_id'] = obj_id
        if type_id is not None:
            mfilter['type_id'] = type_id
        if state is not None:
            # If we are given a dict then expand as nested search criteria, e.g. {'state.colour': 'red'}
            if isinstance(state, dict):
                mfilter.update({"{}.{}".format(STATE, key): item for key, item in state.items()})
            else:
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
            # Finally select those from our collection that have a 'max_version' array entry
            pipeline.append({"$match": {"max_version": {"$ne": []}}},)

        if meta:
            pipeline.append({
                '$lookup': {
                    'from': self._meta_collection.name,
                    'localField': OBJ_ID,
                    'foreignField': '_id',
                    'as': '_meta'
                }
            })
            # _meta should only contain at most one entry per document i.e. the metadata for
            # that object.  So check that for the search criteria
            pipeline.append({'$match': {'_meta.0.{}'.format(key): value for key, value in meta.items()}})

        if limit:
            pipeline.append({'$limit': limit})

        results = self._data_collection.aggregate(pipeline)
        for result in results:
            yield self._to_record(result)

    def _to_record(self, entry) -> archive.DataRecord:
        """Convert a MongoDB data collection entry to a DataRecord"""
        record_dict = DataRecord.defaults()

        # Invert our mapping of keys back to the data record property names and update over any defaults
        record_dict.update({recordkey: entry[dbkey] for recordkey, dbkey in self.KEY_MAP.items() if dbkey in entry})
        decoded_state = self._decode_state(record_dict[archive.STATE])
        record_dict[archive.STATE] = decoded_state

        return DataRecord(**record_dict)

    def _to_entry(self, record: DataRecord) -> dict:
        """Convert a DataRecord to a MongoDB data collection entry"""
        defaults = DataRecord.defaults()
        entry = {}
        for key, item in record._asdict().items():
            if key == archive.STATE:
                entry[self.KEY_MAP[key]] = self._encode_state(item)
            else:
                # Exclude entries that have the default value
                if not (key in defaults and defaults[key] == item):
                    entry[self.KEY_MAP[key]] = item

        return entry

    def _encode_state(self, entry):
        if isinstance(entry, list):
            return [self._encode_state(item) for item in entry]

        if isinstance(entry, dict):
            return {key: self._encode_state(item) for key, item in entry.items()}

        return entry

    def _decode_state(self, entry):
        if isinstance(entry, dict):
            return {key: self._decode_state(item) for key, item in entry.items()}

        if isinstance(entry, list):
            return [self._decode_state(item) for item in entry]

        return entry


class GridFsFile(builtins.BaseFile):
    TYPE_ID = uuid.UUID('3bf3c24e-f6c8-4f70-956f-bdecd7aed091')
    ATTRS = '_persistent_id', '_file_id'

    def __init__(self, file_bucket: gridfs.GridFSBucket, filename: str = None, encoding: str = None):
        super().__init__(filename, encoding)
        self._file_store = file_bucket
        self._file_id = None
        self._persistent_id = bson.ObjectId()
        self._buffer_file = self._create_buffer_file()

    def open(self, mode='r', **kwargs):
        self._ensure_buffer()
        if 'b' not in mode:
            kwargs.setdefault('encoding', self.encoding)
        return open(self._buffer_file, mode, **kwargs)

    def save_instance_state(self, depositor: depositors.Depositor):
        filename = self.filename or ""
        with open(self._buffer_file, 'rb') as fstream:
            self._file_id = self._file_store.upload_from_stream(filename, fstream)

        return super().save_instance_state(depositor)

    def load_instance_state(self, saved_state, depositor):
        super().load_instance_state(saved_state, depositor)
        self._file_store = depositor.get_archive().get_gridfs_bucket()  # type: gridfs.GridFSBucket
        # Don't copy the file over now, do it lazily when the file is first opened
        self._buffer_file = None

    def _ensure_buffer(self):
        if self._buffer_file is None:
            if self._file_id is not None:
                self._update_buffer()
            else:
                self._create_buffer_file()

    def _update_buffer(self):
        self._buffer_file = self._create_buffer_file()
        with open(self._buffer_file, 'wb') as fstream:
            self._file_store.download_to_stream(self._file_id, fstream)

    def _create_buffer_file(self):
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        tmp_path = tmp_file.name
        tmp_file.close()
        return tmp_path
