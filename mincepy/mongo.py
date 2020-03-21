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

from . import archives
from . import builtins
from . import depositors
from . import exceptions
from . import helpers
from . import records

__all__ = 'MongoArchive', 'GridFsFile'

OBJ_ID = records.OBJ_ID
TYPE_ID = records.TYPE_ID
CREATION_TIME = 'ctime'
VERSION = 'ver'
STATE = 'state'
STATE_TYPES = 'state_type'
SNAPSHOT_HASH = 'hash'
SNAPSHOT_TIME = 'stime'
EXTRAS = records.EXTRAS


def and_(*conditions) -> dict:
    """Helper that produces mongo query dict for AND of multiple conditions"""
    if len(conditions) > 1:
        return {'$and': list(conditions)}

    return conditions[0]


def eq_(one, other) -> dict:
    """Helper that produces mongo query dict for to items being equal"""
    return {'$eq': [one, other]}


class ObjectIdHelper(helpers.TypeHelper):
    TYPE = bson.ObjectId
    TYPE_ID = uuid.UUID('bdde0765-36d2-4f06-bb8b-536a429f32ab')

    def yield_hashables(self, obj, hasher):
        yield obj.binary

    def eq(self, one, other) -> bool:  # pylint: disable=invalid-name
        return one.__eq__(other)

    def save_instance_state(self, obj, _depositor):
        return obj

    def load_instance_state(self, obj, saved_state, _depositor):
        return obj.__init__(saved_state)


class MongoArchive(archives.BaseArchive[bson.ObjectId]):
    ID_TYPE = bson.ObjectId

    DATA_COLLECTION = 'data'
    META_COLLECTION = 'meta'

    # Here we map the data record property names onto ones in our entry format.
    # If a record property doesn't appear here it means the name says the same
    KEY_MAP = bidict({
        records.OBJ_ID: OBJ_ID,
        records.TYPE_ID: TYPE_ID,
        records.CREATION_TIME: CREATION_TIME,
        records.VERSION: VERSION,
        records.STATE: STATE,
        records.STATE_TYPES: STATE_TYPES,
        records.SNAPSHOT_HASH: SNAPSHOT_HASH,
        records.SNAPSHOT_TIME: SNAPSHOT_TIME,
        records.EXTRAS: EXTRAS,
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
        self._data_collection.create_index([(self.KEY_MAP[records.OBJ_ID], pymongo.ASCENDING),
                                            (self.KEY_MAP[records.VERSION], pymongo.ASCENDING)],
                                           unique=True)

    def create_archive_id(self):  # pylint: disable=no-self-use
        return bson.ObjectId()

    def construct_archive_id(self, value) -> bson.ObjectId:
        if not isinstance(value, str):
            raise TypeError("Cannot construct an ObjectID from a '{}'".format(type(value)))
        try:
            return bson.ObjectId(value)
        except bson.errors.InvalidId as exc:
            raise ValueError(str(exc))

    def create_file(self, filename: str = None, encoding: str = None):
        return GridFsFile(self._file_bucket, filename, encoding)

    def get_gridfs_bucket(self) -> gridfs.GridFSBucket:
        return self._file_bucket

    def save(self, record: records.DataRecord):
        self.save_many([record])
        return record

    def save_many(self, records: typing.List[records.DataRecord]):
        # Generate the entries for our collection collecting the metadata that we gathered
        entries = [self._to_entry(record) for record in records]
        try:
            self._data_collection.insert_many(entries)
        except pymongo.errors.BulkWriteError as exc:
            write_errors = exc.details['writeErrors']
            if write_errors:
                raise exceptions.ModificationError(
                    "You're trying to rewrite history, that's not allowed!")
            raise  # Otherwise just raise what we got

    def load(self, reference: records.Ref) -> records.DataRecord:
        if not isinstance(reference, records.Ref):
            raise TypeError(reference)

        results = list(
            self._data_collection.find({
                self.KEY_MAP[records.OBJ_ID]: reference.obj_id,
                self.KEY_MAP[records.VERSION]: reference.version,
            }))
        if not results:
            raise exceptions.NotFound("Snapshot id '{}' not found".format(reference))
        return self._to_record(results[0])

    def get_snapshot_refs(self, obj_id):
        results = self._data_collection.find({OBJ_ID: obj_id}, sort=[(VERSION, pymongo.ASCENDING)])
        if not results:
            return []

        return [records.Ref(obj_id, result[VERSION]) for result in results]

    # region Meta

    def get_meta(self, obj_id):
        assert isinstance(obj_id, bson.ObjectId), "Must pass an ObjectId"
        return self._meta_collection.find_one({'_id': obj_id},
                                              projection={
                                                  'obj_id': False,
                                                  '_id': False
                                              })

    def set_meta(self, obj_id, meta):
        if meta:
            # Make sure the obj id is in the record
            meta['_id'] = obj_id
            meta['obj_id'] = obj_id
            found = self._meta_collection.replace_one({'_id': obj_id}, meta, upsert=True)
            if not found:
                raise exceptions.NotFound("No record with snapshot id '{}' found".format(obj_id))
        else:
            # Just remove the meta entry outright
            self._meta_collection.delete_one({'_id': obj_id})

    def update_meta(self, obj_id, meta):
        assert meta.get('obj_id',
                        obj_id) == obj_id, "Can't use the 'obj_id' key in metadata, it is reserved"

        meta['obj_id'] = obj_id
        self._meta_collection.update_one({'_id': obj_id}, {'$set': meta}, upsert=True)

    def find_meta(self, filter: dict):
        # Make sure to project away the _id but leave obj_id as there may be multiple and this is
        # what the user is probably looking for
        for result in self._meta_collection.find(filter, projection={'_id': False}):
            yield result

    # endregion

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
             sort=None,
             skip=0):
        pipeline = self._get_pipeline(obj_id=obj_id,
                                      type_id=type_id,
                                      _created_by=_created_by,
                                      _copied_from=_copied_from,
                                      version=version,
                                      state=state,
                                      snapshot_hash=snapshot_hash,
                                      meta=meta)

        if skip:
            pipeline.append({'$skip': skip})

        if limit:
            pipeline.append({'$limit': limit})

        if sort:
            if not isinstance(sort, dict):
                sort_dict = {sort: 1}
            else:
                sort_dict = sort
            sort_dict = self._remap(sort_dict)
            pipeline.append({'$sort': sort_dict})

        results = self._data_collection.aggregate(pipeline)

        for result in results:
            yield self._to_record(result)

    def count(self,
              obj_id: Optional[bson.ObjectId] = None,
              type_id=None,
              _created_by=None,
              _copied_from=None,
              version=-1,
              state=None,
              snapshot_hash=None,
              meta=None,
              limit=0):
        mfilter = {}
        if obj_id is not None:
            mfilter['obj_id'] = obj_id
        if type_id is not None:
            mfilter['type_id'] = type_id
        if state is not None:
            # If we are given a dict then expand as nested search criteria, e.g.
            # {'state.colour': 'red'}
            if isinstance(state, dict):
                mfilter.update({"{}.{}".format(STATE, key): item for key, item in state.items()})
            else:
                mfilter[STATE] = state
        if snapshot_hash is not None:
            mfilter[self.KEY_MAP[records.SNAPSHOT_HASH]] = snapshot_hash
        if version == -1:
            # For counting we don't care which version we get we just want there to be only 1
            # counted per obj_id so select the first
            mfilter[VERSION] = 0
        else:
            mfilter[VERSION] = version

        pipeline = [{'$match': mfilter}]

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
            pipeline.append(
                {'$match': {'_meta.0.{}'.format(key): value for key, value in meta.items()}})

        if limit:
            pipeline.append({'$limit': limit})

        pipeline.append({'$count': "total"})
        result = next(self._data_collection.aggregate(pipeline))

        return result['total']

    def _get_pipeline(self,
                      obj_id: Optional[bson.ObjectId] = None,
                      type_id=None,
                      _created_by=None,
                      _copied_from=None,
                      version=-1,
                      state=None,
                      snapshot_hash=None,
                      meta=None):
        """Get a pipeline that would perform the given search.  Can be used directly in an aggregate
         call"""
        mfilter = {}
        if obj_id is not None:
            mfilter['obj_id'] = obj_id
        if type_id is not None:
            mfilter['type_id'] = type_id
        if state is not None:
            # If we are given a dict then expand as nested search criteria, e.g.
            # {'state.colour': 'red'}
            mfilter.update(flatten_filter(STATE, state))
        if snapshot_hash is not None:
            mfilter[self.KEY_MAP[records.SNAPSHOT_HASH]] = snapshot_hash
        if version is not None and version != -1:
            mfilter[VERSION] = version

        pipeline = [{'$match': mfilter}]

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
            # pipeline.append({'$match': flatten_filter('_meta.0', meta)})
            pipeline.append({'$match': flatten_filter('_meta.0', meta)})

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
                                '$expr': and_(eq_('$_id', '$$obj_id'), eq_('$ver', '$$ver')),
                            }
                        }
                    ],
                    'as': "max_version"
                }
            })
            # Finally select those from our collection that have a 'max_version' array entry
            pipeline.append({"$match": {"max_version": {"$ne": []}}},)

        return pipeline

    def _to_record(self, entry) -> records.DataRecord:
        """Convert a MongoDB data collection entry to a DataRecord"""
        record_dict = records.DataRecord.defaults()

        # Invert our mapping of keys back to the data record property names and update over any
        # defaults
        record_dict.update({
            recordkey: entry[dbkey] for recordkey, dbkey in self.KEY_MAP.items() if dbkey in entry
        })
        decoded_state = self._decode_state(record_dict[records.STATE])
        record_dict[records.STATE] = decoded_state

        return records.DataRecord(**record_dict)

    def _to_entry(self, record: records.DataRecord) -> dict:
        """Convert a DataRecord to a MongoDB data collection entry"""
        defaults = records.DataRecord.defaults()
        entry = {}
        for key, item in record._asdict().items():
            if key == records.STATE:
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

    def _remap(self, record_dict: dict) -> dict:
        """Given a dictionary return a new dictionary with the key names that we use"""
        remapped = {}
        for key, value in record_dict.items():
            split_key = key.split('.')
            split_key[0] = self.KEY_MAP[split_key[0]]
            remapped['.'.join(split_key)] = value
        return remapped


class GridFsFile(builtins.BaseFile):
    TYPE_ID = uuid.UUID('3bf3c24e-f6c8-4f70-956f-bdecd7aed091')
    ATTRS = '_persistent_id', '_file_id'

    def __init__(self,
                 file_bucket: gridfs.GridFSBucket,
                 filename: str = None,
                 encoding: str = None):
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

    def save_instance_state(self, saver: depositors.Saver):
        filename = self.filename or ""
        with open(self._buffer_file, 'rb') as fstream:
            self._file_id = self._file_store.upload_from_stream(filename, fstream)

        return super().save_instance_state(saver)

    def load_instance_state(self, saved_state, loader: depositors.Loader):
        super().load_instance_state(saved_state, loader)
        self._file_store = loader.get_archive().get_gridfs_bucket()  # type: gridfs.GridFSBucket
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


def flatten_filter(entry_name: str, query) -> dict:
    """Expand nested search criteria, e.g. state={'color': 'red'} -> {'state.colour': 'red'}"""
    flattened = {}

    if isinstance(query, dict):
        predicates = []
        # Sort out entries containing operators and those without
        for key, value in query.items():
            if key.startswith('$'):
                predicates.append({key: value})
            else:
                flattened.update({"{}.{}".format(entry_name, key): value})
        if predicates:
            flattened.update(and_(*[{entry_name: predicate} for predicate in predicates]))

    else:
        flattened[entry_name] = query

    return flattened
