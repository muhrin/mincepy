from typing import Optional, Sequence, Union, Iterable, Mapping, Iterator, Dict, Tuple
import uuid

import bson
import gridfs
import pymongo
import pymongo.uri_parser
import pymongo.database
import pymongo.errors

import mincepy
import mincepy.records
from mincepy import qops

from . import files
from . import db
from . import references
from . import queries

__all__ = ('MongoArchive',)

DEFAULT_REFERENCES_COLLECTION = 'references'

scalar_query_spec = mincepy.archives.scalar_query_spec  # pylint: disable=invalid-name


class ObjectIdHelper(mincepy.TypeHelper):
    TYPE = bson.ObjectId
    TYPE_ID = uuid.UUID('bdde0765-36d2-4f06-bb8b-536a429f32ab')

    def yield_hashables(self, obj, hasher):
        yield obj.binary

    def eq(self, one, other) -> bool:
        return one.__eq__(other)

    def save_instance_state(self, obj, _depositor):
        return obj

    def load_instance_state(self, obj, saved_state, _depositor):
        return obj.__init__(saved_state)


class MongoArchive(mincepy.BaseArchive[bson.ObjectId]):
    """MongoDB implementation of the mincepy archive"""

    # pylint: disable=too-many-public-methods

    ID_TYPE = bson.ObjectId
    SnapshotRef = mincepy.SnapshotRef[bson.ObjectId]

    DATA_COLLECTION = 'data'
    META_COLLECTION = 'meta'

    @classmethod
    def get_types(cls) -> Sequence:
        return ObjectIdHelper(), files.GridFsFile

    def __init__(self, database: pymongo.database.Database):
        self._database = database
        self._data_collection = database[self.DATA_COLLECTION]
        self._meta_collection = database[self.META_COLLECTION]
        self._file_bucket = gridfs.GridFSBucket(database)
        self._refman = references.ReferenceManager(
            self._data_collection[DEFAULT_REFERENCES_COLLECTION], self._data_collection)
        self._create_indices()

    @property
    def database(self) -> pymongo.database.Database:
        return self._database

    @property
    def data_collection(self) -> pymongo.database.Collection:
        return self._data_collection

    def _create_indices(self):
        # Create all the necessary indexes
        self._data_collection.create_index(db.OBJ_ID)
        self._data_collection.create_index(db.VERSION)

    def create_archive_id(self):  # pylint: disable=no-self-use
        return bson.ObjectId()

    def construct_archive_id(self, value) -> bson.ObjectId:  # pylint: disable=no-self-use
        if not isinstance(value, str):
            raise TypeError("Cannot construct an ObjectID from a '{}'".format(type(value)))
        try:
            return bson.ObjectId(value)
        except bson.errors.InvalidId as exc:
            raise ValueError(str(exc))

    def create_file(self, filename: str = None, encoding: str = None):
        return files.GridFsFile(self._file_bucket, filename, encoding)

    def get_gridfs_bucket(self) -> gridfs.GridFSBucket:
        return self._file_bucket

    def save(self, record: mincepy.DataRecord):
        self.save_many([record])
        return record

    def save_many(self, records: Sequence[mincepy.DataRecord]):
        # Generate the entries for our collection collecting the metadata that we gathered
        entries = [db.to_entry(record) for record in records]
        try:
            self._data_collection.insert_many(entries)
        except pymongo.errors.BulkWriteError as exc:
            write_errors = exc.details['writeErrors']
            if write_errors:
                raise mincepy.ModificationError(
                    "You're trying to rewrite history, that's not allowed!")
            raise  # Otherwise just raise what we got

    def load(self, reference: mincepy.SnapshotRef) -> mincepy.DataRecord:
        if not isinstance(reference, mincepy.SnapshotRef):
            raise TypeError(reference)

        results = tuple(self._data_collection.find({'_id': db.to_id_dict(reference)}))
        if not results:
            raise mincepy.NotFound("Snapshot id '{}' not found".format(reference))
        return db.to_record(results[0])

    def get_snapshot_refs(self, obj_id: bson.ObjectId):
        results = self._data_collection.find({db.OBJ_ID: obj_id},
                                             projection={'_id': 1},
                                             sort=[(db.VERSION, pymongo.ASCENDING)])
        if not results:
            return []

        return [db.to_sref(result['_id']) for result in results]

    # region Meta

    def meta_get(self, obj_id: Union[bson.ObjectId, Iterable[bson.ObjectId]]):
        # Single obj id
        if not isinstance(obj_id, bson.ObjectId):
            raise TypeError("Must pass an ObjectId, got {}".format(obj_id))
        found = self._meta_collection.find_one({'_id': obj_id})
        if found is None:
            return found
        found.pop('_id')
        return found

    def meta_get_many(self, obj_ids: Iterable[bson.ObjectId]) -> Dict[bson.ObjectId, dict]:
        # Find multiple
        for obj_id in obj_ids:
            if not isinstance(obj_id, bson.ObjectId):
                raise TypeError("Must pass an ObjectId, got {}".format(obj_id))

        cur = self._meta_collection.find({'_id': qops.in_(*obj_ids)})
        results = {oid: None for oid in obj_ids}
        for found in cur:
            results[found.pop('_id')] = found

        return results

    def meta_set(self, obj_id, meta):
        if meta:
            try:
                found = self._meta_collection.replace_one({'_id': obj_id}, meta, upsert=True)
            except pymongo.errors.DuplicateKeyError as exc:
                raise mincepy.DuplicateKeyError(str(exc))
            else:
                if not found:
                    raise mincepy.NotFound("No record with snapshot id '{}' found".format(obj_id))
        else:
            # Just remove the meta entry outright
            self._meta_collection.delete_one({'_id': obj_id})

    def meta_set_many(self, metas: Mapping[bson.ObjectId, Optional[dict]]):
        documents = []
        for obj_id, meta in metas.items():
            meta = dict(meta)
            meta['_id'] = obj_id
            documents.append(meta)

        self._meta_collection.insert_many(documents, ordered=False)

    def meta_update(self, obj_id, meta: Mapping):
        if meta.get('_id', obj_id) != obj_id:
            raise ValueError("Cannot use the key _id, in metadata: it is reserved")

        try:
            self._meta_collection.update_one({'_id': obj_id}, {'$set': meta}, upsert=True)
        except pymongo.errors.DuplicateKeyError as exc:
            raise mincepy.DuplicateKeyError(str(exc))

    def meta_find(
        self,
        filter: dict,  # pylint: disable=redefined-builtin
        obj_id: Union[bson.ObjectId, Iterable[bson.ObjectId], Dict] = None
    ) -> Iterator[Tuple[bson.ObjectId, Dict]]:
        match = dict(filter)
        if obj_id is not None:
            match['_id'] = scalar_query_spec(obj_id)

        for meta in self._meta_collection.find(match):
            oid = meta.pop('_id')
            yield self.MetaEntry(oid, meta)

    def meta_create_index(self, keys, unique=True, where_exist=False):
        kwargs = {}
        if where_exist:
            if not isinstance(keys, str) and isinstance(keys, Iterable):
                key_names = tuple(entry[0] for entry in keys)
            else:
                key_names = (keys,)
            kwargs['partialFilterExpression'] = \
                qops.and_(*tuple(qops.exists_(name) for name in key_names))
        self._meta_collection.create_index(keys, unique=unique, **kwargs)

    # endregion

    # pylint: disable=too-many-arguments
    def find(self,
             obj_id: Union[bson.ObjectId, Iterable[bson.ObjectId], Dict] = None,
             type_id: Union[bson.ObjectId, Iterable[bson.ObjectId], Dict] = None,
             _created_by=None,
             _copied_from=None,
             version=-1,
             state=None,
             deleted=True,
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
                                      deleted=deleted,
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
            sort_dict = db.remap(sort_dict)
            pipeline.append({'$sort': sort_dict})

        results = self._data_collection.aggregate(pipeline)

        for result in results:
            yield db.to_record(result)

    def count(self,
              obj_id: Optional[bson.ObjectId] = None,
              type_id=None,
              _created_by=None,
              _copied_from=None,
              version=-1,
              state=None,
              deleted=True,
              snapshot_hash=None,
              meta=None,
              limit=0):
        mfilter = {}
        if obj_id is not None:
            mfilter['obj_id'] = obj_id

        if type_id is not None:
            mfilter['type_id'] = type_id

        if state is not None:
            mfilter.update(flatten_filter(db.STATE, state))

        if not deleted:
            condition = [qops.ne_(mincepy.DELETED)]
            if db.STATE in mfilter:
                condition.append(mfilter[db.STATE])
            mfilter.update(flatten_filter(db.STATE, qops.and_(*condition)))

        if snapshot_hash is not None:
            mfilter[db.KEY_MAP[mincepy.SNAPSHOT_HASH]] = snapshot_hash

        if version == -1:
            # For counting we don't care which version we get we just want there to be only 1
            # counted per obj_id so select the first
            mfilter[db.VERSION] = 0
        else:
            mfilter[db.VERSION] = version

        pipeline = [{'$match': mfilter}]

        if meta:
            pipeline.extend(
                queries.pipeline_match_metadata(meta, self._meta_collection.name, db.OBJ_ID))

        if limit:
            pipeline.append({'$limit': limit})

        pipeline.append({'$count': "total"})
        result = next(self._data_collection.aggregate(pipeline))

        return result['total']

    def get_reference_graph(self,
                            srefs: Sequence[SnapshotRef]) -> Sequence[mincepy.Archive.RefGraph]:
        return self._refman.get_reference_graphs(srefs)

    def _get_pipeline(self,
                      obj_id: Union[bson.ObjectId, Iterable[bson.ObjectId]] = None,
                      type_id=None,
                      _created_by=None,
                      _copied_from=None,
                      version=-1,
                      state=None,
                      deleted=True,
                      snapshot_hash=None,
                      meta=None):
        """Get a pipeline that would perform the given search.  Can be used directly in an aggregate
         call"""
        # pylint: disable=too-many-branches
        pipeline = []

        mfilter = {}
        if obj_id is not None:
            mfilter[db.OBJ_ID] = scalar_query_spec(obj_id)

        if version is not None and version != -1:
            mfilter[db.VERSION] = version

        if type_id is not None:
            mfilter[db.TYPE_ID] = scalar_query_spec(type_id)

        if mfilter:
            pipeline.append({'$match': mfilter})

        if meta:
            pipeline.extend(
                queries.pipeline_match_metadata(meta, self._meta_collection.name, db.OBJ_ID))

        if version == -1:
            if isinstance(obj_id, bson.ObjectId):
                # Special case for just one object to find, faster
                pipeline.append({'$sort': {db.VERSION: pymongo.DESCENDING}})
                pipeline.append({'$limit': 1})
            else:
                # Join with a collection that is grouped to get the maximum version for each object
                # ID then only take the the matching documents
                pipeline.extend(queries.pipeline_latest_version(self._data_collection.name))

        post_match = {}

        if state is not None:
            post_match.update(flatten_filter(db.STATE, state))

        if not deleted:
            condition = [qops.ne_(mincepy.DELETED)]
            if db.STATE in post_match:
                condition.append(post_match[db.STATE])
            post_match.update(flatten_filter(db.STATE, qops.and_(*condition)))

        if snapshot_hash is not None:
            post_match[db.KEY_MAP[mincepy.SNAPSHOT_HASH]] = scalar_query_spec(snapshot_hash)

        if post_match:
            pipeline.append({'$match': post_match})

        return pipeline


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
            flattened.update(qops.and_(*[{entry_name: predicate} for predicate in predicates]))

    else:
        flattened[entry_name] = query

    return flattened
