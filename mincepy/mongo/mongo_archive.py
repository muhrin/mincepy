# -*- coding: utf-8 -*-
from typing import Optional, Sequence, Union, Iterable, Mapping, Iterator, Dict, Tuple
import weakref
from urllib import parse
import uuid

import bson
import gridfs
import networkx
import pymongo
import pymongo.uri_parser
import pymongo.database
import pymongo.errors

# MincePy imports
from mincepy import archives
from mincepy import helpers
from mincepy import operations
from mincepy import q
from mincepy import records
from mincepy import exceptions

# Local imports
from . import bulk
from . import migrate
from . import migrations
from . import db
from . import references
from . import queries

__all__ = ("MongoArchive", "connect")

DEFAULT_REFERENCES_COLLECTION = "references"

scalar_query_spec = archives.scalar_query_spec


class ObjectIdHelper(helpers.TypeHelper):
    TYPE = bson.ObjectId
    TYPE_ID = uuid.UUID("bdde0765-36d2-4f06-bb8b-536a429f32ab")

    def yield_hashables(self, obj, hasher):  # pylint: disable=unused-argument
        yield obj.binary

    def eq(self, one, other) -> bool:  # pylint: disable=invalid-name
        return one == other

    def save_instance_state(self, obj, _depositor):
        return obj

    def load_instance_state(self, obj, saved_state, _depositor):
        return obj.__init__(saved_state)  # pylint: disable=unnecessary-dunder-call


class MongoArchive(archives.BaseArchive[bson.ObjectId]):
    """MongoDB implementation of the mincepy archive"""

    # pylint: disable=too-many-public-methods

    ID_TYPE = bson.ObjectId
    SnapshotId = records.SnapshotId[bson.ObjectId]

    DATA_COLLECTION = "data"
    HISTORY_COLLECTION = "history"

    @classmethod
    def get_types(cls) -> Sequence:
        return (ObjectIdHelper(),)

    def __init__(self, database: pymongo.database.Database):
        super().__init__()

        self._database = database
        migrate.ensure_up_to_date(database, migrations.LATEST)

        self._data_collection = database[self.DATA_COLLECTION]
        self._history_collection = database[self.HISTORY_COLLECTION]
        self._file_bucket = gridfs.GridFSBucket(database)
        self._refman = references.ReferenceManager(
            database[DEFAULT_REFERENCES_COLLECTION],
            self._data_collection,
            self._history_collection,
        )

        self._snapshots = MongoRecordCollection(self, self._history_collection)
        self._objects = MongoRecordCollection(self, self._data_collection)

        self._create_indices()

    @property
    def database(self) -> pymongo.database.Database:
        return self._database

    @property
    def data_collection(self) -> pymongo.database.Collection:
        return self._data_collection

    @property
    def snapshots(self) -> "MongoRecordCollection":
        return self._snapshots

    @property
    def objects(self) -> "MongoRecordCollection":
        """Access the objects collection"""
        return self._objects

    @property
    def file_store(self) -> gridfs.GridFSBucket:
        return self._file_bucket

    @property
    def schema_version(self) -> Optional[int]:
        return migrate.get_version(self._database)

    def _create_indices(self):
        # Create all the necessary indexes
        self._data_collection.create_index(db.OBJ_ID)
        self._history_collection.create_index(
            [(db.OBJ_ID, pymongo.ASCENDING), (db.VERSION, pymongo.ASCENDING)],
            unique=True,
        )
        # Performance related
        self._data_collection.create_index(db.TYPE_ID, unique=False)
        self._data_collection.create_index(db.VERSION, unique=False)

    def create_archive_id(self):
        return bson.ObjectId()

    def construct_archive_id(self, value) -> bson.ObjectId:
        if not isinstance(value, str):
            raise TypeError(f"Cannot construct an ObjectID from a '{type(value)}'")
        try:
            return bson.ObjectId(value)
        except bson.errors.InvalidId as exc:
            raise ValueError(str(exc)) from None

    def get_gridfs_bucket(self) -> gridfs.GridFSBucket:
        return self._file_bucket

    def bulk_write(self, ops: Sequence[operations.Operation]):
        self._fire_event(archives.ArchiveListener.on_bulk_write, ops)

        # First, convert these to corresponding mongo bulk operations.  Because of the way we split
        # objects into 'data' and 'history' we have to perform these operations on both
        data_ops = []
        history_ops = []

        for data_op, history_op in map(bulk.to_mongo_op, ops):
            data_ops.extend(data_op)
            history_ops.extend(history_op)

        try:
            # First perform the data operations
            self._data_collection.bulk_write(data_ops, ordered=True)
            # Then the history operations
            self._history_collection.bulk_write(history_ops, ordered=True)
        except pymongo.errors.BulkWriteError as exc:
            write_errors = exc.details["writeErrors"]
            if write_errors and write_errors[0]["code"] == 11000:
                raise exceptions.DuplicateKeyError(str(exc)) from exc

            raise  # Otherwise, just raise what we got

        self._refman.invalidate(
            obj_ids=[op.obj_id for op in ops],
            snapshot_ids=[op.snapshot_id for op in ops],
        )

        self._fire_event(archives.ArchiveListener.on_bulk_write_complete, ops)

    def load(self, snapshot_id: records.SnapshotId) -> records.DataRecord:
        if not isinstance(snapshot_id, records.SnapshotId):
            raise TypeError(snapshot_id)

        results = tuple(
            self._history_collection.find(
                {db.OBJ_ID: snapshot_id.obj_id, db.VERSION: snapshot_id.version}
            )
        )
        if not results:
            raise exceptions.NotFound(f"Snapshot id '{snapshot_id}' not found")
        return db.to_record(results[0])

    def get_snapshot_ids(self, obj_id: bson.ObjectId):
        results = self._history_collection.find(
            {db.OBJ_ID: obj_id},
            projection={db.OBJ_ID: 1, db.VERSION: 1},
            sort=[(db.VERSION, pymongo.ASCENDING)],
        )
        if not results:
            return []

        return list(map(db.sid_from_dict, results))

    # region Meta

    def meta_get(self, obj_id: bson.ObjectId):
        # Single obj id
        if not isinstance(obj_id, bson.ObjectId):
            raise TypeError(f"Must pass an ObjectId, got {obj_id}")
        found = self._data_collection.find_one({"_id": obj_id}, {db.META: 1})
        if found is None:
            return found
        found.pop("_id")
        return found.get(db.META, None)

    def meta_get_many(
        self, obj_ids: Iterable[bson.ObjectId]
    ) -> Dict[bson.ObjectId, dict]:
        # Find multiple
        for obj_id in obj_ids:
            if not isinstance(obj_id, bson.ObjectId):
                raise TypeError(f"Must pass an ObjectId, got {obj_id}")

        cur = self._data_collection.find({"_id": q.in_(*obj_ids)}, {db.META: 1})
        results = {oid: None for oid in obj_ids}
        for found in cur:
            results[found.pop("_id")] = found[db.META]

        return results

    def meta_set(self, obj_id, meta):
        try:
            found = self._data_collection.update_one(
                {"_id": obj_id}, {"$set": {db.META: meta}}, upsert=False
            )
        except pymongo.errors.DuplicateKeyError as exc:
            raise exceptions.DuplicateKeyError(str(exc))
        else:
            if found.modified_count == 0:
                raise exceptions.NotFound(f"No record with object id '{obj_id}' found")

    def meta_set_many(self, metas: Mapping[bson.ObjectId, Optional[dict]]):
        ops = []
        for obj_id, meta in metas.items():
            operation = pymongo.operations.UpdateOne(
                {"_id": obj_id}, {"$set": {db.META: meta}}, upsert=False
            )
            ops.append(operation)

        try:
            self._data_collection.bulk_write(ops, ordered=True)
        except pymongo.errors.BulkWriteError as exc:
            # This is a rather complicated way to get the error - mongo doesn't seem to document
            # error codes, absolute madness.
            if exc.code == 65:
                write_errors = exc.details.get("writeErrors", {})
                if write_errors:
                    # There should only be one as it is an ordered write
                    error = write_errors[0]
                    if error.get("code") == 11000:
                        raise exceptions.DuplicateKeyError(error.get("errmsg"))
            raise

    def meta_update(self, obj_id, meta: Mapping):
        try:
            to_set = queries.expand_filter(db.META, meta)
            res = self._data_collection.update_one(
                {"_id": obj_id}, {"$set": to_set}, upsert=False
            )
        except pymongo.errors.DuplicateKeyError as exc:
            raise exceptions.DuplicateKeyError(str(exc))
        else:
            if res.matched_count == 0:
                raise exceptions.NotFound(f"No record with object id '{obj_id}' found")

    def meta_find(
        self,
        filter: dict = None,  # pylint: disable=redefined-builtin
        obj_id: Union[bson.ObjectId, Iterable[bson.ObjectId], Dict] = None,
    ) -> Iterator[Tuple[bson.ObjectId, Dict]]:
        match = queries.expand_filter(db.META, filter)
        if obj_id is not None:
            match["_id"] = scalar_query_spec(obj_id)

        for entry in self._data_collection.find(match, {db.META: 1}):
            if db.META in entry:
                oid = entry.pop("_id")
                yield self.MetaEntry(oid, entry[db.META])

    def meta_distinct(
        self,
        key: str,
        filter: dict = None,  # pylint: disable=redefined-builtin
        obj_id: Union[bson.ObjectId, Iterable[bson.ObjectId], Mapping] = None,
    ) -> "Iterator":
        match = queries.expand_filter(db.META, filter)
        if obj_id is not None:
            match["_id"] = scalar_query_spec(obj_id)

        yield from self._data_collection.distinct(f"{db.META}.{key}", match)

    def meta_create_index(self, keys, unique=True, where_exist=False):
        if isinstance(keys, str):
            keys = [(keys, pymongo.ASCENDING)]
        else:
            if not isinstance(keys, list):
                raise TypeError(
                    f"Keys must be a string or a list of tuples, got {keys.__class__.__name__}"
                )
            if len(keys) == 0:
                return
            for entry in keys:
                if not isinstance(entry, tuple):
                    raise TypeError(
                        f"Keys must be list of tuples, got {entry.__class__.__name__}"
                    )

        # Transform the keys
        keys = [(f"{db.META}.{name}", direction) for name, direction in keys]

        kwargs = {}
        if where_exist:
            key_names = tuple(entry[0] for entry in keys)
            kwargs["partialFilterExpression"] = q.and_(
                *tuple(q.exists_(name) for name in key_names)
            )

        self._data_collection.create_index(keys, unique=unique, **kwargs)

    # endregion

    # pylint: disable=too-many-arguments
    def find(
        self,
        obj_id: Union[bson.ObjectId, Iterable[bson.ObjectId], Dict] = None,
        type_id: Union[bson.ObjectId, Iterable[bson.ObjectId], Dict] = None,
        _created_by=None,
        _copied_from=None,
        version=None,
        state=None,
        state_types=None,
        snapshot_hash=None,
        meta: dict = None,
        extras: dict = None,
        limit=0,
        sort=None,
        skip=0,
    ):
        pipeline = self._get_pipeline(
            obj_id=obj_id,
            type_id=type_id,
            _created_by=_created_by,
            _copied_from=_copied_from,
            version=version,
            state=state,
            state_types=state_types,
            snapshot_hash=snapshot_hash,
            meta=meta,
            extras=extras,
        )

        if skip:
            pipeline.append({"$skip": skip})

        if limit:
            pipeline.append({"$limit": limit})

        if sort:
            if not isinstance(sort, dict):
                sort_dict = {sort: 1}
            else:
                sort_dict = sort
            sort_dict = db.remap(sort_dict)
            pipeline.append({"$sort": sort_dict})

        if version == -1:
            coll = self._data_collection
        else:
            coll = self._history_collection

        results = coll.aggregate(pipeline, allowDiskUse=True)

        for result in results:
            yield db.to_record(result)

    def find_exp(self, records_filter: dict):
        """Experimental find"""
        results = self._data_collection.find(records_filter)
        for result in results:
            yield db.to_record(result)

    def distinct(
        self, key: str, filter: dict = None
    ) -> Iterator:  # pylint: disable=redefined-builtin
        filter = db.remap(filter or {})

        if filter.get(db.VERSION, None) == -1:
            filter.pop(db.VERSION)
            coll = self._data_collection
        else:
            coll = self._history_collection

        yield from coll.distinct(db.remap_key(key), filter=_flatten_filter_dict(filter))

    def count(
        self,
        obj_id: Optional[bson.ObjectId] = None,
        type_id=None,
        _created_by=None,
        _copied_from=None,
        version=-1,
        state=None,
        snapshot_hash=None,
        meta=None,
        limit=0,
    ):

        pipeline = self._get_pipeline(
            obj_id=obj_id,
            type_id=type_id,
            _created_by=_created_by,
            _copied_from=_copied_from,
            version=version,
            state=state,
            snapshot_hash=snapshot_hash,
            meta=meta,
        )

        if version == -1:
            coll = self._data_collection
        else:
            coll = self._history_collection

        if limit:
            pipeline.append({"$limit": limit})

        pipeline.append({"$count": "total"})
        result = next(coll.aggregate(pipeline))

        return result["total"]

    def get_snapshot_ref_graph(
        self,
        *snapshot_ids: SnapshotId,
        direction=archives.OUTGOING,
        max_dist: int = None,
    ) -> Iterator[networkx.DiGraph]:
        return self._refman.get_snapshot_ref_graph(
            snapshot_ids, direction=direction, max_dist=max_dist
        )

    def get_obj_ref_graph(
        self, *obj_ids: bson.ObjectId, direction=archives.OUTGOING, max_dist: int = None
    ) -> Iterator[networkx.DiGraph]:
        return self._refman.get_obj_ref_graphs(
            obj_ids, direction=direction, max_dist=max_dist
        )

    @staticmethod
    def _get_pipeline(
        obj_id: Union[bson.ObjectId, Iterable[bson.ObjectId]] = None,
        type_id=None,
        _created_by=None,
        _copied_from=None,
        version=None,
        state=None,
        state_types=None,
        snapshot_hash=None,
        meta: dict = None,
        extras: dict = None,
    ):
        """Get a pipeline that would perform the given search.  Can be used directly in an aggregate
        call"""
        pipeline = []

        query = queries.QueryBuilder()

        if obj_id is not None:
            query.and_({db.OBJ_ID: scalar_query_spec(obj_id)})

        if version is not None and version != -1:
            query.and_({db.VERSION: version})

        if type_id is not None:
            query.and_({db.TYPE_ID: scalar_query_spec(type_id)})

        if state is not None:
            query.and_(*queries.flatten_filter(db.STATE, state))

        if state_types is not None:
            query.and_(*queries.flatten_filter(db.STATE_TYPES, state_types))

        if snapshot_hash is not None:
            query.and_({db.SNAPSHOT_HASH: scalar_query_spec(snapshot_hash)})

        if extras:
            query.and_(*queries.flatten_filter(db.EXTRAS, extras))

        if meta:
            query.and_(*queries.flatten_filter(db.META, meta))

        mfilter = query.build()
        if mfilter:
            pipeline.append({"$match": mfilter})

        return pipeline


def _flatten_filter_dict(filter: dict) -> dict:  # pylint: disable=redefined-builtin
    query = queries.QueryBuilder()

    obj_id = filter.get(db.OBJ_ID, None)
    type_id = filter.get(db.TYPE_ID, None)
    version = filter.get(db.VERSION, None)
    state = filter.get(db.STATE, None)
    state_types = filter.get(db.STATE_TYPES)
    snapshot_hash = filter.get(db.SNAPSHOT_HASH)
    extras = filter.get(db.EXTRAS)

    if obj_id is not None:
        query.and_({db.OBJ_ID: scalar_query_spec(obj_id)})

    if version is not None and version != -1:
        query.and_({db.VERSION: version})

    if type_id is not None:
        query.and_({db.TYPE_ID: scalar_query_spec(type_id)})

    if state is not None:
        query.and_(*queries.flatten_filter(db.STATE, state))

    if state_types is not None:
        query.and_(*queries.flatten_filter(db.STATE_TYPES, state_types))

    if snapshot_hash is not None:
        query.and_({db.SNAPSHOT_HASH: scalar_query_spec(snapshot_hash)})

    if extras:
        query.and_(*queries.flatten_filter(db.EXTRAS, extras))

    return query.build()


class MongoRecordCollection(archives.RecordCollection):
    def __init__(self, archive: MongoArchive, collection: pymongo.database.Collection):
        self._archive = archive
        self._collection = collection

    @property
    def archive(self) -> MongoArchive:
        """Get the corresponding archive"""
        return self._archive

    def find(
        self,
        filter: dict,  # pylint: disable=redefined-builtin
        *,
        projection=None,
        meta: dict = None,
        limit=0,
        sort=None,
        skip=0,
    ) -> Iterator[dict]:

        # Create the pipeline
        pipeline = []

        if filter:
            if meta:
                # Add the metadata criterion
                filter = q.and_(filter, *queries.flatten_filter(db.META, meta))

            pipeline.append({"$match": filter})

        if sort:
            if not isinstance(sort, dict):
                sort_dict = {sort: 1}
            else:
                sort_dict = sort
            sort_dict = db.remap(sort_dict)
            pipeline.append({"$sort": sort_dict})

        if skip:
            pipeline.append({"$skip": skip})

        if limit:
            pipeline.append({"$limit": limit})

        if projection:
            pipeline.append({"$project": db.remap(projection)})

        for entry in self._collection.aggregate(pipeline, allowDiskUse=True):
            yield db.remap_back(entry)

    def distinct(
        self,
        key: str,
        filter: dict = None,  # pylint: disable=redefined-builtin
    ) -> Iterator[dict]:
        key = db.remap_key(key)
        yield from self._collection.distinct(key, filter)

    def get(self, entry_id: bson.ObjectId) -> dict:
        doc = self._collection.find_one({"_id": entry_id})  # type: dict
        if doc is None:
            raise exceptions.NotFound(entry_id)
        return db.remap_back(doc)

    def count(
        self, filter: dict, *, meta: dict = None  # pylint: disable=redefined-builtin
    ) -> int:
        """Get the number of entries that match the search criteria"""
        # Create the pipeline
        pipeline = []

        if filter:
            if meta:
                # Add the metadata criterion
                filter = q.and_(filter, *queries.flatten_filter(db.META, meta))

            pipeline.append({"$match": filter})

        pipeline.append({"$count": "total"})
        try:
            result = next(self._collection.aggregate(pipeline))
        except StopIteration:
            return 0
        else:
            return result["total"]


MOCKED = weakref.WeakValueDictionary()


def connect(uri: str, timeout=30000) -> MongoArchive:
    """
    Connect to the database using the passed URI string.

    :param uri: specification of where to connect to
    :param timeout: a connection time (in milliseconds)
    :return: the connected mongo archive
    """
    parsed = parse.urlparse(uri)

    if parsed.scheme == "mongodb":
        return pymongo_connect(uri, timeout=timeout)
    if parsed.scheme == "mongomock":
        return mongomock_connect(uri, timeout=timeout)
    if parsed.scheme == "litemongo":
        return litemongo_connect(uri)

    raise ValueError(f"Unknown scheme: {uri}")


def pymongo_connect(uri, database: str = None, timeout=30000):
    # URI Format is:
    # mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[database][?options]]
    try:
        parsed = pymongo.uri_parser.parse_uri(uri)
    except pymongo.errors.InvalidURI as exc:
        raise ValueError(str(exc)) from exc

    if not parsed.get("database", None):
        raise ValueError(f"Failed to supply database on MongoDB uri: {uri}")

    try:
        client = pymongo.MongoClient(
            uri, connect=True, serverSelectionTimeoutMS=timeout
        )
        database = client.get_default_database()
        return MongoArchive(database)
    except pymongo.errors.ServerSelectionTimeoutError as exc:
        raise exceptions.ConnectionError(str(exc))


def mongomock_connect(uri, timeout=30000) -> MongoArchive:
    # Cache, this makes sure that if we get two requests to connect to exactly the same URI then
    # an existing connection will be returned
    import mongomock
    import mongomock.gridfs

    global MOCKED  # pylint: disable=global-statement, global-variable-not-assigned
    if uri in MOCKED:
        return MOCKED[uri]

    parsed = parse.urlparse(uri)

    mongomock.gridfs.enable_gridfs_integration()
    client = mongomock.MongoClient(
        f"mongodb://localhost/{parsed.fragment}", serverSelectionTimeoutMS=timeout
    )
    database = client.get_default_database()
    return MongoArchive(database)


def litemongo_connect(uri) -> MongoArchive:
    import litemongo
    import litemongo._vendor.mongomock.gridfs

    litemongo._vendor.mongomock.gridfs.enable_gridfs_integration()  # pylint: disable=protected-access

    client = litemongo.connect(uri)
    database = client.get_default_database()
    archive = MongoArchive(database)

    return archive
