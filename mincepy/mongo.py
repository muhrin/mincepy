import typing

import bson
import pymongo

from .archive import BaseArchive, DataRecord

__all__ = ('MongoArchive',)


class MongoArchive(BaseArchive):
    DATA_COLLECTION = 'data'

    def __init__(self, db):
        super(MongoArchive, self).__init__()
        self._data_collection = db[self.DATA_COLLECTION]

    def create_archive_id(self):
        return bson.ObjectId()

    def save(self, record: DataRecord):
        meta = None
        if record.ancestor_id:
            meta = self._data_collection.find_one(_id=record.ancestor_id, projection={'meta': 1})['meta']

        entry = self._to_entry(record, meta)
        self._data_collection.insert_one(entry)

    def save_many(self, records: typing.List[DataRecord]):
        # Collect all the ones that have ancestors
        ancestor_ids = {record.ancestor_id for record in records if record.ancestor_id}
        # Now check that the ancestors are accounted for, either in the list passed or in the database
        ancestor_ids -= {record.persistent_id for record in records}

        metadatas = {}
        if ancestor_ids:
            # Have to go look in the collection, collect the metadata while we're at it
            results = self._data_collection.find({'_id': {'$in': list(ancestor_ids)}}, projection={'meta': 1})
            for entry in results:
                ancestor_ids.discard(entry['_id'])
                metadatas[entry['_id']] = entry['meta']

        if ancestor_ids:
            raise ValueError(
                "Records were passed that refer to ancestors not present the passed list nor in the archive")

        # Generate the entries for our collection collecting the metadata that we gathered
        entries = [self._to_entry(record, metadatas.get(record.ancestor_id, None)) for record in records]
        self._data_collection.insert_many(entries)

    def load(self, archive_id) -> DataRecord:
        entry = self._data_collection.find_one(filter=archive_id)
        return self._to_record(entry)

    def get_meta(self, persistent_id):
        result = self._data_collection.find_one(persistent_id, projection={'meta': 1})
        if not result:
            # TODO: Should raise some kind of NotFound exception here
            return None

        return result['meta']

        # meta = result.get('meta', None)
        # if meta:
        #     return meta
        #
        # # OK, no meta, so search the ancestors
        # match_initial_document = {'$match': {'_id': persistent_id}}
        # find_ancestors = {
        #     "$graphLookup": {
        #         "from": self._data_collection.name,
        #         "startWith": "$ancestor_id",
        #         "connectFromField": "ancestor_id",
        #         "connectToField": "_id",
        #         "as": "ancestors",
        #         "depthField": "depth"
        #     }
        # }
        # unwind_ancestors = {'$unwind': '$ancestors'}
        # match_with_meta = {'$match': {'ancestors.meta': {'$ne': None}}}
        # sort_closest = {'$sort': {'ancestors.depth': pymongo.ASCENDING}}
        # limit_1 = {'$limit': 1}
        # result = tuple(self._data_collection.aggregate([
        #     match_initial_document,
        #     find_ancestors,
        #     unwind_ancestors,
        #     match_with_meta,
        #     sort_closest,
        #     limit_1
        # ]))
        #
        # if not result:
        #     return None
        #
        # entry = result[0]
        # if result[0]['ancestors']:
        #     return entry['ancestors']['meta']

    def set_meta(self, persistent_id, meta):
        self._data_collection.find_one_and_update({'_id': persistent_id}, {'$set': {'meta': meta}})

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

    def get_leaves(self, persistent_id):
        match_initial_document = {'$match': {'_id': persistent_id}}
        find_descendents = {
            "$graphLookup": {
                "from": self._data_collection.name,
                "startWith": "$_id",
                "connectFromField": "_id",
                "connectToField": "ancestor_id",
                "as": "descendents"
            }
        }
        unwind_descendents = {'$unwind': '$descendents'}
        replace_root = {'$replaceRoot': {'newRoot': '$descendents'}}
        # TODO: This can probably be replaced with a join rather than a graph lookup
        find_descendent_descendents = {
            "$graphLookup": {
                "from": self._data_collection.name,
                "startWith": "$_id",
                "connectFromField": "_id",
                "connectToField": "ancestor_id",
                "as": "children",
                'maxDepth': 1
            }
        }
        match_no_children = {'$match': {'children': []}}

        pipeline = [
            match_initial_document,
            find_descendents,
            unwind_descendents,
            replace_root,
            find_descendent_descendents,
            match_no_children
        ]
        cursor = self._data_collection.aggregate(pipeline)
        return [self._to_record(entry) for entry in cursor]

    def _to_record(self, entry):
        return DataRecord(
            entry['_id'],
            entry['type_id'],
            entry['ancestor_id'],
            entry['obj'],
            entry['hash'],
        )

    def _to_entry(self, record, meta=None):
        return {
            '_id': record.persistent_id,
            'type_id': record.type_id,
            'ancestor_id': record.ancestor_id,
            'obj': record.state,
            'hash': record.obj_hash,
            'meta': meta
        }
