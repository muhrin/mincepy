import logging
from typing import Sequence, Union, Callable, Iterator, Iterable, List

import bson
import pymongo.collection

import mincepy
from mincepy import q
from . import db
from . import types

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class ReferenceManager:
    """Reference manager is able to find references between objects efficiently.

    This relies on the following assumptions:
        * The data collection _id field os the obj_id, and,
        * The history collection _id field is the snapshot id string
    """

    def __init__(self, ref_collection: pymongo.collection.Collection,
                 data_collection: pymongo.collection.Collection,
                 history_collection: pymongo.collection.Collection):
        self._references = ref_collection
        self._data_collection = data_collection
        self._history_collection = history_collection

    def get_object_ref_graphs(self,
                              obj_ids: Sequence[bson.ObjectId]) -> Sequence[types.ObjRefGraph]:
        ref_graph = []  # type: List[types.ObjRefGraph]
        for graph in self._get_edges(obj_ids):
            transformed = [types.ObjRefEdge(source, target) for source, target in graph]
            ref_graph.append(transformed)

        return ref_graph

    def get_reference_graphs(self,
                             ids: Sequence[mincepy.SnapshotId]) -> Sequence[types.SnapshotRefGraph]:
        """Get the reference graph for a sequence of ids"""
        ref_graph = []  # type: List[types.SnapshotRefGraph]
        for graph in self._get_edges(ids):
            transformed = [
                types.SnapshotRefEdge(db.sid_from_str(source), db.sid_from_str(target))
                for source, target in graph
            ]
            ref_graph.append(transformed)

        return ref_graph

    def invalidate(self, obj_ids: Iterable[bson.ObjectId],
                   snapshot_ids: Iterable[types.SnapshotId]):
        """Invalidate the cache for these objects and snapshots"""
        to_delete = list(obj_ids)
        for sid in snapshot_ids:
            to_delete.append(str(sid))
        self._references.delete_many({'_id': q.in_(*to_delete)})

    def _get_edges(self, ids: Sequence[Union[bson.ObjectId, types.SnapshotId]]) -> Iterator[tuple]:
        """Get the reference graph for a sequence of ids"""
        ids = self._prepare_for_ref_search(ids)
        pipeline = self._get_pipeline(ids)

        for result in self._references.aggregate(pipeline):
            edge_list = []
            # Do the first entry as special case
            my_id = result['_id']
            for neighbour_id in result['refs']:
                edge_list.append((my_id, neighbour_id))

            for entry in result['references']:
                entry_id = entry['_id']
                if entry_id == my_id:
                    # Prevent double counting when there are cyclic refs
                    continue

                for neighbour_id in entry['refs']:
                    edge_list.append((entry_id, neighbour_id))

            yield edge_list

    def _get_pipeline(self, ids: Sequence) -> list:
        return [{
            '$match': {
                '_id': q.in_(*ids)
            }
        }, {
            '$graphLookup': {
                'from': self._references.name,
                'startWith': '$refs',
                'connectFromField': 'refs',
                'connectToField': '_id',
                'as': 'references'
            }
        }]

    def _prepare_for_ref_search(self, ids: Sequence[Union[bson.ObjectId, mincepy.SnapshotId]]):
        hist_updated = False
        data_updated = False
        converted = []
        for entry in ids:
            if isinstance(entry, mincepy.SnapshotId):
                if not hist_updated:
                    self._ensure_current('history')
                    hist_updated = True

                # We use the string version of a snapshot id
                converted.append(str(entry))
            elif isinstance(entry, bson.ObjectId):
                if not data_updated:
                    self._ensure_current('data')
                    data_updated = True

                converted.append(entry)
            else:
                raise TypeError(entry)

        return converted

    def _ensure_current(self, collection_name: str):
        """This call ensures that the reference collection is up to date"""
        # Find all the objects that we don't have in the references collection
        if collection_name == 'history':
            collection = self._history_collection
            id_func = lambda schema_entry: str(schema_entry[1])
        elif collection_name == 'data':
            collection = self._data_collection
            id_func = lambda schema_entry: schema_entry[1].obj_id
        else:
            raise ValueError("Unsupported collection: {}".format(collection_name))

        to_insert = []
        for data_entry in self._get_missing_entries(collection):
            ref_entry = _generate_ref_entry(data_entry, id_func)
            ref_entry['_id'] = data_entry['_id']
            to_insert.append(ref_entry)

        # Now insert all the calculated refs
        if to_insert:
            logging.info("Updating references cache for collection '%s' with %i new entries",
                         collection_name, len(to_insert))
            self._references.insert_many(to_insert)

    def _get_missing_entries(self, collection: pymongo.collection.Collection) -> Iterator:
        """Given a collection get the records that do not have entries in the references cache"""
        pipeline = [{
            '$lookup': {
                'from': self._references.name,
                'localField': '_id',
                'foreignField': '_id',
                'as': 'references'
            }
        }, {
            '$match': {
                'references': []
            }
        }]
        yield from collection.aggregate(pipeline)


def _generate_ref_entry(data_entry: dict, id_func: Callable) -> dict:
    refs = [id_func(info) for info in db.to_record(data_entry).get_references()]
    return {'refs': refs}
