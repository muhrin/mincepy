import itertools
import logging
from typing import Sequence, Union, Callable, Iterator, Iterable

import bson
import networkx
import pymongo.collection

import mincepy
from mincepy import OUTGOING
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

    def get_obj_ref_graphs(
            self, obj_ids: Sequence[bson.ObjectId], direction=OUTGOING, max_dist: int = None) \
            -> networkx.DiGraph:
        return self._get_graph(obj_ids, direction=direction, max_dist=max_dist)

    def get_snapshot_ref_graph(
            self, ids: Sequence[mincepy.SnapshotId], direction=OUTGOING, max_dist: int = None) \
            -> Iterator[networkx.DiGraph]:
        """Get the reference graph for a sequence of ids"""
        return self._get_graph(ids,
                               direction=direction,
                               max_dist=max_dist,
                               node_factory=db.sid_from_str)

    def invalidate(self, obj_ids: Iterable[bson.ObjectId],
                   snapshot_ids: Iterable[types.SnapshotId]):
        """Invalidate the cache for these objects and snapshots"""
        to_delete = list(obj_ids)
        for sid in snapshot_ids:
            to_delete.append(str(sid))
        self._references.delete_many({'_id': q.in_(*to_delete)})

    def _get_graph(self,
                   ids: Sequence[Union[bson.ObjectId, types.SnapshotId]],
                   direction=OUTGOING,
                   max_dist: int = None,
                   node_factory: Callable = None) -> networkx.DiGraph:
        """Get the reference graph for a sequence of ids"""
        if max_dist == 0:
            # Special case for 0 distance, can't be any references just the ids as lone nodes
            graph = networkx.DiGraph()
            for entry_id in ids:
                graph.add_node(entry_id)
            return graph

        node_factory = node_factory or (lambda x: x)

        search_max_dist = max_dist
        if max_dist is not None and direction == OUTGOING:
            search_max_dist = max(max_dist - 1, 0)

        search_ids = self._prepare_for_ref_search(ids)
        pipeline = self._get_ref_pipeline(search_ids, direction=direction, max_dist=search_max_dist)
        ref_results = {result['_id']: result for result in self._references.aggregate(pipeline)}

        graph = networkx.DiGraph()

        for entry_id in search_ids:
            this_id = node_factory(entry_id)

            graph.add_node(this_id)
            result = ref_results.get(entry_id, None)
            if result:
                refs = result.get('references', [])

                # First add all the nodes
                for ref in refs:
                    neighbour = node_factory(ref['_id'])
                    graph.add_node(neighbour)

                # Then the edges
                for entry in itertools.chain([result], refs):
                    this = node_factory(entry['_id'])

                    for neighbour_id in entry['refs']:
                        neighbour = node_factory(neighbour_id)

                        if direction == OUTGOING or neighbour in graph.nodes:
                            graph.add_edge(this, neighbour)

        return graph

    def _get_ref_pipeline(self, ids: Sequence, direction=OUTGOING, max_dist: int = None) -> list:
        """Get the reference lookup pipeline."""
        if max_dist is not None and max_dist < 0:
            raise ValueError("max_dist must be positive, got '{}'".format(max_dist))

        pipeline = [{'$match': {'_id': q.in_(*ids)}}]

        if max_dist is None or max_dist != 0:
            lookup_params = {
                'from': self._references.name,
                'as': 'references',
                'depthField': 'depth'
            }
            if direction == OUTGOING:
                lookup_params.update(
                    dict(startWith='$refs', connectFromField='refs', connectToField='_id'))
            else:
                lookup_params.update(
                    dict(startWith='$_id', connectFromField='_id', connectToField='refs'))

            if max_dist is not None:
                lookup_params['maxDepth'] = max_dist - 1

            # Only do graph lookup if checking depth that involves a hop
            pipeline.append({'$graphLookup': lookup_params})

        return pipeline

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
