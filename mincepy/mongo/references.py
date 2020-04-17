from typing import Sequence

import pymongo

import mincepy
from mincepy import qops
from . import db


class ReferenceManager:

    def __init__(self, ref_collection: pymongo.collection.Collection,
                 data_collection: pymongo.collection.Collection):
        self._references = ref_collection
        self._data_collection = data_collection

    def get_reference_graphs(self, srefs: Sequence[mincepy.SnapshotRef]):
        self._ensure_current()

        ids = tuple(db.to_id_dict(sref) for sref in srefs)
        pipeline = [{
            '$match': {
                '_id': qops.in_(*ids)
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

        edge_lists = []
        for result in self._references.aggregate(pipeline):
            edge_list = []
            # Do the first entry as special case
            my_id = result['_id']
            for neighbour_id in result['refs']:
                edge_list.append((db.to_sref(my_id), db.to_sref(neighbour_id)))
            for entry in result['references']:
                entry_id = entry['_id']
                if entry_id == my_id:
                    # Prevent double counting when there are cyclic refs
                    continue

                for neighbour_id in entry['refs']:
                    edge_list.append((db.to_sref(entry_id), db.to_sref(neighbour_id)))
            if edge_list:
                edge_lists.append(edge_list)

        return edge_lists

    def _ensure_current(self):
        """This call ensures that the reference collection is up to date"""
        # Find all the objects that we don't have in the references collection

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

        cur = self._data_collection.aggregate(pipeline)
        to_insert = []
        for data_entry in cur:
            ref_entry = _generate_ref_entry(data_entry)
            ref_entry['_id'] = data_entry['_id']
            to_insert.append(ref_entry)

        # Now insert all the calculated refs
        self._references.insert_many(to_insert)


def _generate_ref_entry(data_entry: dict) -> dict:
    refs = [db.to_id_dict(info[1]) for info in db.to_record(data_entry).get_references()]
    return {'refs': refs}
