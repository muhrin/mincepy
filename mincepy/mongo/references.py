import bson

import mincepy.mongo
from . import db

DEFAULT_REFERENCES_COLLECTION = 'references'


class ReferenceManager:

    def __init__(self, archive, ref_collection=DEFAULT_REFERENCES_COLLECTION):
        self._archive = archive  # type: mincepy.mongo.MongoArchive
        self._references = self._archive.database[ref_collection]

    def get_reference_graph(self, obj_id: bson.ObjectId, max_depth=None):
        self._ensure_current()
        results = self._references.aggregate([{
            'from': self._references.name,
            'startWith': '${}'.format(obj_id),
            'connectFromField': 'refs',
            'connectToField': db.OBJ_ID,
            'as': 'regs_graph',
            'maxDepth': max_depth,
        }])

        for result in results:
            yield result

    def get_referer_graph(self, obj_id: bson.ObjectId):
        pass

    def _ensure_current(self):
        """This call ensures that the reference collection is up to date"""
        self._archive.data_collection.aggregate([
            # Get the references entries for each record in the data collection
            {
                '$lookup': {
                    'from': self._archive.database.name,
                    'localField': db.OBJ_ID,
                    'foreign': db.OBJ_ID,
                    'as': 'refs'
                }
            },
            # Now get those that don't have an entry in the references collection
            {
                '$match': {
                    'refs': {
                        '$eq': []
                    }
                }
            }
        ])
