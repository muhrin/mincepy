from mincepy import qops
from . import db


def pipeline_latest_version(data_collection: str) -> list:
    """Returns a pipeline that will take the incoming data record documents and for each one find
    the latest version."""
    pipeline = []
    pipeline.extend([
        # Group by object id the maximum version
        {
            '$group': {
                '_id': "${}".format(db.OBJ_ID),
                'max_ver': {
                    '$max': '${}'.format(db.VERSION)
                }
            }
        },
        # Then do a lookup against the same collection to get the records
        {
            '$lookup': {
                'from': data_collection,
                'let': {
                    'obj_id': '$_id',
                    'max_ver': '$max_ver'
                },
                'pipeline': [{
                    '$match': {
                        '$expr': qops.eq_('$_id', {
                            'oid': '$$obj_id',
                            'v': '$$max_ver'
                        })
                    }
                }],
                'as': 'latest'
            }
        },
        # Now unwind and promote the 'latest' field
        {
            '$unwind': {
                'path': '$latest'
            }
        },
        {
            '$replaceRoot': {
                'newRoot': '$latest'
            }
        },
    ])

    return pipeline


def pipeline_match_metadata(meta: dict, meta_collection: str, local_field: str):
    pipeline = []

    pipeline.append({
        '$lookup': {
            'from': meta_collection,
            'localField': local_field,
            'foreignField': '_id',
            'as': '_meta'
        }
    })
    # _meta should only contain at most one entry per document i.e. the metadata for
    # that object.  So check that for the search criteria
    pipeline.append({'$match': {'_meta.0.{}'.format(key): value for key, value in meta.items()}})

    return pipeline
