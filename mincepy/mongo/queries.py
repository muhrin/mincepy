def and_(*conditions) -> dict:
    """Helper that produces mongo query dict for AND of multiple conditions"""
    if len(conditions) > 1:
        return {'$and': list(conditions)}

    return conditions[0]


def eq_(one, other) -> dict:
    """Helper that produces mongo query dict for to items being equal"""
    return {'$eq': [one, other]}


def in_(*possibilities) -> dict:
    """Helper that produces mongo query dict for items being one of"""
    if len(possibilities) == 1:
        return possibilities[0]

    return {'$in': list(possibilities)}


def ne_(value) -> dict:
    """Not equal to value"""
    return {'$ne': value}


def exists_(key) -> dict:
    """Return condition for the existence of a key"""
    return {key: {'$exists': True}}


def pipeline_latest_version(data_collection: str) -> list:
    """Returns a pipeline that will take the incoming data record documents and for each one find
    the latest version."""
    pipeline = []
    pipeline.extend([
        # Group by object id the maximum version
        {
            '$group': {
                '_id': "$obj_id",
                'ver': {
                    '$max': '$ver'
                }
            }
        },
        # Then do a lookup against the same collection to get the records
        {
            '$lookup': {
                'from': data_collection,
                'let': {
                    'obj_id': '$_id',
                    'ver': '$ver'
                },
                'pipeline': [{
                    '$match': {
                        '$expr': and_(eq_('$obj_id', '$$obj_id'), eq_('$ver', '$$ver')),
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
