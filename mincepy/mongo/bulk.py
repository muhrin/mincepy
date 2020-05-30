import functools

import pymongo

import mincepy
from mincepy import q
from . import db

# To present 'op' being flagged
# pylint: disable=invalid-name


@functools.singledispatch
def to_mongo_op(op: mincepy.operations.Operation):
    """Convert a mincepy operation to a mongodb one.  Returns a tuple of the data operation and
    history operation that need to be bulk written"""
    raise NotImplementedError


@to_mongo_op.register(mincepy.operations.Insert)
def _(op: mincepy.operations.Insert):
    """Insert"""
    record = op.record
    document = db.to_document(record)
    document['_id'] = record.obj_id

    if record.is_deleted_record():
        data_op = pymongo.operations.DeleteOne(filter={
            db.OBJ_ID: record.obj_id,
            db.VERSION: q.lt_(record.version)
        },)
    else:
        data_op = pymongo.operations.ReplaceOne(filter={
            db.OBJ_ID: record.obj_id,
            db.VERSION: q.lt_(record.version)
        },
                                                replacement=document.copy(),
                                                upsert=True)

    # History uses the sid as the document id
    document['_id'] = str(record.snapshot_id)
    history_op = pymongo.operations.InsertOne(document)

    return data_op, history_op


@to_mongo_op.register(mincepy.operations.Update)
def _(op: mincepy.operations.Update):
    """Update"""
    sid = op.snapshot_id
    update = db.to_document(op.update)
    update = {'$set': update}

    # It's fine if either (or both) of these fail to find anything to update
    data_op = pymongo.operations.UpdateOne(filter={
        db.OBJ_ID: sid.obj_id,
        db.VERSION: sid.version
    },
                                           update=update)
    history_op = pymongo.operations.UpdateOne(filter={'_id': str(sid)}, update=update)

    return data_op, history_op


@to_mongo_op.register(mincepy.operations.Delete)
def _(op: mincepy.operations.Delete):
    """Delete"""
    sid = op.snapshot_id

    # It's fine if either (or both) of these fail to find anything to update
    data_op = pymongo.operations.DeleteOne(filter={db.OBJ_ID: sid.obj_id},)
    history_op = pymongo.operations.DeleteOne(filter={'_id': str(sid)},)

    return data_op, history_op
