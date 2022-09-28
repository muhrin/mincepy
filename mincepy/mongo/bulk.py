# -*- coding: utf-8 -*-
import functools

import pymongo

from mincepy import operations
from mincepy import q

from . import db

# To prevent 'op' being flagged
# pylint: disable=invalid-name


@functools.singledispatch
def to_mongo_op(op: operations.Operation):
    """Convert a mincepy operation to a mongodb one.  Returns a tuple of the data operation and
    history operation that need to be bulk written"""
    raise NotImplementedError


@to_mongo_op.register(operations.Insert)
def _(op: operations.Insert):
    """Insert"""
    record = op.record
    document = db.to_document(record)
    document["_id"] = record.obj_id

    if record.is_deleted_record():
        data_op = pymongo.operations.DeleteOne(
            filter={db.OBJ_ID: record.obj_id, db.VERSION: q.lt_(record.version)},
        )
    else:
        data_op = pymongo.operations.UpdateOne(
            filter={db.OBJ_ID: record.obj_id, db.VERSION: q.lt_(record.version)},
            update={"$set": document.copy()},
            upsert=True,
        )

    # History uses the sid as the document id
    document["_id"] = str(record.snapshot_id)
    history_op = pymongo.operations.InsertOne(document)

    return [data_op], [history_op]


@to_mongo_op.register(operations.Update)
def _(op: operations.Update):
    """Update"""
    sid = op.snapshot_id
    update = db.to_document(op.update)
    update = {"$set": update}

    # It's fine if either (or both) of these fail to find anything to update
    data_op = pymongo.operations.UpdateOne(
        filter={db.OBJ_ID: sid.obj_id, db.VERSION: sid.version}, update=update
    )
    history_op = pymongo.operations.UpdateOne(filter={"_id": str(sid)}, update=update)

    return [data_op], [history_op]


@to_mongo_op.register(operations.Delete)
def _(op: operations.Delete):
    """Delete"""
    sid = op.snapshot_id

    # It's fine if either (or both) of these fail to find anything to update
    data_op = pymongo.operations.DeleteOne(
        filter={db.OBJ_ID: sid.obj_id, db.VERSION: sid.version},
    )
    history_op = pymongo.operations.DeleteOne(
        filter={"_id": str(sid)},
    )

    return [data_op], [history_op]


@to_mongo_op.register(operations.Merge)
def _(op: operations.Merge):
    """Merge"""
    record = op.record
    document = db.to_document(record)
    document["_id"] = record.obj_id

    data_ops = []

    if record.is_deleted_record():
        # Delete record, so expunge the record from the current objects
        data_ops.append(
            pymongo.operations.DeleteOne(
                filter={db.OBJ_ID: record.obj_id, db.VERSION: q.lt_(record.version)},
            )
        )
    else:
        data_ops.append(
            pymongo.operations.DeleteOne(
                filter={db.OBJ_ID: record.obj_id, db.VERSION: q.lt_(record.version)}
            )
        )
        data_ops.append(
            pymongo.operations.UpdateOne(
                filter={db.OBJ_ID: record.obj_id},
                update={"$setOnInsert": document.copy()},
                upsert=True,
            )
        )

    # History uses the sid as the document id
    document["_id"] = str(record.snapshot_id)
    history_op = pymongo.operations.InsertOne(document)

    return data_ops, [history_op]
