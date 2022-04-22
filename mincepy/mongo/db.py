# -*- coding: utf-8 -*-
"""This module contains the names of the keys in the various collections used by the mongo
archive and methods to convert mincepy types to mongo collection entries and back"""
import functools

import pymongo.collection
import pymongo.errors
from bidict import bidict
import bson

import mincepy.records
from mincepy import q

SETTINGS_COLLECTION = 'settings'
GLOBAL_SETTINGS = 'global'

# region Data collection
OBJ_ID = 'obj_id'
VERSION = 'ver'
TYPE_ID = mincepy.records.TYPE_ID
CREATION_TIME = 'ctime'
STATE = 'state'
STATE_TYPES = 'state_types'
SNAPSHOT_HASH = 'hash'
SNAPSHOT_TIME = 'stime'
EXTRAS = mincepy.records.EXTRAS
REFERENCES = 'refs'

# Here we map the data record property names onto ones in our entry format.
# If a record property doesn't appear here it means the name says the same
KEY_MAP = bidict({
    mincepy.records.OBJ_ID: OBJ_ID,
    mincepy.records.TYPE_ID: TYPE_ID,
    mincepy.records.CREATION_TIME: CREATION_TIME,
    mincepy.records.VERSION: VERSION,
    mincepy.records.STATE: STATE,
    mincepy.records.STATE_TYPES: STATE_TYPES,
    mincepy.records.SNAPSHOT_HASH: SNAPSHOT_HASH,
    mincepy.records.SNAPSHOT_TIME: SNAPSHOT_TIME,
    mincepy.records.EXTRAS: EXTRAS,
})

# endregion


def to_record(entry) -> mincepy.DataRecord:
    """Convert a MongoDB data collection entry to a DataRecord"""
    record_dict = mincepy.DataRecord.defaults()

    record_dict[mincepy.OBJ_ID] = entry[OBJ_ID]
    record_dict[mincepy.VERSION] = entry[VERSION]

    # Invert our mapping of keys back to the data record property names and update over any
    # defaults
    record_dict.update(
        {recordkey: entry[dbkey] for recordkey, dbkey in KEY_MAP.items() if dbkey in entry})

    return mincepy.DataRecord(**record_dict)


@functools.singledispatch
def to_document(record, exclude_defaults=False) -> dict:
    """Convert mincepy record information to a MongoDB document.  Optionally exclude entries that
    have the same value as the default"""
    raise TypeError(record.__class__)


@to_document.register(mincepy.DataRecord)
def _(record: mincepy.DataRecord, exclude_defaults=False) -> dict:
    """Convert a DataRecord to a MongoDB document with our keys"""
    defaults = mincepy.DataRecord.defaults()
    entry = {}
    for key, item in record.__dict__.items():
        db_key = KEY_MAP[key]  # pylint: disable=unsubscriptable-object
        # Exclude entries that have the default value
        if not (exclude_defaults and key in defaults and defaults[key] == item):
            entry[db_key] = item

    return entry


@to_document.register(dict)
def _(record: dict, exclude_defaults=False) -> dict:
    """Convert a dictionary containing record keys to a MongoDB document with our keys"""
    defaults = mincepy.DataRecord.defaults()
    entry = {}
    for key, item in record.items():
        db_key = KEY_MAP[key]  # pylint: disable=unsubscriptable-object
        # Exclude entries that have the default value
        if not (exclude_defaults and key in defaults and defaults[key] == item):
            entry[db_key] = item

    return entry


def remap(record_dict: dict) -> dict:
    """Given a dictionary return a new dictionary with the key names that we use"""
    remapped = {}
    for key, value in record_dict.items():
        remapped[remap_key(key)] = value
    return remapped


def remap_back(entry_dict: dict) -> dict:
    remapped = {}
    inverse_map = KEY_MAP.inverse
    for key, value in entry_dict.items():
        if key in inverse_map:
            remapped[inverse_map[key]] = value
    return remapped


def remap_key(key: str) -> str:
    """Given a key remap it to the names that we use, even if it as a path e.g. state.colour"""
    split_key = key.split('.')
    base = KEY_MAP[split_key[0]]  # pylint: disable=unsubscriptable-object
    split_key[0] = base
    return '.'.join(split_key)


def to_id_dict(sid: mincepy.SnapshotId) -> dict:
    return {OBJ_ID: sid.obj_id, VERSION: sid.version}


def sid_from_dict(record: dict):
    return mincepy.SnapshotId(record[OBJ_ID], record[VERSION])


def sid_from_str(sid_str: str):
    parts = sid_str.split('#')
    return mincepy.SnapshotId(bson.ObjectId(parts[0]), int(parts[1]))


def safe_bulk_delete(collection: pymongo.collection.Collection, ids, id_key='_id'):
    """Sometimes when you want to delete a bunch of documents using an identifier the 'delete document' itself exceeds
    the 16MB Mongo limit.  This function will catch such cases and break up the command into suitably batches"""
    ids = list(set(ids))  # No needs to repeat ourselves
    try:
        collection.delete_many({id_key: q.in_(*ids)})
    except pymongo.errors.DocumentTooLarge:
        # Use bulk operation instead
        # Note, this could be spead up further by batching the deletes but for now it's not worth it
        bulk_ops = [pymongo.DeleteOne({id_key: entry_id}) for entry_id in ids]
        collection.bulk_write(bulk_ops)
