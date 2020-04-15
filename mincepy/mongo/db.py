"""This module contains the names of the keys in the various collections used by the mongo
archive and methods to convert mincepy types to mongo collection entries and back"""
from bidict import bidict

import mincepy.records

# region Data collection
OBJ_ID = '_id.oid'
VERSION = '_id.v'
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


def to_record(entry) -> mincepy.DataRecord:
    """Convert a MongoDB data collection entry to a DataRecord"""
    record_dict = mincepy.DataRecord.defaults()

    record_dict[mincepy.OBJ_ID] = entry['_id']['oid']
    record_dict[mincepy.VERSION] = entry['_id']['v']

    # Invert our mapping of keys back to the data record property names and update over any
    # defaults
    record_dict.update(
        {recordkey: entry[dbkey] for recordkey, dbkey in KEY_MAP.items() if dbkey in entry})

    return mincepy.DataRecord(**record_dict)


def to_entry(record: mincepy.DataRecord) -> dict:
    """Convert a DataRecord to a MongoDB data collection entry"""
    defaults = mincepy.DataRecord.defaults()
    entry = {'_id': to_id_dict(record.get_reference())}
    for key, item in record._asdict().items():
        db_key = KEY_MAP[key]
        if db_key in (OBJ_ID, VERSION):
            # Already dealt with above
            continue

        # Exclude entries that have the default value
        if not (key in defaults and defaults[key] == item):
            entry[db_key] = item

    return entry


def remap(record_dict: dict) -> dict:
    """Given a dictionary return a new dictionary with the key names that we use"""
    remapped = {}
    for key, value in record_dict.items():
        split_key = key.split('.')
        base = KEY_MAP[split_key[0]]
        split_key[0] = base
        remapped['.'.join(split_key)] = value
    return remapped


def to_id_dict(sref: mincepy.SnapshotRef) -> dict:
    return {'oid': sref.obj_id, 'v': sref.version}


def to_sref(id_dict: dict):
    return mincepy.SnapshotRef(id_dict['oid'], id_dict['v'])


# endregion
