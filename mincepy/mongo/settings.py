from typing import Optional

import pymongo.database

from . import db


def get_settings(database: pymongo.database.Database, name: str = db.GLOBAL_SETTINGS) -> \
        Optional[dict]:
    return database[db.SETTINGS_COLLECTION].find_one(name)


def set_settings(database: pymongo.database.Database,
                 settings: Optional[dict],
                 name: str = db.GLOBAL_SETTINGS):
    coll = database[db.SETTINGS_COLLECTION]

    if settings is None:
        coll.delete_one({'_id': name})
    else:
        coll.replace_one({'_id': name}, settings, upsert=True)
