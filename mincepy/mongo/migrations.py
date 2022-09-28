# -*- coding: utf-8 -*-
import pymongo.database
import tqdm

from . import migrate
from .aggregation import eq_, and_


class Initial(migrate.Migration):
    NAME = "initial-setup"
    VERSION = 0

    def upgrade(self, database: pymongo.database.Database):
        collections = database.list_collection_names()
        if "data" not in collections:
            database.create_collection("data")
        if "meta" not in collections:
            database.create_collection("meta")
        super().upgrade(database)


class CollectionsSplit(migrate.Migration):
    """
    This migration makes the following changes wrt the initial database layout:

    Data collection
    ---------------

    `_id` is now the object id
    All historical and deleted records are removed.

    History collection
    ------------------

    Contains what data used to contain (including currently live objects).
    The _id field is the sref string i.e. <obj_id>#<version>

    References collection
    ---------------------

    Has been changed because of the format change of history ids.  This can
    simply be dropped as it will be lazily re-created if needed.

    Meta collection
    ---------------

    Unchanged.

    """

    NAME = "split-data-collection"
    VERSION = 1
    PREVIOUS = Initial

    def upgrade(self, database: pymongo.database.Database):
        history_collection = "history"
        data_collection = "data"

        old_data = database[data_collection]
        history = database[history_collection]

        if old_data.count_documents({}) != 0:
            # Transform the entries to the new history format
            for entry in old_data.find():
                obj_id = entry["_id"]["oid"]
                version = entry["_id"]["v"]
                entry["_id"] = f"{obj_id}#{version}"
                entry["obj_id"] = obj_id
                entry["ver"] = version
                history.insert_one(entry)

        # Ok, now rename, copy over what we need and drop
        old_data.rename("old_data")
        new_data = database[data_collection]

        if history.count_documents({}) != 0:
            pipeline = self.pipeline_latest_version(history_collection)
            pipeline.append({"$match": {"state": {"$ne": "!!deleted"}}})

            for entry in history.aggregate(pipeline):
                entry["_id"] = entry["obj_id"]
                new_data.insert_one(entry)

        # Finally drop
        database.drop_collection("old_data")

        # This will be lazily re-created
        database.drop_collection("references")

        super().upgrade(database)

    @staticmethod
    def pipeline_latest_version(collection: str) -> list:
        """Returns a pipeline that will take the incoming data record documents and for each one
        find the latest version."""
        pipeline = []
        pipeline.extend(
            [
                # Group by object id the maximum version
                {"$group": {"_id": "$obj_id", "max_ver": {"$max": "$ver"}}},
                # Then do a lookup against the same collection to get the records
                {
                    "$lookup": {
                        "from": collection,
                        "let": {"obj_id": "$_id", "max_ver": "$max_ver"},
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": and_(
                                        eq_(
                                            "$obj_id", "$$obj_id"
                                        ),  # Match object id and version
                                        eq_("$ver", "$$max_ver"),
                                    ),
                                }
                            }
                        ],
                        "as": "latest",
                    }
                },
                # Now unwind and promote the 'latest' field
                {"$unwind": {"path": "$latest"}},
                {"$replaceRoot": {"newRoot": "$latest"}},
            ]
        )

        return pipeline


class MergeMeta(migrate.Migration):
    """
    Merge metadata into directly into the collection of data records.
    """

    NAME = "merge-meta"
    VERSION = 2
    PREVIOUS = CollectionsSplit

    def upgrade(self, database: pymongo.database.Database):
        data_coll_name = "data"  # Name of the data collection
        meta_coll_name = "meta"  # Name of the metadata collection
        meta_field = "meta"
        new_data_coll_name = "new_data"

        colls = database.list_collection_names()

        # Guard against one (or more) of the collections we need not existing
        if data_coll_name in colls and meta_coll_name in colls:
            meta_coll = database[meta_coll_name]
            data_coll = database[data_coll_name]

            # Join with meta and place in the new data collection
            data_coll.aggregate(
                [
                    {
                        "$lookup": {
                            "from": "meta",
                            "localField": "obj_id",
                            "foreignField": "_id",
                            "as": meta_field,
                        }
                    },
                    {
                        "$addFields": {
                            meta_field: {"$arrayElemAt": [f"${meta_field}", 0]}
                        }
                    },
                    {
                        "$project": {f"{meta_field}._id": 0}
                    },  # Exclude the old '_id' field from meta entries
                    {"$out": new_data_coll_name},
                ]
            )

            new_data_coll = database[new_data_coll_name]

            # Copy over the indexes
            for name, index_info in tqdm.tqdm(
                meta_coll.index_information().items(), desc="Copying indexes"
            ):
                keys = index_info["key"]
                if len(keys) == 1 and keys[0][0] == "_id":
                    continue

                keys = [(f"{meta_field}.{key[0]}", key[1]) for key in keys]

                del index_info["ns"]
                del index_info["v"]
                del index_info["key"]
                new_data_coll.create_index(keys, name=name, **index_info)

            # Drop the old one data
            database.drop_collection(data_coll)
            # Rename the new
            new_data_coll.rename(data_coll_name)
            # Drop the old meta
            database.drop_collection(meta_coll_name)

        super().upgrade(database)


LATEST = MergeMeta
