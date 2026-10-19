"""Tests for the MongoDB schema migrations"""

import bson
import pymongo
import pytest

import mincepy
from mincepy.mongo import migrate, migrations, settings


@pytest.fixture
def raw_database(mongodb_archive):
    """An empty database on the same server as the test archive"""
    client = mongodb_archive.database.client
    name = f"{mongodb_archive.database.name}-migrations"
    client.drop_database(name)
    yield client[name]
    client.drop_database(name)


def _create_v0_database(database):
    """Create a database using the original (version 0) layout"""
    car_id = bson.ObjectId()
    deleted_id = bson.ObjectId()

    database.create_collection("data")
    database.create_collection("meta")
    data = database["data"]
    # In v0 the data collection contains all snapshots, keyed by object id and version
    for version, colour in enumerate(("red", "blue")):
        data.insert_one(
            {"_id": {"oid": car_id, "v": version}, "type_id": "car", "state": {"colour": colour}}
        )
    data.insert_one({"_id": {"oid": deleted_id, "v": 0}, "type_id": "car", "state": {}})
    data.insert_one({"_id": {"oid": deleted_id, "v": 1}, "type_id": "car", "state": "!!deleted"})

    # Metadata lived in its own collection
    meta = database["meta"]
    meta.insert_one({"_id": car_id, "reg": "AB12"})
    meta.create_index([("reg", pymongo.ASCENDING)], unique=True, name="reg_unique")

    migrations.Initial().upgrade(database)
    return car_id, deleted_id


def test_upgrade_from_v0(raw_database):
    car_id, deleted_id = _create_v0_database(raw_database)
    assert migrate.get_version(raw_database) == 0

    applied = migrate.ensure_up_to_date(raw_database, migrations.LATEST)
    assert applied == 2
    assert migrate.get_version(raw_database) == migrations.LATEST.VERSION

    # History now contains every snapshot, keyed by the snapshot id string
    history_ids = set(raw_database["history"].distinct("_id"))
    assert history_ids == {
        f"{car_id}#0",
        f"{car_id}#1",
        f"{deleted_id}#0",
        f"{deleted_id}#1",
    }

    # Data only contains the latest, live, version of each object with the metadata merged in
    data = list(raw_database["data"].find())
    assert len(data) == 1
    assert data[0]["_id"] == car_id
    assert data[0]["state"] == {"colour": "blue"}
    assert data[0]["meta"] == {"reg": "AB12"}
    assert "meta" not in raw_database.list_collection_names()

    # The metadata index was carried across
    indexes = raw_database["data"].index_information()
    assert "reg_unique" in indexes
    assert indexes["reg_unique"]["key"] == [("meta.reg", pymongo.ASCENDING)]
    assert indexes["reg_unique"]["unique"]

    # Running again should do nothing
    assert migrate.ensure_up_to_date(raw_database, migrations.LATEST) == 0


def test_newer_database_is_rejected(raw_database):
    settings.set_settings(raw_database, {migrate.VERSION: migrations.LATEST.VERSION + 1})
    with pytest.raises(migrate.MigrationError):
        migrate.ensure_up_to_date(raw_database, migrations.LATEST)


def test_migration_required(raw_database):
    manager = migrate.MigrationManager(migrations.LATEST)
    assert manager.migration_required(raw_database)
    manager.migrate(raw_database)
    assert not manager.migration_required(raw_database)
    assert settings.get_settings(raw_database)[migrate.MIGRATIONS] == [
        migration.NAME for migration in reversed(manager.get_migration_sequence())
    ]


def test_archive_on_migrated_database(raw_database, mongodb_archive):
    """A database created with the old layout should be usable once connected"""
    car_id, _ = _create_v0_database(raw_database)
    archive = mincepy.mongo.MongoArchive(raw_database)
    assert archive.schema_version == migrations.LATEST.VERSION
    assert archive.meta_get(car_id) == {"reg": "AB12"}
