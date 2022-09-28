# -*- coding: utf-8 -*-
"""Specific tests for the MongoDB archive"""

import bson
import gridfs
import pytest

import mincepy
from mincepy import testing
import mincepy.mongo.migrations


def test_schema_version(historian: mincepy.Historian):
    archive: mincepy.mongo.MongoArchive = historian.archive
    assert archive.schema_version == mincepy.mongo.migrations.LATEST.VERSION


def test_get_gridfs_bucket(historian: mincepy.Historian):
    archive: mincepy.mongo.MongoArchive = historian.archive
    assert isinstance(archive.get_gridfs_bucket(), gridfs.GridFSBucket)


def test_load(historian: mincepy.Historian):
    archive: mincepy.mongo.MongoArchive = historian.archive
    with pytest.raises(TypeError):
        archive.load("invalid")

    with pytest.raises(mincepy.NotFound):
        archive.load(mincepy.SnapshotId(bson.ObjectId(), 0))


def test_distinct(historian: mincepy.Historian):
    archive: mincepy.mongo.MongoArchive = historian.archive
    testing.Car(colour="blue").save()
    testing.Car(colour="red").save()

    assert set(archive.distinct("state.colour")) == {"red", "blue"}
    assert set(archive.distinct("state.colour", {"state": {"colour": "red"}})) == {
        "red"
    }
