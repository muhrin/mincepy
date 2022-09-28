# -*- coding: utf-8 -*-
"""Module containing record operations that can be performed sent to the archive to perform"""
import abc

from . import records

__all__ = "Operation", "Insert", "Update", "Delete"


class Operation(metaclass=abc.ABCMeta):
    """Base class for all operations"""

    @property
    @abc.abstractmethod
    def obj_id(self):
        """The is of the object being operated on"""

    @property
    @abc.abstractmethod
    def snapshot_id(self) -> records.SnapshotId:
        """The snapshot id of the object being operated on"""


class Insert(Operation):
    """Insert a record into the archive

    For use with :meth:`~mincepy.Archive.bulk_write`
    """

    def __init__(self, record: records.DataRecord):
        self._record = record

    @property
    def obj_id(self):
        return self._record.obj_id

    @property
    def snapshot_id(self) -> records.SnapshotId:
        return self._record.snapshot_id

    @property
    def record(self) -> records.DataRecord:
        return self._record


class Update(Operation):
    """Update a record currently in the archive.  This takes the snapshot id and a dictionary
    containing the fields to be updated.  The update operation behaves like a dict.update()"""

    def __init__(self, sid: records.SnapshotId, update: dict):
        diff = set(update.keys()) - set(records.DataRecord._fields)
        if diff:
            raise ValueError(f"Invalid keys found in the update operation: {diff}")

        self._sid = sid
        self._update = update

    @property
    def obj_id(self):
        return self._sid.obj_id

    @property
    def snapshot_id(self) -> records.SnapshotId:
        """The snapshot being updated"""
        return self._sid

    @property
    def update(self) -> dict:
        """The update that will be performed"""
        return self._update


class Delete(Operation):
    """Delete a record from the archive"""

    def __init__(self, sid: records.SnapshotId):
        self._sid = sid

    @property
    def obj_id(self):
        return self._sid.obj_id

    @property
    def snapshot_id(self) -> records.SnapshotId:
        """The snapshot being deleted"""
        return self._sid


class Merge(Operation):
    """Merge a record into the archive.  This could be:
        * An entirely new snapshot, i.e. the object id doesn't exist in the archive at all
        * A new version of a record, i.e. the object id does exist but this version is newer than any other
        * An old version of a record, i.e. the object id does exist but this version is older than the latest

    In any case the snapshot id should not exist in the database already.
    """

    def __init__(self, record: records.DataRecord):
        self._record = record

    @property
    def obj_id(self):
        return self._record.obj_id

    @property
    def snapshot_id(self) -> records.SnapshotId:
        return self._record.snapshot_id

    @property
    def record(self) -> records.DataRecord:
        return self._record
