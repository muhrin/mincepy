"""Module containing record operations that can be performed sent to the archive to perform"""
import abc

from . import records

__all__ = 'Operation', 'Insert', 'Update', 'Delete'


class Operation(metaclass=abc.ABCMeta):  # pylint: disable=too-few-public-methods
    """Base class for all operations"""


class Insert(Operation):
    """Insert a record into the archive

    For use with :meth:`~mincepy.Archive.bulk_write`
    """

    def __init__(self, record: records.DataRecord):
        self._record = record

    @property
    def record(self) -> records.DataRecord:
        return self._record


class Update(Operation):
    """Update a record currently in the archive.  This takes the snapshot id and a dictionary
    containing the fields to be updated.  The update operation behaves like a dict.update()"""

    def __init__(self, sid: records.SnapshotId, update: dict):
        diff = set(update.keys()) - set(records.DataRecord._fields())
        if diff:
            raise ValueError("Invalid keys found in the update operation: {}".format(diff))

        self._sid = sid
        self._update = update

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
    def snapshot_id(self) -> records.SnapshotId:
        """The snapshot being deleted"""
        return self._sid
