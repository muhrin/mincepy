import logging
from typing import Callable

from mincepy import archives, frontend, operations, result_types
import mincepy.records as records_

__all__ = ("SnapshotsCollection",)

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class SnapshotsCollection(frontend.ObjectCollection):
    def __init__(self, historian, archive_collection: archives.Collection):
        super().__init__(
            historian,
            archive_collection,
            record_factory=lambda record_dict: SnapshotLoadableRecord(
                record_dict, historian.load_snapshot_from_record
            ),
            obj_loader=historian.load_snapshot_from_record,
        )

    def purge(self, deleted=True, dry_run=True) -> result_types.PurgeResult:
        """Function to delete various unused objects from the database.

        This function cannot and will never delete data the is currently in use."""
        if deleted:
            # First find all the object ids of those that have been deleted
            # pylint: disable=protected-access
            res = self.records.find(records_.DataRecord.state == records_.DELETED)._project(
                records_.OBJ_ID
            )
            obj_ids = [entry[records_.OBJ_ID] for entry in res]  # DB HIT

            logging.debug("Found %i objects that have been deleted", len(obj_ids))

            # Need the object id and version to create the snapshot ids
            # pylint: disable=protected-access
            res = self.records.find(records_.DataRecord.obj_id.in_(*obj_ids))._project(
                records_.OBJ_ID, records_.VERSION
            )
            snapshot_ids = [records_.SnapshotId(**entry) for entry in res]  # DB HIT

            logging.info(
                "Found %i objects with %i snapshots that are deleted, removing.",
                len(obj_ids),
                len(snapshot_ids),
            )

            if snapshot_ids and not dry_run:
                # Commit the changes
                self._historian.archive.bulk_write([operations.Delete(sid) for sid in snapshot_ids])
                logging.info("Deleted %i snapshots", len(snapshot_ids))

        return result_types.PurgeResult(set(snapshot_ids))


class SnapshotLoadableRecord(records_.DataRecord):
    __slots__ = ()

    def __new__(cls, record_dict: dict, snapshot_loader: Callable[[records_.DataRecord], object]):
        loadable = super().__new__(cls, **record_dict)
        loadable._snapshot_loader = snapshot_loader
        return loadable

    def load(self) -> object:
        return self._snapshot_loader(self)
