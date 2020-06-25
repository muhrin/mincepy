import contextlib
import copy
from typing import MutableMapping, Any, List, Sequence, Optional, Dict, Set, Union, overload
import weakref

import deprecation

from . import archives
from . import exceptions
from . import operations
from . import records
from . import utils
from . import version as version_mod


class LiveObjects:
    """A container for storing live objects"""

    def __init__(self):
        # Live object -> data records
        self._records = \
            utils.WeakObjectIdDict()  # type: MutableMapping[object, archives.DataRecord]
        # Obj id -> (weak) object
        self._objects = weakref.WeakValueDictionary()  # type: MutableMapping[Any, object]

    def __str__(self):
        return "{} live".format(len(self._objects))

    def __contains__(self, item):
        """Determine if an object instance is in this live objects container"""
        return item in self._records

    def insert(self, obj, record: records.DataRecord):
        self._records[obj] = record
        self._objects[record.obj_id] = obj

    def update(self, live_objects: 'LiveObjects'):
        """Like a dictionary update, take the given live objects container and absorb it into
        ourselves overwriting any existing values and incorporating any new"""
        # pylint: disable=protected-access
        self._records.update(live_objects._records)
        self._objects.update(live_objects._objects)

    def remove(self, obj_id) -> Any:
        """Remove an object from the collection.  Returns the removed object.

        :raises: :class:`mincepy.NotFound` if the ID is not found
        """
        try:
            return self._records.pop(self._objects.pop(obj_id))
        except KeyError:
            raise exceptions.NotFound(obj_id)

    def get_record(self, obj) -> records.DataRecord:
        try:
            return self._records[obj]
        except KeyError:
            raise exceptions.NotFound("No live object found '{}'".format(obj))

    @overload
    def get_object(self, identifier: records.SnapshotId):  # pylint: disable=no-self-use
        ...

    @overload
    def get_object(self, identifier: Any):  # pylint: disable=no-self-use
        ...

    def get_object(self, identifier: Union[records.SnapshotId, Any]):
        """Get an object from the collection either by snapshot id or object id

        :raises: :class:`mincepy.NotFound` if the ID is not found
        """
        if isinstance(identifier, records.SnapshotId):
            for obj, record in self._records.items():
                if record.snapshot_id == identifier:
                    return obj
            raise exceptions.NotFound(identifier)

        # Must be an object id
        try:
            return self._objects[identifier]
        except KeyError:
            raise exceptions.NotFound("No live object with id '{}'".format(identifier))

    def get_snapshot_id(self, obj) -> records.SnapshotId:
        """Given an object, get the snapshot id"""
        try:
            return self._records[obj].snapshot_id
        except KeyError:
            raise exceptions.NotFound(obj)


class RollbackTransaction(Exception):
    pass


class Transaction:
    """A transaction object for keeping track of actions performed in a transaction that will be
    committed at the end.

    A transaction has no interaction with the database and simply stores a transient state.  For
    this reason things like constrains are not enforced and database queries will no reflect
    mutations performed within the transaction (only upon commit).
    """

    # pylint: disable=too-many-public-methods

    @deprecation.deprecated(deprecated_in="0.14.4",
                            removed_in="0.16.0",
                            current_version=version_mod.__version__,
                            details="Use get_snapshot_id_for_live_object() instead")
    def get_reference_for_live_object(self, obj) -> records.SnapshotId:
        return self.get_snapshot_id_for_live_object(obj)

    @deprecation.deprecated(deprecated_in="0.14.4",
                            removed_in="0.16.0",
                            current_version=version_mod.__version__,
                            details="Use get_live_object_from_snapshot_id() instead")
    def get_live_object_from_reference(self, snapshot_id: records.SnapshotId):
        return self.get_live_object(snapshot_id)

    @deprecation.deprecated(deprecated_in="0.14.4",
                            removed_in="0.16.0",
                            current_version=version_mod.__version__,
                            details="Use get_live_object() instead")
    def get_live_object_from_snapshot_id(self, snapshot_id: records.SnapshotId):
        return self.get_live_object(snapshot_id)

    def __init__(self):
        # Records staged for saving to the archive
        self._staged = []  # type: List[operations.Operation]

        self._deleted = set()  # A set of deleted obj ids

        self._live_objects = LiveObjects()
        # Snapshot id -> obj for objects currently being saved
        self._in_progress_cache = {}  # type: Dict[records.SnapshotId, object]

        # Snapshots: snapshot id -> obj
        self._snapshots = {}  # type: Dict[records.SnapshotId, Any]
        # Maps from object id -> metadata dictionary
        self._metas = {}  # type: Dict[Any, dict]

    def __str__(self):
        return "{}, {} live ref(s), {} snapshots, {} staged".format(self._live_objects,
                                                                    len(self._in_progress_cache),
                                                                    len(self._snapshots),
                                                                    len(self._staged))

    @property
    def live_objects(self) -> LiveObjects:
        return self._live_objects

    @property
    def deleted(self) -> Set:
        return self._deleted

    @property
    def snapshots(self):
        return self._snapshots

    @property
    def metas(self) -> dict:
        return self._metas

    # region LiveObjects

    def insert_live_object(self, obj, record: records.DataRecord):
        """Insert a live object along with an up-to-date record into the transaction"""
        if self.is_deleted(record.obj_id):
            raise ValueError("Object with id '{}' has already been deleted!".format(record.obj_id))

        sid = record.snapshot_id
        if sid in self._in_progress_cache:
            assert self._in_progress_cache[sid] is obj

        self._live_objects.insert(obj, record)

    @contextlib.contextmanager
    def prepare_for_saving(self, snapshot_id: records.SnapshotId, obj):
        """Insert a snapshot reference for an object into the transaction"""
        self._in_progress_cache[snapshot_id] = obj
        try:
            yield
        except Exception:  # Need this for the 'else' pylint: disable=try-except-raise
            raise
        else:
            if self._live_objects.get_snapshot_id(obj) != snapshot_id:
                raise RuntimeError(
                    "Problem saving object with snapshot id '{}', "
                    "the snapshot id saved does not match that given".format(snapshot_id))
        finally:
            del self._in_progress_cache[snapshot_id]

    @overload
    def get_live_object(self, identifier: records.SnapshotId) -> object:  # pylint: disable=no-self-use
        ...

    @overload
    def get_live_object(self, identifier: Any) -> object:  # pylint: disable=no-self-use
        ...

    def get_live_object(self, identifier: Union[records.SnapshotId, Any]) -> object:
        if isinstance(identifier, records.SnapshotId):
            self._ensure_not_deleted(identifier.obj_id)
            try:
                return self._in_progress_cache[identifier]
            except KeyError:
                return self._live_objects.get_object(identifier)

        # Must be an object id
        self._ensure_not_deleted(identifier)
        return self._live_objects.get_object(identifier)

    def get_record_for_live_object(self, obj) -> records.DataRecord:
        return self._live_objects.get_record(obj)

    def get_snapshot_id_for_live_object(self, obj) -> records.SnapshotId:
        for ref, cached_obj in self._in_progress_cache.items():
            if obj is cached_obj:
                return ref

        return self._live_objects.get_snapshot_id(obj)

    def delete(self, obj_id):
        """Mark an object as deleted"""
        try:
            self._live_objects.remove(obj_id)
        except exceptions.NotFound:
            pass

        self.set_meta(obj_id, None)
        self._deleted.add(obj_id)

    def is_deleted(self, obj_id):
        return obj_id in self.deleted

    # endregion LiveObjects

    # region meta

    def set_meta(self, obj_id, meta: Optional[dict]):
        """Set an object's metadata.  Can pass None to unset."""
        self._ensure_not_deleted(obj_id)
        self._metas[obj_id] = copy.deepcopy(meta)

    def get_meta(self, obj_id) -> dict:
        """Get an object's metadata.
        If the returned metadata is None then it means the user has set this metadata to be removed.

        :raise exceptions.NotFound: if not metadata information for the object is contained in this
            transaction.
        """
        self._ensure_not_deleted(obj_id)
        try:
            return self._metas[obj_id]
        except KeyError:
            raise exceptions.NotFound

    # endregion

    def insert_snapshot(self, obj, snapshot_id):
        self._snapshots[snapshot_id] = obj

    def get_snapshot(self, snapshot_id):
        try:
            return self._snapshots[snapshot_id]
        except KeyError:
            raise exceptions.NotFound("No snapshot with id '{}' found".format(snapshot_id))

    def stage(self, op: operations.Operation):  # pylint: disable=invalid-name
        """Stage an operation to be carried out on completion of this transaction"""
        self._staged.append(op)

    @property
    def staged(self) -> Sequence[operations.Operation]:
        """The list of records that were staged during this transaction"""
        return self._staged

    @contextlib.contextmanager
    def nested(self):
        nested = NestedTransaction(self)
        try:
            yield nested
        except RollbackTransaction:
            pass
        else:
            # Update our transaction with the nested
            self._update(nested)

    def _update(self, transaction: 'Transaction'):
        """Absorb a nested transaction into this one, done at the end of a nested context"""
        # pylint: disable=protected-access
        self._live_objects.update(transaction.live_objects)
        self._snapshots.update(transaction.snapshots)
        self._in_progress_cache.update(transaction._in_progress_cache)
        self._staged.extend(transaction.staged)
        self._metas.update(transaction.metas)
        for deleted in transaction.deleted:
            self.delete(deleted)

    @staticmethod
    def rollback():
        raise RollbackTransaction

    def _ensure_not_deleted(self, obj_id):
        """Make sure that an object id has not been deleted in this transaction.  Raises an
        ObjectDeleted exception if so."""
        if self.is_deleted(obj_id):
            raise exceptions.ObjectDeleted(obj_id)


class NestedTransaction(Transaction):

    def __init__(self, parent: Transaction):
        super(NestedTransaction, self).__init__()
        self._parent = parent

    def __str__(self):
        return "{} (parent: {})".format(super().__str__(), self._parent)

    def get_live_object(self, identifier) -> object:
        try:
            return super().get_live_object(identifier)
        except exceptions.NotFound:
            return self._parent.get_live_object(identifier)

    def get_snapshot_id_for_live_object(self, obj):
        try:
            return super().get_snapshot_id_for_live_object(obj)
        except exceptions.NotFound:
            return self._parent.get_snapshot_id_for_live_object(obj)

    def get_record_for_live_object(self, obj):
        try:
            return super().get_record_for_live_object(obj)
        except exceptions.NotFound:
            return self._parent.get_record_for_live_object(obj)

    def get_snapshot(self, snapshot_id):
        try:
            return super(NestedTransaction, self).get_snapshot(snapshot_id)
        except exceptions.NotFound:
            return self._parent.get_snapshot(snapshot_id)

    def get_meta(self, obj_id) -> dict:
        try:
            return super(NestedTransaction, self).get_meta(obj_id)
        except exceptions.NotFound:
            return self._parent.get_meta(obj_id)

    def is_deleted(self, obj_id):
        if obj_id in self.deleted:
            return True

        return self._parent.is_deleted(obj_id)
