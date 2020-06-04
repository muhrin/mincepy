import contextlib
import copy
from typing import MutableMapping, Any, List, Sequence, Optional, Dict, Set
import weakref

from . import archives
from . import exceptions
from . import operations
from . import records
from . import utils


class LiveObjects:
    """A container for storing live objects"""

    def __init__(self):
        # Live object -> data records
        self._records = utils.WeakObjectIdDict()  # type: MutableMapping[Any, archives.DataRecord]
        # Obj id -> (weak) object
        self._objects = weakref.WeakValueDictionary()  # type: MutableMapping[Any, Any]

    def __str__(self):
        return "{} live".format(len(self._objects))

    def __contains__(self, item):
        """Determine if an object instance is in this live objects container"""
        return item in self._records

    def insert(self, obj, record):
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

    def get_object(self, obj_id):
        """Get an object from the transaction by id.

        :raises: :class:`mincepy.NotFound` if the ID is not found
        """
        try:
            return self._objects[obj_id]
        except KeyError:
            raise exceptions.NotFound("No live object with id '{}'".format(obj_id))

    def get_snapshot_id(self, obj) -> records.SnapshotId:
        """Given an object, get the snapshot reference"""
        for stored, record in self._records:
            if obj is stored:
                return record.snapshot_id

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

    def __init__(self):
        # Records staged for saving to the archive
        self._staged = []  # type: List[operations.Operation]

        self._deleted = set()  # A set of deleted obj ids

        self._live_objects = LiveObjects()
        # Ref -> obj
        self._live_object_references = {}

        # Snapshots: snapshot id -> obj
        self._snapshots = {}  # type: Dict[records.SnapshotId, Any]
        # Maps from object id -> metadata dictionary
        self._metas = {}  # type: Dict[Any, dict]

    def __str__(self):
        return "{}, {} live ref(s), {} snapshots, {} staged".format(
            self._live_objects, len(self._live_object_references), len(self._snapshots),
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
        if sid not in self._live_object_references:
            self._live_object_references[sid] = obj
        else:
            assert self._live_object_references[sid] is obj

        self._live_objects.insert(obj, record)

    def insert_live_object_reference(self, ref: records.SnapshotId, obj):
        """Insert a snapshot reference for an object into the transaction"""
        self._live_object_references[ref] = obj

    def get_live_object(self, obj_id):
        if self.is_deleted(obj_id):
            raise exceptions.ObjectDeleted(obj_id)

        return self._live_objects.get_object(obj_id)

    def get_record_for_live_object(self, obj) -> records.DataRecord:
        return self._live_objects.get_record(obj)

    def get_live_object_from_reference(self, snapshot_id: records.SnapshotId):
        if self.is_deleted(snapshot_id.obj_id):
            raise exceptions.ObjectDeleted(snapshot_id.obj_id)

        try:
            return self._live_object_references[snapshot_id]
        except KeyError:
            raise exceptions.NotFound(
                "No live object with reference '{}' found".format(snapshot_id))

    def get_reference_for_live_object(self, obj):
        for ref, cached in self._live_object_references.items():
            if obj is cached:
                return ref

        raise exceptions.NotFound("Live object '{} not found".format(obj))

    def delete(self, obj_id):
        """Mark an object as deleted"""
        try:
            self._live_objects.remove(obj_id)
        except exceptions.NotFound:
            pass
        else:
            found_ref = None
            for ref in self._live_object_references:
                if ref.obj_id == obj_id:
                    found_ref = ref
                    break
            del self._live_object_references[found_ref]

        self.set_meta(obj_id, None)
        self._deleted.add(obj_id)

    def is_deleted(self, obj_id):
        return obj_id in self.deleted

    # endregion LiveObjects

    # region meta

    def set_meta(self, obj_id, meta: Optional[dict]):
        """Set an object's metadata.  Can pass None to unset."""
        self._metas[obj_id] = copy.deepcopy(meta)

    def get_meta(self, obj_id) -> dict:
        """Get an object's metadata.
        If the returned metadata is None then it means the user has set this metadata to be removed.

        :raise exceptions.NotFound: if not metadata information for the object is contained in this
            transaction.
        """
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
        self._live_object_references.update(transaction._live_object_references)
        self._staged.extend(transaction.staged)
        self._metas.update(transaction.metas)
        for deleted in transaction.deleted:
            self.delete(deleted)

    @staticmethod
    def rollback():
        raise RollbackTransaction


class NestedTransaction(Transaction):

    def __init__(self, parent: Transaction):
        super(NestedTransaction, self).__init__()
        self._parent = parent

    def __str__(self):
        return "{} (parent: {})".format(super().__str__(), self._parent)

    def get_live_object(self, obj_id):
        try:
            return super().get_live_object(obj_id)
        except exceptions.NotFound:
            return self._parent.get_live_object(obj_id)

    def get_live_object_from_reference(self, snapshot_id):
        try:
            return super().get_live_object_from_reference(snapshot_id)
        except exceptions.NotFound:
            return self._parent.get_live_object_from_reference(snapshot_id)

    def get_reference_for_live_object(self, obj):
        try:
            return super().get_reference_for_live_object(obj)
        except exceptions.NotFound:
            return self._parent.get_reference_for_live_object(obj)

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
