import contextlib
import copy
from typing import MutableMapping, Any, List, Sequence, Optional, Mapping
import weakref

from . import archives
from . import exceptions
from . import records
from . import utils


class LiveObjects:

    def __init__(self):
        # Live object -> data records
        self._records = utils.WeakObjectIdDict()  # type: MutableMapping[Any, archives.DataRecord]
        # Obj id -> object
        self._objects = weakref.WeakValueDictionary()  # type: MutableMapping[Any, Any]

    def __str__(self):
        return "{} live".format(len(self._objects))

    def __contains__(self, item):
        """Determine if an object instance is in this live objects container"""
        return item in self._records

    def insert(self, obj, record):
        self._records[obj] = record
        self._objects[record.obj_id] = obj

    def update(self, live_objects):
        """Like a dictionary update, take the given live objects container and absorb it into ourselves
        overwriting any existing values and incorporating any new"""
        self._records.update(live_objects._records)
        self._objects.update(live_objects._objects)

    def delete(self, obj):
        del self._objects[self.get_record(obj).obj_id]
        del self._records[obj]

    def get_record(self, obj):
        try:
            return self._records[obj]
        except KeyError:
            raise exceptions.NotFound("No live object found '{}'".format(obj))

    def get_object(self, obj_id):
        try:
            return self._objects[obj_id]
        except KeyError:
            raise exceptions.NotFound("No live object with id '{}'".format(obj_id))


class RollbackTransaction(Exception):
    pass


class Transaction:

    def __init__(self):
        # Records staged for saving to the archive
        self._staged = []  # type: List[archives.DataRecord]

        self._live_objects = LiveObjects()
        # Ref -> obj
        self._live_object_references = {}

        # Snapshots: ref -> obj
        self._snapshots = {}  # type: MutableMapping[archives.Ref, Any]
        # Maps from the object id to a metadata dictionary
        self._metas = {}  # type: MutableMapping[Any, dict]

    def __str__(self):
        return "{}, {} live ref(s), {} snapshots, {} staged".format(
            self._live_objects, len(self._live_object_references), len(self._snapshots),
            len(self._staged))

    @property
    def live_objects(self) -> LiveObjects:
        return self._live_objects

    @property
    def snapshots(self):
        return self._snapshots

    @property
    def metas(self) -> dict:
        return self._metas

    # region LiveObjects

    def insert_live_object(self, obj, record: records.DataRecord):
        """Insert a live object along with an up-to-date record into the transaction"""
        ref = record.get_reference()
        if ref not in self._live_object_references:
            self._live_object_references[ref] = obj
        else:
            assert self._live_object_references[ref] is obj
        self._live_objects.insert(obj, record)

    def get_live_object(self, obj_id):
        return self._live_objects.get_object(obj_id)

    def get_record_for_live_object(self, obj) -> records.DataRecord:
        return self._live_objects.get_record(obj)

    def insert_live_object_reference(self, ref: records.Ref, obj):
        """Insert a snapshot reference for an object into the transaction"""
        self._live_object_references[ref] = obj

    def get_live_object_from_reference(self, ref: records.Ref):
        try:
            return self._live_object_references[ref]
        except KeyError:
            raise exceptions.NotFound("No live object with reference '{}' found".format(ref))

    def get_reference_for_live_object(self, obj):
        for ref, cached in self._live_object_references.items():
            if obj is cached:
                return ref
        raise exceptions.NotFound("Live object '{} not found".format(obj))

    def delete(self, obj):
        """Delete an object form the transaction"""
        self._live_objects.delete(obj)
        found_ref = None
        for ref, referenced in self._live_object_references.items():
            if referenced is obj:
                found_ref = ref
                break
        del self._live_object_references[found_ref]

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

    def insert_snapshot(self, obj, ref):
        self._snapshots[ref] = obj

    def get_snapshot(self, ref):
        try:
            return self._snapshots[ref]
        except KeyError:
            raise exceptions.NotFound("No snapshot with reference '{}' found".format(ref))

    def stage(self, record: records.DataRecord):
        """Stage a record to be saved once on completion of this transaction"""
        self._staged.append(record)

    @property
    def staged(self) -> Sequence[records.DataRecord]:
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

    def _update(self, transaction):
        """Absorb a nested transaction into this one, done at the end of a nested context"""
        self._live_objects.update(transaction.live_objects)
        self._snapshots.update(transaction.snapshots)
        self._live_object_references.update(transaction._live_object_references)
        self._staged.extend(transaction.staged)
        self._metas.update(transaction.metas)

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

    def get_live_object_from_reference(self, ref):
        try:
            return super().get_live_object_from_reference(ref)
        except exceptions.NotFound:
            return self._parent.get_live_object_from_reference(ref)

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

    def get_snapshot(self, ref):
        try:
            return super(NestedTransaction, self).get_snapshot(ref)
        except exceptions.NotFound:
            return self._parent.get_snapshot(ref)

    def get_meta(self, obj_id) -> dict:
        try:
            return super(NestedTransaction, self).get_meta(obj_id)
        except exceptions.NotFound:
            return self._parent.get_meta(obj_id)
