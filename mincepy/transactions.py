import contextlib
from typing import MutableMapping, Any, List, Sequence

from . import archive
from . import exceptions
from . import utils


class RollbackTransaction(Exception):
    pass


class Transaction:

    def __init__(self):
        # Records staged for saving to the archive
        self._staged = []  # type: List[archive.DataRecord]

        self._live_objects = utils.LiveObjects()
        # Ref -> obj
        self._live_object_references = {}

        # Snapshots: ref -> obj
        self._snapshots = {}  # type: MutableMapping[archive.Ref, Any]

    def __str__(self):
        return "{}, {} live ref(s), {} snapshots, {} staged".format(self._live_objects,
                                                                    len(self._live_object_references),
                                                                    len(self._snapshots), len(self._staged))

    @property
    def live_objects(self) -> utils.LiveObjects:
        return self._live_objects

    @property
    def snapshots(self):
        return self._snapshots

    def insert_live_object(self, obj, record):
        ref = record.get_reference()
        if ref not in self._live_object_references:
            self._live_object_references[ref] = obj
        else:
            assert self._live_object_references[ref] is obj
        self._live_objects.insert(obj, record)

    def get_live_object(self, obj_id):
        return self._live_objects.get_object(obj_id)

    def get_record_for_live_object(self, obj):
        return self._live_objects.get_record(obj)

    def insert_live_object_reference(self, ref, obj):
        self._live_object_references[ref] = obj

    def get_live_object_from_reference(self, ref):
        try:
            return self._live_object_references[ref]
        except KeyError:
            raise exceptions.NotFound("No live object with reference '{}' found".format(ref))

    def get_reference_for_live_object(self, obj):
        for ref, cached in self._live_object_references.items():
            if obj is cached:
                return ref
        raise exceptions.NotFound("Live object '{} not found".format(obj))

    def insert_snapshot(self, obj, ref):
        self._snapshots[ref] = obj

    def get_snapshot(self, ref):
        try:
            return self._snapshots[ref]
        except KeyError:
            raise exceptions.NotFound("No snapshot with reference '{}' found".format(ref))

    def stage(self, record: archive.DataRecord):
        """Stage a record to be saved once on completion of this transaction"""
        self._staged.append(record)

    @property
    def staged(self) -> Sequence[archive.DataRecord]:
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
        self._live_objects.update(transaction.live_objects)
        self._snapshots.update(transaction.snapshots)
        self._live_object_references.update(transaction._live_object_references)
        self._staged.extend(transaction.staged)

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

    def get_snapshot(self, ref):
        try:
            return super(NestedTransaction, self).get_snapshot(ref)
        except exceptions.NotFound:
            return self._parent.get_snapshot(ref)
