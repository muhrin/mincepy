import contextlib
from typing import MutableMapping, Any, List, Sequence

from . import archive
from . import utils


class RollbackTransaction(Exception):
    pass


class Transaction:

    def __init__(self):
        self._objects = {} # type: MutableMapping[archive.Ref, Any]
        self._records = utils.WeakObjectIdDict()  # type: MutableMapping[Any, archive.DataRecord]
        self._staged = []  # type: List[archive.DataRecord]

    def insert_object(self, obj, ref):
        self._objects[ref] = obj

    def insert_object_and_record(self, obj, record: archive.DataRecord):
        self.insert_object(obj, record.get_reference())
        self._records[obj] = record

    def stage(self, record: archive.DataRecord):
        """Stage a record to be saved once on completion of this transaction"""
        self._staged.append(record)

    @property
    def objects(self):
        """The objects and references in this transaction"""
        return self._objects

    @property
    def records(self):
        """The objects with corresponding data records in this transaction"""
        return self._records

    @property
    def staged(self) -> Sequence[archive.DataRecord]:
        """The list of records that were staged during this transaction"""
        return self._staged

    def get_object(self, ref):
        """Get an object with the given reference in this transaction"""
        try:
            return self._objects[ref]
        except KeyError:
            raise ValueError("Unknown object reference '{}'".format(ref))

    def get_record(self, obj) -> archive.DataRecord:
        """Get a data record corresponding to an object from this transaction"""
        try:
            return self._records[obj]
        except KeyError:
            raise ValueError("Unknown object '{}'".format(obj))

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
        self._objects.update(transaction.objects)
        self._records.update(transaction.records)
        self._staged.extend(transaction.staged)

    @staticmethod
    def rollback():
        raise RollbackTransaction


class NestedTransaction(Transaction):

    def __init__(self, parent: Transaction):
        super(NestedTransaction, self).__init__()
        self._parent = parent

    def get_object(self, ref):
        try:
            return super(NestedTransaction, self).get_object(ref)
        except ValueError:
            return self._parent.get_object(ref)

    def get_record(self, obj):
        try:
            return super(NestedTransaction, self).get_record(obj)
        except ValueError:
            return self._parent.get_record(obj)
