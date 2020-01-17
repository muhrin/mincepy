from abc import ABCMeta, abstractmethod
from collections import namedtuple
import typing
import uuid

from . import types
from . import utils

__all__ = ('Archive', 'DataRecord', 'Ref')

OBJ_ID = 'obj_id'
TYPE_ID = 'type_id'
CREATED_IN = 'created_in'
VERSION = 'version'
STATE = 'state'
SNAPSHOT_HASH = 'snapshot_hash'

IdT = typing.TypeVar('IdT')  # The archive ID type


class Ref(typing.Generic[IdT], types.SavableComparable):
    TYPE_ID = uuid.UUID('05fe092b-07b3-4ffc-8cf2-cee27aa37e81')

    def __init__(self, obj_id, version):
        super(Ref, self).__init__()
        self._obj_id = obj_id
        self._version = version

    def __hash__(self):
        return (self.obj_id, self.version).__hash__()

    def __eq__(self, other):
        if not isinstance(other, Ref):
            return False

        return self.obj_id == other.obj_id and self.version == other.version

    @property
    def obj_id(self) -> IdT:
        return self._obj_id

    @property
    def version(self):
        return self._version

    def yield_hashables(self, hasher):
        yield from hasher.hash(self.obj_id)
        yield from hasher.hash(self.version)

    def save_instance_state(self, referencer):
        return [self.obj_id, self.version]

    def load_instance_state(self, saved_state, referencer):
        self.__init__(*saved_state)


class DataRecord(
        namedtuple(
            'DataRecord',
            (
                # Object properties
                OBJ_ID,  # The ID of the object (spanning all snapshots)
                TYPE_ID,  # The type ID of this object
                CREATED_IN,  # The ID of the process the data was created in
                # Snapshot properties
                VERSION,  # The ID of this particular snapshot of the object
                STATE,  # The saved state of the object
                SNAPSHOT_HASH,  # The hash of the state
            ))):
    DEFAULTS = {CREATED_IN: None}

    @classmethod
    def get_builder(cls, **kwargs):
        """Get a DataRecord builder optionally passing in some initial values"""
        values = dict(cls.DEFAULTS)
        values.update(kwargs)
        return utils.NamedTupleBuilder(cls, values)

    def get_reference(self) -> Ref:
        return Ref(self.obj_id, self.version)

    def child_builder(self, **kwargs) -> utils.NamedTupleBuilder:
        """
        Get a child builder from this DataRecord instance.  The following attributes will be copied over:
            * obj_id
            * type_id
            * created_in
        and version will be incremented by one.
        """
        defaults = {
            OBJ_ID: self.obj_id,
            TYPE_ID: self.type_id,
            CREATED_IN: self.created_in,
            VERSION: self.version + 1,
        }
        defaults.update(kwargs)
        return utils.NamedTupleBuilder(type(self), defaults)


class Archive(typing.Generic[IdT], metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def get_id_type(cls) -> typing.Type[IdT]:
        """Get the type used as an ID by this archive"""

    @abstractmethod
    def create_archive_id(self) -> IdT:
        """Create a new archive id"""

    @abstractmethod
    def save(self, record: DataRecord):
        """Save a data record to the archive"""

    @abstractmethod
    def save_many(self, records: typing.Sequence[DataRecord]):
        """Save many data records to the archive"""

    @abstractmethod
    def get_meta(self, obj_id: IdT):
        """Get the metadata for the given object snapshot."""

    @abstractmethod
    def set_meta(self, obj_id: IdT, meta):
        """Set the metadata on on the object with the corresponding persistent id"""

    @abstractmethod
    def load(self, reference: Ref[IdT]) -> DataRecord:
        """Load a snapshot of an object with the given reference"""

    @abstractmethod
    def history(self, obj_id: IdT, idx_or_slice) -> [DataRecord, typing.List[DataRecord]]:
        """Load the snapshot records for a particular object, can return a single or multiple records"""

    @abstractmethod
    def get_snapshot_refs(self, obj_id: IdT) -> typing.Sequence[Ref[IdT]]:
        """Returns a list of time ordered snapshot references"""

    @abstractmethod
    def find(self, obj_type_id=None, snapshot_hash=None, criteria=None, limit=0, sort=None):
        """Find objects matching the given criteria"""


class BaseArchive(Archive[IdT]):
    ID_TYPE = None  # type: typing.Type[IdT]

    @classmethod
    def get_id_type(cls) -> typing.Type[IdT]:
        assert cls.ID_TYPE, "The ID type has not been set on this archive"
        return cls.ID_TYPE

    def save_many(self, records: typing.Sequence[DataRecord]):
        """
        This will save records one by one but subclass may want to override this behaviour if
        they can save multiple records at once.
        """
        for record in records:
            self.save(record)

    def history(self, obj_id: IdT, idx_or_slice) -> [DataRecord, typing.List[DataRecord]]:
        refs = self.get_snapshot_refs(obj_id)[idx_or_slice]
        if len(refs) > 1:
            return [self.load(ref) for ref in refs]

        # Single one
        return self.load(refs[0])
