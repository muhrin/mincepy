from abc import ABCMeta, abstractmethod
from collections import namedtuple
import copy
import datetime
import typing
from typing import Sequence
import uuid

from . import types
from . import utils

__all__ = 'Archive', 'DataRecord', 'Ref'

OBJ_ID = 'obj_id'
TYPE_ID = 'type_id'
CREATED_BY = 'created_by'
COPIED_FROM = 'copied_from'
CREATION_TIME = 'creation_time'
VERSION = 'version'
STATE = 'state'
SNAPSHOT_HASH = 'snapshot_hash'
SNAPSHOT_TIME = 'snapshot_time'

DELETED = '!!deleted'

IdT = typing.TypeVar('IdT')  # The archive ID type

REF_KEY = '!!ref'


class Ref(typing.Generic[IdT], types.SavableObject):
    TYPE_ID = uuid.UUID('05fe092b-07b3-4ffc-8cf2-cee27aa37e81')

    def __init__(self, obj_id, version):
        super(Ref, self).__init__()
        self._obj_id = obj_id
        self._version = version

    def __str__(self):
        return "{}#{}".format(self._obj_id, self._version)

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
        yield from hasher.yield_hashables(self.obj_id)
        yield from hasher.yield_hashables(self.version)

    def save_instance_state(self, _depositor):
        return [self.obj_id, self.version]

    def load_instance_state(self, saved_state, _depositor):
        self.__init__(*saved_state)


class DataRecord(
        namedtuple(
            'DataRecord',
            (
                # Object properties
                OBJ_ID,  # The ID of the object (spanning all snapshots)
                TYPE_ID,  # The type ID of this object
                CREATION_TIME,  # The time this object was created
                CREATED_BY,  # The ID of the process the data was created in
                COPIED_FROM,  # The reference to the snapshot that this object was copied from
                # Snapshot properties
                VERSION,  # The ID of this particular snapshot of the object
                STATE,  # The saved state of the object
                SNAPSHOT_HASH,  # The hash of the state
                SNAPSHOT_TIME,  # The time this snapshot was created
            ))):
    """An immutable record that describes a snapshot of an object"""

    @classmethod
    def defaults(cls) -> dict:
        """Returns a dictionary of default values, the caller owns the ditionary and is free to modify it"""
        return {
            CREATION_TIME: None,
            CREATED_BY: None,
            COPIED_FROM: None,
            SNAPSHOT_TIME: None,
        }

    @classmethod
    def new_builder(cls, **kwargs) -> utils.NamedTupleBuilder:
        """Get a builder for a new data record, the version will be set to 0"""
        values = cls.defaults()
        values.update({
            CREATION_TIME: utils.DefaultFromCall(datetime.datetime.now),
            VERSION: 0,
            SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
        })
        values.update(kwargs)
        return utils.NamedTupleBuilder(cls, values)

    def is_deleted_record(self):
        """Does this record represent the object having been deleted"""
        return self.state == DELETED

    def get_reference(self) -> Ref:
        """Get a reference for this data record"""
        return Ref(self.obj_id, self.version)

    def get_copied_from(self) -> Ref:
        """Get the reference of the data record this object was originally copied from"""
        if self.copied_from is None:
            return None

        return Ref(*self.copied_from)

    def copy_builder(self, **kwargs) -> utils.NamedTupleBuilder:
        """Get a copy builder from this DataRecord instance.  The following attributes will be copied over:
            * type_id
            * state [deepcopy]
            * snapshot_hash
        the version will be set to 0 and the creation time to now
        """
        defaults = self.defaults()
        defaults.update({
            TYPE_ID: self.type_id,
            CREATION_TIME: utils.DefaultFromCall(datetime.datetime.now),
            STATE: copy.deepcopy(self.state),
            SNAPSHOT_HASH: self.snapshot_hash,
            VERSION: 0,
            COPIED_FROM: self.get_reference().save_instance_state(None),
            SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
        })
        defaults.update(kwargs)
        return utils.NamedTupleBuilder(type(self), defaults)

    def child_builder(self, **kwargs) -> utils.NamedTupleBuilder:
        """
        Get a child builder from this DataRecord instance.  The following attributes will be copied over:
            * obj_id
            * type_id
            * creation_time
            * created_by
        and version will be incremented by one.
        """
        defaults = self.defaults()
        defaults.update({
            OBJ_ID: self.obj_id,
            TYPE_ID: self.type_id,
            CREATION_TIME: self.creation_time,
            CREATED_BY: self.created_by,
            VERSION: self.version + 1,
            SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now)
        })
        defaults.update(kwargs)
        return utils.NamedTupleBuilder(type(self), defaults)


def make_deleted_record(last_record: DataRecord) -> DataRecord:
    """Get a record that represents the deletion of this object"""
    return last_record.child_builder(state=DELETED, snapshot_hash=None).build()


class Archive(typing.Generic[IdT], metaclass=ABCMeta):

    @classmethod
    def get_types(cls) -> Sequence:
        """This method allows the archive to return either types or type helper that the historian
        should support.  A common example is the type helper for the object id type"""
        return tuple()

    @classmethod
    def get_extra_primitives(cls) -> tuple:
        """Can optionally return a list of types that are treated as primitives i.e. considered to be
        storable and retrievable directly without encoding."""
        return tuple()

    @classmethod
    @abstractmethod
    def get_id_type(cls) -> typing.Type[IdT]:
        """Get the type used as an ID by this archive"""

    @abstractmethod
    def create_archive_id(self) -> IdT:
        """Create a new archive id"""

    @abstractmethod
    def create_file(self, filename: str = None, encoding: str = None):
        """Create a new file object specific for this archive type"""

    @abstractmethod
    def save(self, record: DataRecord):
        """Save a data record to the archive"""

    @abstractmethod
    def save_many(self, records: typing.Sequence[DataRecord]):
        """Save many data records to the archive"""

    # region Metadata

    @abstractmethod
    def get_meta(self, obj_id: IdT):
        """Get the metadata for the given object snapshot."""

    @abstractmethod
    def set_meta(self, obj_id: IdT, meta):
        """Set the metadata on on the object with the corresponding id"""

    @abstractmethod
    def update_meta(self, obj_id: IdT, meta):
        """Update the metadata on the object with the corresponding id"""

    # endregion

    @abstractmethod
    def load(self, reference: Ref[IdT]) -> DataRecord:
        """Load a snapshot of an object with the given reference"""

    @abstractmethod
    def history(self, obj_id: IdT, idx_or_slice) -> [DataRecord, typing.List[DataRecord]]:
        """Load the snapshot records for a particular object, can return a single or multiple records"""

    @abstractmethod
    def get_snapshot_refs(self, obj_id: IdT) -> typing.Sequence[Ref[IdT]]:
        """Returns a list of time ordered snapshot references"""

    # pylint: disable=too-many-arguments
    @abstractmethod
    def find(self,
             obj_id=None,
             type_id=None,
             created_by=None,
             copied_from=None,
             version=-1,
             state=None,
             snapshot_hash=None,
             meta=None,
             limit=0,
             sort=None,
             skip=0):
        """Find records matching the given criteria"""

    @abstractmethod
    def count(self,
              obj_id=None,
              type_id=None,
              created_by=None,
              copied_from=None,
              version=-1,
              state=None,
              snapshot_hash=None,
              meta=None,
              limit=0):
        """Count the number of entries that match the given query"""


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
