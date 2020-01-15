from abc import ABCMeta, abstractmethod
from collections import namedtuple
import typing

from . import utils

__all__ = ('Archive', 'DataRecord')

OBJ_ID = 'obj_id'
TYPE_ID = 'type_id'
CREATED_IN = 'created_in'
SNAPSHOT_ID = 'snapshot_id'
ANCESTOR_ID = 'ancestor_id'
STATE = 'state'
SNAPSHOT_HASH = 'snapshot_hash'


class DataRecord(
        namedtuple(
            'DataRecord',
            (
                # Object properties
                OBJ_ID,  # The ID of the object (spanning all snapshots)
                TYPE_ID,  # The type ID of this object
                CREATED_IN,  # The ID of the process the data was created in
                # Snapshot properties
                SNAPSHOT_ID,  # The ID of this particular snapshot of the object
                ANCESTOR_ID,  # The ID of the previous snapshot of the object
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

    def child_builder(self, **kwargs) -> utils.NamedTupleBuilder:
        """
        Get a child builder from this DataRecord instance.  The following attributes will be copied over:
            * obj_id
            * type_id
            * created_in
        and ancestor_id will be used as the new snapshot id.
        """
        defaults = {
            OBJ_ID: self.obj_id,
            TYPE_ID: self.type_id,
            CREATED_IN: self.created_in,
            ANCESTOR_ID: self.snapshot_id,
        }
        defaults.update(kwargs)
        return utils.NamedTupleBuilder(type(self), defaults)


IdT = typing.TypeVar('IdT')


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
    def get_meta(self, snapshot_id: IdT):
        """Get the metadata for the given object snapshot."""

    @abstractmethod
    def set_meta(self, snapshot_id: IdT, meta):
        """Set the metadata on on the object with the corresponding persistent id"""

    @abstractmethod
    def load(self, snapshot_id: IdT) -> DataRecord:
        """Load a snapshot of an object with the given id, by default gives the latest"""

    @abstractmethod
    def history(self, obj_id: IdT, idx_or_slice) -> [DataRecord, typing.List[DataRecord]]:
        """Load the snapshot records for a particular object, can return a single or multiple records"""

    @abstractmethod
    def get_snapshot_ids(self, obj_id: IdT):
        """Returns a list of ordered snapshot ids for a given object"""

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
        ids = self.get_snapshot_ids(obj_id)[idx_or_slice]
        if len(ids) > 1:
            return [self.load(sid) for sid in ids]

        # Single one
        return self.load(ids[0])
