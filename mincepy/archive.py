from abc import ABCMeta, abstractmethod
from collections import namedtuple
import typing

from .depositor import Referencer
from . import utils

__all__ = ('Archive', 'DataRecord', 'TypeCodec')


class DataRecord(
    namedtuple(
        'DataRecord',
        (
                # Object properties
                'obj_id',  # The ID of the object (spanning all snapshots)
                'type_id',  # The type ID of this object
                'created_in',  # The ID of the process the data was created in
                # Snapshot properties
                'snapshot_id',  # The ID of this particular snapshot of the object
                'ancestor_id',  # The ID of the previous snapshot of the object
                'state',  # The saved state of the object
                'snapshot_hash',  # The hash of the state
        ))):

    @classmethod
    def get_builder(cls, **kwargs):
        """Get a DataRecord builder optionally passing in some initial values"""
        return utils.NamedTupleBuilder(cls, kwargs)

    def child_builder(self, **kwargs) -> utils.NamedTupleBuilder:
        """
        Get a child builder from this DataRecord instance.  The following attributes will be copied over:
            * obj_id
            * type_id
            * created_in
        and ancestor_id will be used as the new snapshot id.
        """
        defaults = {
            'obj_id': self.obj_id,
            'type_id': self.type_id,
            'created_in': self.created_in,
            'ancestor_id': self.snapshot_id, }
        defaults.update(kwargs)
        return utils.NamedTupleBuilder(type(self), defaults)


class TypeCodec(metaclass=ABCMeta):
    """Defines how to encode and decode an object"""
    TYPE = None
    TYPE_ID = None

    def __init__(self):
        assert self.TYPE is not None, "Must set TYPE that this codec corresponds to"
        assert self.TYPE_ID is not None, "Must set the TYPE_ID for this codec"

    @abstractmethod
    def encode(self, value, lookup: Referencer):
        pass

    @abstractmethod
    def decode(self, state, obj, lookup: Referencer):
        pass


class Archive(metaclass=ABCMeta):
    @abstractmethod
    def create_archive_id(self):
        """Create a new archive id"""

    @abstractmethod
    def save(self, record: DataRecord):
        """Save a data record to the archive"""

    @abstractmethod
    def save_many(self, records: typing.Sequence[DataRecord]):
        """Save many data records to the archive"""

    @abstractmethod
    def get_meta(self, snapshot_id):
        """Get the metadata for the given object snapshot."""

    @abstractmethod
    def set_meta(self, snapshot_id, meta):
        """Set the metadata on on the object with the corresponding persistent id"""

    @abstractmethod
    def load(self, snapshot_id) -> DataRecord:
        """Load a snapshot of an object with the given id, by default gives the latest"""

    @abstractmethod
    def get_snapshot_ids(self, obj_id):
        """Returns a list of ordered snapshot ids for a given object"""

    @abstractmethod
    def find(self, obj_type_id=None, filter=None, limit=0, sort=None):
        """Find objects matching the given criteria"""


class BaseArchive(Archive):
    def save_many(self, records: typing.Sequence[DataRecord]):
        """
        This will save records one by one but subclass may want to override this behaviour if
        they can save multiple records at once.
        """
        for record in records:
            self.save(record)
