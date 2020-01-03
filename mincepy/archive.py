from abc import ABCMeta, abstractmethod
from collections import namedtuple
import typing

from .depositor import Referencer

__all__ = ('Archive', 'DataRecord', 'TypeCodec')

DataRecord = namedtuple(
    'DataRecord',
    ('obj_id', 'type_id', 'ancestor_id', 'encoded_value', 'obj_hash'))


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
    def decode(self, encoded_value, obj, lookup: Referencer):
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
    def get_meta(self, persistent_id):
        """Get the metadata for the object with the corresponding persistent id."""

    @abstractmethod
    def set_meta(self, persistent_id, meta):
        """Set the metadata on on the object with the corresponding persistent id"""

    @abstractmethod
    def load(self, archive_id) -> DataRecord:
        """Load a data record from its archive id"""

    @abstractmethod
    def find(self, obj_type_id=None, filter=None, limit=0, sort=None):
        """Find objects matching the given criteria"""

    @abstractmethod
    def get_leaves(self, archive_id):
        """Get the leaf records for a particular archive id"""


class BaseArchive(Archive):
    def save_many(self, records: typing.Sequence[DataRecord]):
        """
        This will save records one by one but subclass may want to override this behaviour if
        they can save multiple records at once.
        """
        for record in records:
            self.save(record)
