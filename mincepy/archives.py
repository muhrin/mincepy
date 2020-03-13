from abc import ABCMeta, abstractmethod
import typing
from typing import Sequence

from .records import DataRecord, Ref

__all__ = 'Archive', 'ASCENDING', 'DESCENDING'

IdT = typing.TypeVar('IdT')  # The archive ID type

# Sort options
ASCENDING = 1
DESCENDING = -1


class Archive(typing.Generic[IdT], metaclass=ABCMeta):

    @classmethod
    def get_types(cls) -> Sequence:
        """This method allows the archive to return either types or type helper that the historian
        should support.  A common example is the type helper for the object id type"""
        return tuple()

    @classmethod
    def get_extra_primitives(cls) -> tuple:
        """Can optionally return a list of types that are treated as primitives i.e. considered to
         be storable and retrievable directly without encoding."""
        return tuple()

    @classmethod
    @abstractmethod
    def get_id_type(cls) -> typing.Type[IdT]:
        """Get the type used as an ID by this archive"""

    @abstractmethod
    def create_archive_id(self) -> IdT:
        """Create a new archive id"""

    @abstractmethod
    def construct_archive_id(self, value) -> IdT:
        """If it's possible, construct an archive value from the passed value.
        This is useful as a convenience to the user if, say, the archive id can be
        constructed from a string.  Raise TypeError or ValueError if this is not possible
        for the given value.
        """

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

    @abstractmethod
    def find_meta(self, filter: dict):  # pylint: disable=redefined-builtin
        """Yield metadata satisfying the given filter"""

    # endregion

    @abstractmethod
    def load(self, reference: Ref[IdT]) -> DataRecord:
        """Load a snapshot of an object with the given reference"""

    @abstractmethod
    def history(self, obj_id: IdT, idx_or_slice) -> [DataRecord, typing.List[DataRecord]]:
        """Load the snapshot records for a particular object, can return a single or multiple
        records"""

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

    def construct_archive_id(self, value) -> IdT:  # pylint: disable=no-self-use
        raise TypeError("Not possible to construct an archive id from '{}'".format(type(value)))
