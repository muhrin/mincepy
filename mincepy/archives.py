import abc
import typing
from typing import Sequence, Iterable, Union

import deprecation

from .records import DataRecord, SnapshotRef
from .version import __version__

__all__ = 'Archive', 'BaseArchive', 'ASCENDING', 'DESCENDING'

IdT = typing.TypeVar('IdT')  # The archive ID type

# Sort options
ASCENDING = 1
DESCENDING = -1


class Archive(typing.Generic[IdT], metaclass=abc.ABCMeta):
    """An archive provides the persistent storage for the historian.  It is responsible for storing,
    searching and loading data records and their metadata."""

    RefEdge = typing.NamedTuple('RefEdge', [('source', SnapshotRef[IdT]),
                                            ('target', SnapshotRef[IdT])])
    RefGraph = Iterable[RefEdge]

    @deprecation.deprecated(deprecated_in="0.11.0",
                            removed_in="0.12.0",
                            current_version=__version__,
                            details="Use meta_find() instead")
    def find_meta(self, filter: dict):  # pylint: disable=redefined-builtin
        """Yield metadata satisfying the given filter"""
        return self.meta_find(filter)

    @deprecation.deprecated(deprecated_in="0.11.0",
                            removed_in="0.12.0",
                            current_version=__version__,
                            details="Use meta_update() instead")
    def update_meta(self, obj_id: IdT, meta: dict):
        """Update the metadata on the object with the corresponding id"""
        return self.meta_update(obj_id, meta)

    @deprecation.deprecated(deprecated_in="0.11.0",
                            removed_in="0.12.0",
                            current_version=__version__,
                            details="Use meta_set() instead")
    def set_meta(self, obj_id: IdT, meta: dict):
        """Set the metadata on on the object with the corresponding id"""
        return self.meta_set(obj_id, meta)

    @deprecation.deprecated(deprecated_in="0.11.0",
                            removed_in="0.12.0",
                            current_version=__version__,
                            details="Use meta_get() instead")
    def get_meta(self, obj_id: IdT):
        """Get the metadata for the given object snapshot."""
        return self.meta_get(obj_id)

    @classmethod
    def get_types(cls) -> Sequence:
        """This method allows the archive to return either types or type helper that the historian
        should support.  A common example is the type helper for the object id type"""
        return tuple()

    @classmethod
    @abc.abstractmethod
    def get_id_type(cls) -> typing.Type[IdT]:
        """Get the type used as an ID by this archive"""

    @abc.abstractmethod
    def create_archive_id(self) -> IdT:
        """Create a new archive id"""

    @abc.abstractmethod
    def construct_archive_id(self, value) -> IdT:
        """If it's possible, construct an archive value from the passed value.
        This is useful as a convenience to the user if, say, the archive id can be
        constructed from a string.  Raise TypeError or ValueError if this is not possible
        for the given value.
        """

    @abc.abstractmethod
    def create_file(self, filename: str = None, encoding: str = None):
        """Create a new file object specific for this archive type"""

    @abc.abstractmethod
    def save(self, record: DataRecord):
        """Save a data record to the archive"""

    @abc.abstractmethod
    def save_many(self, records: typing.Sequence[DataRecord]):
        """Save many data records to the archive"""

    # region Metadata

    @abc.abstractmethod
    def meta_get(self, obj_id: IdT):
        """Get the metadata for the given object snapshot."""

    @abc.abstractmethod
    def meta_set(self, obj_id: IdT, meta: dict):
        """Set the metadata on on the object with the corresponding id"""

    @abc.abstractmethod
    def meta_update(self, obj_id: IdT, meta: dict):
        """Update the metadata on the object with the corresponding id"""

    @abc.abstractmethod
    def meta_find(self, filter: dict):  # pylint: disable=redefined-builtin
        """Yield metadata satisfying the given filter"""

    @abc.abstractmethod
    def meta_create_index(self, keys, unique=False, where_exist=False):
        """Create an index on the metadata.  Takes either a single key or list of (key, direction)
         pairs

         :param keys: the key or keys to create the index on
         :param unique: if True, create a uniqueness constraint on this index
         :param where_exist: if True the index only applies for documents where the key(s) exist
         """

    # endregion

    @abc.abstractmethod
    def load(self, reference: SnapshotRef[IdT]) -> DataRecord:
        """Load a snapshot of an object with the given reference"""

    @abc.abstractmethod
    def history(self, obj_id: IdT, idx_or_slice) -> [DataRecord, typing.List[DataRecord]]:
        """Load the snapshot records for a particular object, can return a single or multiple
        records"""

    @abc.abstractmethod
    def get_snapshot_refs(self, obj_id: IdT) -> typing.Sequence[SnapshotRef[IdT]]:
        """Returns a list of time ordered snapshot references"""

    # pylint: disable=too-many-arguments
    @abc.abstractmethod
    def find(self,
             obj_id: Union[IdT, Iterable[IdT]] = None,
             type_id=None,
             created_by=None,
             copied_from=None,
             version=-1,
             state=None,
             deleted=True,
             snapshot_hash=None,
             meta=None,
             limit=0,
             sort=None,
             skip=0):
        """Find records matching the given criteria

        :param type_id: the type id to look for
        :param created_by: find records with the given type id
        :param copied_from: find records copied from the record with the given id
        :param version: find records with this version, -1 for latest
        :param state: find objects with this state filter
        :param deleted: if True, find deleted records too
        :param snapshot_hash: find objects with this snapshot hash
        :param meta: find objects with this meta filter
        :param limit: limit the results to this many records
        :param obj_id: an obj or or an iterable of obj ids to look for
        :param sort: sort the results by the given criteria
        :param skip: skip the this many entries
        """

    @abc.abstractmethod
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

    @abc.abstractmethod
    def get_reference_graph(self,
                            srefs: Sequence[SnapshotRef[IdT]]) -> 'Sequence[Archive.RefGraph]':
        """Given one or more object ids the archive will supply the corresponding reference graph(s)
        """


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
