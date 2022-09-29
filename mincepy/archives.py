# -*- coding: utf-8 -*-
import abc
from typing import (
    Generic,
    TypeVar,
    NamedTuple,
    Sequence,
    Union,
    Mapping,
    Iterable,
    Dict,
    Iterator,
    Any,
    Type,
    Optional,
    Callable,
    List,
    Tuple,
)

import networkx

from . import qops as q
from . import records
from .records import DataRecord
from . import operations

__all__ = (
    "Archive",
    "BaseArchive",
    "ArchiveListener",
    "ASCENDING",
    "DESCENDING",
    "OUTGOING",
    "INCOMING",
)

IdT = TypeVar("IdT")  # The archive ID type

# Sort options
ASCENDING = 1
DESCENDING = -1

OUTGOING = 1
INCOMING = -1


class Archive(Generic[IdT], metaclass=abc.ABCMeta):
    """An archive provides the persistent storage for the Historian.  It is responsible for storing,
    searching and loading data records and their metadata."""

    # pylint: disable=too-many-public-methods

    SnapshotId = records.SnapshotId[IdT]
    MetaEntry = NamedTuple("MetaEntry", [("obj_id", IdT), ["meta", dict]])

    @classmethod
    def get_types(cls) -> Sequence:
        """This method allows the archive to return either types or type helper that the historian
        should support.  A common example is the type helper for the object id type"""
        return tuple()

    @classmethod
    @abc.abstractmethod
    def get_id_type(cls) -> Type[IdT]:
        """Get the type used as an ID by this archive"""

    @property
    @abc.abstractmethod
    def snapshots(self) -> "RecordCollection":
        """Access the snapshots collection"""

    @property
    @abc.abstractmethod
    def objects(self) -> "RecordCollection":
        """Access the objects collection"""

    @property
    @abc.abstractmethod
    def file_store(self):
        """Get the GridFS file bucket"""

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
    def save(self, record: DataRecord):
        """Save a data record to the archive"""

    @abc.abstractmethod
    def save_many(self, data_records: Sequence[DataRecord]):
        """Save many data records to the archive"""

    @abc.abstractmethod
    def bulk_write(self, ops: Sequence[operations.Operation]):
        """Make a collection of write operations to the database"""

    # region Metadata

    @abc.abstractmethod
    def meta_get(self, obj_id: IdT) -> Optional[dict]:
        """Get the metadata for an objects."""

    @abc.abstractmethod
    def meta_get_many(self, obj_ids: Iterable[IdT]) -> Dict:
        """Get the metadata for multiple objects.  Returns a dictionary mapping the object id to
        the metadata dictionary"""

    @abc.abstractmethod
    def meta_set(self, obj_id: IdT, meta: Optional[Mapping]):
        """Set the metadata on on the object with the corresponding id"""

    @abc.abstractmethod
    def meta_set_many(self, metas: Mapping[IdT, Optional[Mapping]]):
        """Set the metadata on multiple objects.  This takes a mapping of the object id to the
        corresponding (optional) metadata dictionary"""

    @abc.abstractmethod
    def meta_update(self, obj_id: IdT, meta: Mapping):
        """Update the metadata on the object with the corresponding id"""

    @abc.abstractmethod
    def meta_update_many(self, metas: Mapping[IdT, Mapping]):
        """Update the metadata on multiple objects.  This method expects to get a mapping of object
        id to the mapping to be used to update the metadata for that object"""

    @abc.abstractmethod
    def meta_find(
        self,
        filter: dict = None,  # pylint: disable=redefined-builtin
        obj_id: Union[IdT, Iterable[IdT], Mapping] = None,
    ) -> "Iterator[Archive.MetaEntry]":
        """Yield metadata satisfying the given criteria.  The search can optionally be restricted to
        a set of passed object ids.

        :param filter: a query filter for the search
        :param obj_id: an optional restriction on the object ids to search.  This ben be either:
            1. a single object id
            2. an iterable of object ids in which is treated as {'$in': list(obj_ids)}
            3. a general query filter to be applied to the object ids
        """

    @abc.abstractmethod
    def meta_distinct(
        self,
        key: str,
        filter: dict = None,  # pylint: disable=redefined-builtin
        obj_id: Union[IdT, Iterable[IdT], Mapping] = None,
    ) -> "Iterator":
        """Yield distinct values found for 'key' within metadata documents, optionally marching a
        search filter.

        The search can optionally be restricted to a set of passed object ids.

        :param key: the document key to get distinct values for
        :param filter: a query filter for the search
        :param obj_id: an optional restriction on the object ids to search.  This ben be either:
            1. a single object id
            2. an iterable of object ids in which is treated as {'$in': list(obj_ids)}
            3. a general query filter to be applied to the object ids
        """

    @abc.abstractmethod
    def meta_create_index(
        self, keys: Union[str, List[Tuple]], unique=False, where_exist=False
    ):
        """Create an index on the metadata.  Takes either a single key or list of (key, direction)
        pairs

        :param keys: the key or keys to create the index on
        :param unique: if True, create a uniqueness constraint on this index
        :param where_exist: if True the index only applies for documents where the key(s) exist
        """

    # endregion

    @abc.abstractmethod
    def load(self, snapshot_id: SnapshotId) -> DataRecord:
        """Load a snapshot of an object with the given reference"""

    @abc.abstractmethod
    def history(self, obj_id: IdT, idx_or_slice) -> [DataRecord, Sequence[DataRecord]]:
        """Load the snapshot records for a particular object, can return a single or multiple
        records"""

    @abc.abstractmethod
    def get_snapshot_ids(self, obj_id: IdT) -> "Sequence[Archive.SnapshotId]":
        """Returns a list of time ordered snapshot ids"""

    # pylint: disable=too-many-arguments
    @abc.abstractmethod
    def find(
        self,
        obj_id: Union[IdT, Iterable[IdT], Dict] = None,
        type_id=None,
        created_by: Optional[IdT] = None,
        copied_from: Optional[IdT] = None,
        version: int = None,
        state=None,
        state_types=None,
        snapshot_hash=None,
        meta: dict = None,
        extras: dict = None,
        limit=0,
        sort=None,
        skip=0,
    ) -> Iterator[DataRecord]:
        """Find records matching the given criteria

        :param type_id: find records with the given type id
        :param created_by: find records with the given created by id
        :param copied_from: find records copied from the record with the given id
        :param version: restrict the search to this version, -1 for latest
        :param state: find objects with this state filter
        :param state_types: file objects with this state types filter
        :param snapshot_hash: find objects with this snapshot hash
        :param meta: find objects with this meta filter
        :param extras: the search criteria to apply on the data record extras
        :param limit: limit the results to this many records
        :param obj_id: an optional restriction on the object ids to search.  This ben be either:
            1. a single object id
            2. an iterable of object ids in which is treated as {'$in': list(obj_ids)}
            3. a general query filter to be applied to the object ids
        :param sort: sort the results by the given criteria
        :param skip: skip the this many entries
        """

    @abc.abstractmethod
    def distinct(
        self, key: str, filter: dict = None
    ) -> Iterator:  # pylint: disable=redefined-builtin
        """Get distinct values of the given record key

        :param key: the key to find distinct values for, see DataRecord for possible keys
        :param filter: an optional filter to restrict the search to.  Should be a dictionary that
            filters on entries in the DataRecord i.e. the kwargs that can be passed to find().
        """

    @abc.abstractmethod
    def count(
        self,
        obj_id=None,
        type_id=None,
        created_by=None,
        copied_from=None,
        version=-1,
        state=None,
        snapshot_hash=None,
        meta=None,
        limit=0,
    ):
        """Count the number of entries that match the given query"""

    @abc.abstractmethod
    def get_snapshot_ref_graph(
        self, *snapshot_ids: SnapshotId, direction=OUTGOING, max_dist: int = None
    ) -> networkx.DiGraph:
        """Given one or more snapshot ids the archive will supply the corresponding reference
        graph(s).  The graphs start at the given id and contains all snapshots that it references,
        all snapshots they reference and so on.
        """

    @abc.abstractmethod
    def get_obj_ref_graph(
        self, *obj_ids: IdT, direction=OUTGOING, max_dist: int = None
    ) -> networkx.DiGraph:
        """Given one or more object ids the archive will supply the corresponding reference
        graph(s).  The graphs start at the given id and contains all object ids that it references,
        all object ids they reference and so on.
        """

    @abc.abstractmethod
    def add_archive_listener(self, listener: "ArchiveListener"):
        """Add a listener to be notified of archive events"""

    @abc.abstractmethod
    def remove_archive_listener(self, listener: "ArchiveListener"):
        """Remove a listener"""


class BaseArchive(Archive[IdT]):
    # This is _still_ an abstract class, pylint is just silly in not recognising that a class only becomes concrete
    # once _all_ abstract methods are implemented.  See: https://github.com/PyCQA/pylint/issues/179
    # pylint:disable=abstract-method

    ID_TYPE = None  # type: Type[IdT]

    @classmethod
    def get_id_type(cls) -> Type[IdT]:
        assert (  # nosec: intentional internal assert
            cls.ID_TYPE
        ), "The ID type has not been set on this archive"
        return cls.ID_TYPE

    def __init__(self):
        super().__init__()
        self._listeners = set()

    def save(self, record: DataRecord):
        return self.bulk_write([operations.Insert(record)])

    def save_many(self, data_records: Sequence[DataRecord]):
        """
        This will save records one by one but subclass may want to override this behaviour if
        they can save multiple records at once.
        """
        self.bulk_write([operations.Insert(record) for record in data_records])

    def meta_get_many(self, obj_ids: Iterable[IdT]) -> Dict[IdT, dict]:
        metas = {}
        for obj_id in obj_ids:
            metas[obj_id] = self.meta_get(obj_id)
        return metas

    def meta_update_many(self, metas: Mapping[IdT, Mapping]):
        for entry in metas.items():
            self.meta_update(*entry)

    def meta_set_many(self, metas: Mapping[IdT, Mapping]):
        for entry in metas.items():
            self.meta_set(*entry)

    def history(self, obj_id: IdT, idx_or_slice) -> [DataRecord, Sequence[DataRecord]]:
        refs = self.get_snapshot_ids(obj_id)[idx_or_slice]
        if len(refs) > 1:
            return [self.load(ref) for ref in refs]

        # Single one
        return self.load(refs[0])

    def construct_archive_id(self, value) -> IdT:
        raise TypeError(f"Not possible to construct an archive id from '{type(value)}'")

    def add_archive_listener(self, listener: "ArchiveListener"):
        self._listeners.add(listener)

    def remove_archive_listener(self, listener: "ArchiveListener"):
        self._listeners.remove(listener)

    def _fire_event(self, evt: Callable, *args, **kwargs):
        """Inform all listeners of an event.  The event should be a method from the ArchiveListener interface"""
        for listener in self._listeners:
            getattr(listener, evt.__name__)(self, *args, **kwargs)


def scalar_query_spec(
    specifier: Union[Mapping, Iterable[Any], Any]
) -> Union[Any, Dict]:
    """Convenience function to create a query specifier for a given item.  There are three
    possibilities:

    1. The item is a mapping in which case it is returned as is.
    2. The item is an iterable (but not a mapping) in which case it is interpreted to mean:
        {'$in': list(iterable)}
    3. it is a raw item item in which case it is matched directly
    """
    if isinstance(specifier, dict):  # This has to be first as dict is iterable
        return specifier
    if isinstance(
        specifier, Iterable
    ):  # pylint: disable=isinstance-second-argument-not-valid-type
        return q.in_(*specifier)

    return specifier


# pylint: disable=arguments-differ


class Collection(metaclass=abc.ABCMeta):
    """An abstraction for a database collection"""

    @property
    @abc.abstractmethod
    def archive(self) -> Archive:
        """Get the archive that this collection belongs to"""

    @abc.abstractmethod
    def find(
        self,
        filter: dict,  # pylint: disable=redefined-builtin
        *,
        projection=None,
        limit=0,
        sort=None,
        skip=0,
        **kwargs,
    ) -> Iterator[dict]:
        """Find entries matching the given criteria"""

    @abc.abstractmethod
    def distinct(
        self,
        key: str,
        filter: dict = None,  # pylint: disable=redefined-builtin
        **kwargs,
    ) -> Iterator:
        """Get distinct values of the given entry key

        :param key: the key to find distinct values for
        :param filter: an optional filter to restrict the search to
        """

    @abc.abstractmethod
    def get(self, entry_id) -> dict:
        """Get one entry using the principal id

        :raises mincepy.NotFound: if no object is found with the given id
        """

    @abc.abstractmethod
    def count(self, filter: dict, **kwargs) -> int:  # pylint: disable=redefined-builtin
        """Get the number of entries that match the search criteria"""


class RecordCollection(Collection):
    """An abstraction for collections holding records"""

    @abc.abstractmethod
    def find(
        self,
        filter: dict,  # pylint: disable=redefined-builtin
        *,
        projection=None,
        meta: dict = None,
        limit=0,
        sort=None,
        skip=0,
    ) -> Iterator[dict]:
        """Find all records matching the given criteria"""

    @abc.abstractmethod
    def distinct(
        self,
        key: str,
        filter: dict = None,  # pylint: disable=redefined-builtin
        meta: dict = None,
    ) -> Iterator[dict]:
        """Find all records matching the given criteria"""

    @abc.abstractmethod
    def count(
        self, filter: dict, *, meta: dict = None  # pylint: disable=redefined-builtin
    ) -> int:
        """Get the number of entries that match the search criteria"""


class ArchiveListener:
    """Archive listener interface"""

    def on_bulk_write(self, archive: Archive, ops: Sequence[operations.Operation]):
        """Called when an archive is about to perform a sequence of write operations but has not performed them yet.
        The listener must not assume that the operations will be completed as there are a number of reasons why this
        process could be interrupted.
        """

    def on_bulk_write_complete(
        self, archive: Archive, ops: Sequence[operations.Operation]
    ):
        """Called when an archive is has successfully performed a sequence of write operations"""
