# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
import collections
import contextlib

try:
    from contextlib import nullcontext
except ImportError:
    from contextlib2 import nullcontext
import getpass
import logging
import socket
from typing import MutableMapping, Any, Optional, Iterable, Union, Iterator, Type, Dict, Callable, Sequence
import weakref

import deprecation
import networkx

from . import archives
from . import builtins
from . import frontend
from . import defaults
from . import depositors
from . import refs
from . import exceptions
from . import expr
from . import files
from . import helpers
from . import hist
from . import migrate
from . import operations
from . import qops
from . import records as recordsm  # The records module
from . import result_types
from . import staging
from . import tracking
from . import types
from . import type_registry
from . import utils
from . import version as version_mod
from .transactions import RollbackTransaction, Transaction, LiveObjects

__all__ = 'Historian', 'ObjectEntry'

logger = logging.getLogger(__name__)

ObjectEntry = collections.namedtuple('ObjectEntry', 'ref obj')
HistorianType = Union[helpers.TypeHelper, Type[types.SavableObject]]


class Historian:  # pylint: disable=too-many-public-methods, too-many-instance-attributes
    """The historian acts as a go-between between your python objects and the archive which is
    a persistent store of the records.  It will keep track of all live objects (i.e. those that
    have active references to them) that have been loaded and/or saved as well as enabling the
    user to lookup objects in the archive."""

    @deprecation.deprecated(deprecated_in='0.14.5',
                            removed_in='0.16.0',
                            current_version=version_mod.__version__,
                            details='Use mincepy.copy() instead')
    def copy(self, obj):  # pylint: disable=no-self-use
        """Create a shallow copy of the object.  Using this method allows the historian to inject
        information about where the object was copied from into the record if saved."""
        return tracking.copy(obj)

    @deprecation.deprecated(deprecated_in='0.15.10',
                            removed_in='0.17.0',
                            current_version=version_mod.__version__,
                            details='Use mincepy.records.find() instead')
    def find_records(self, *args, **kwargs) -> Iterator[recordsm.DataRecord]:
        """Find records

        Has same signature as py:meth:`mincepy.Records.find`.
        """
        yield from self.records.find(*args, **kwargs)

    @deprecation.deprecated(deprecated_in='0.15.10',
                            removed_in='0.17.0',
                            current_version=version_mod.__version__,
                            details='Use mincepy.records.distinct() instead')
    def find_distinct(self, *args, **kwargs):
        """Get distinct values of the given record key

        Has same signature as py:meth:`mincepy.Records.distinct`.
        """
        yield from self.records.distinct(*args, **kwargs)

    def __init__(self, archive: archives.Archive, equators=()):
        self._archive = archive
        self._equator = types.Equator(defaults.get_default_equators() + equators)
        # Register default types
        self._type_registry = type_registry.TypeRegistry()
        self.register_type(refs.ObjRef)
        self.register_types(builtins.HISTORIAN_TYPES)
        self.register_type(builtins.SnapshotIdHelper())
        self.register_types(archive.get_types())

        # Snapshot objects -> reference. Objects that were loaded from historical snapshots
        self._snapshots_objects = utils.WeakObjectIdDict(
        )  # type: MutableMapping[Any, recordsm.SnapshotId]
        self._live_objects = LiveObjects()

        self._transactions = None

        self._user = getpass.getuser()
        self._hostname = socket.gethostname()

        self._live_depositor = depositors.LiveDepositor(self)
        self._meta = hist.Meta(self, self._archive)
        self._migrate = migrate.Migrations(self)
        self._references = hist.References(self)

        self._snapshots = frontend.ObjectCollection(
            self,
            self._archive.snapshots,
            record_factory=lambda record_dict: SnapshotLoadableRecord(
                record_dict, self.load_snapshot_from_record),
            obj_loader=self.load_snapshot_from_record)
        self._objects = frontend.ObjectCollection(
            self,
            self._archive.objects,
            record_factory=lambda record_dict: LoadableRecord(
                record_dict, self.load_snapshot_from_record, self._load_object_from_record),
            obj_loader=self._load_object_from_record)

    @property
    def archive(self):
        return self._archive

    @property
    def meta(self) -> hist.Meta:
        """Access to functions that operate on the metadata"""
        return self._meta

    @property
    def primitives(self) -> tuple:
        """A tuple of all the primitive types"""
        return types.PRIMITIVE_TYPES + (self._archive.get_id_type(),)

    @property
    def migrations(self) -> migrate.Migrations:
        """Access the migration possibilities"""
        return self._migrate

    @property
    def records(self) -> frontend.EntriesCollection['LoadableRecord']:
        """Access methods and properties that act on and return data records"""
        return self._objects.records

    @property
    def objects(self) -> frontend.ObjectCollection:
        """Access the snapshots"""
        return self._objects

    @property
    def references(self) -> hist.References:
        """Access the references possibilities"""
        return self._references

    @property
    def snapshots(self) -> frontend.ObjectCollection:
        """Access the snapshots"""
        return self._snapshots

    def create_file(self, filename: str = None, encoding: str = None) -> builtins.BaseFile:
        """Create a new file.  The historian will supply file type compatible with the archive in
         use."""
        return files.File(self._archive.file_store, filename, encoding)

    def save(self, *objs: object):
        """Save multiple objects producing corresponding object identifiers.  This returns a
        sequence of ids that is in the same order as the passed objects.

        :param objs: the object(s) to save.  Can also be a tuple of (obj, meta) to optionally
            include metadata to be saved with the object(s)
        """
        to_save = []
        # Convert everything to tuples
        for entry in objs:
            if isinstance(entry, tuple):
                if len(entry) > 2:
                    raise ValueError(
                        f"Supplied tuples can only contain (object, meta), got '{entry}'")
            else:
                entry = (entry,)
            to_save.append(entry)

        ids = []
        with self.in_transaction():
            for entry in to_save:
                ids.append(self.save_one(*entry))

        if len(objs) == 1:
            return ids[0]

        return ids

    def save_one(self, obj: object, meta: dict = None):
        """Save the object returning an object id.  If metadata is supplied it will be set on the
        object.

        Developer note: this is the front end point-of-entry for a user/client code saving an object
        however subsequent objects being saved in this transaction will only go through _save_object
        and therefore any code common to all objects being saved should possibly go there.
        """
        if obj in self._snapshots_objects:
            raise exceptions.ModificationError(
                'Cannot save a snapshot object, that would rewrite history!')

        if meta and not isinstance(meta, dict):
            raise TypeError("Metadata must be a dictionary, got type '{}'".format(type(meta)))

        # Save the object and metadata
        with self.in_transaction():
            record = self._live_depositor._save_object(obj)  # pylint: disable=protected-access
            if meta:
                self.meta.update(record.obj_id, meta)

        return record.obj_id

    def is_known(self, obj: object) -> bool:
        """Check if an object has ever been saved and is therefore known to the historian

        :return: True if ever saved, False otherwise
        """
        return self.get_obj_id(obj) is not None

    def replace(self, old: object, new: object):
        """Replace a live object with a new version.

        This is especially useful if you have made a copy of an object and modified it but you want
        to continue the history of the object as the original rather than a brand new object.  Then
        just replace the old object with the new one by calling this function.
        """
        if self.current_transaction() is not None:
            raise RuntimeError("Can't replace during a transaction for the time being")

        if not isinstance(new, type(old)):
            raise TypeError("Can't replace type '{} with type '{}!".format(type(old), type(new)))

        # Get the current record and replace the object with the new one
        record = self._live_objects.get_record(old)
        self._live_objects.remove(record.obj_id)
        self._live_objects.insert(new, record)

        # Make sure creators is correct as well
        staging.replace(old, new)

    def load_snapshot(self, snapshot_id: recordsm.SnapshotId) -> object:
        return self._new_snapshot_depositor().load(snapshot_id)

    def load_snapshot_from_record(self, record: recordsm.DataRecord) -> object:
        return self._new_snapshot_depositor().load_from_record(record)

    def load(self, *obj_id_or_snapshot_id):
        """Load object(s) or snapshot(s)."""
        loaded = []
        for entry in obj_id_or_snapshot_id:
            loaded.append(self.load_one(entry))

        if len(obj_id_or_snapshot_id) == 1:
            return loaded[0]

        return loaded

    def load_one(self, obj_id_or_snapshot_id) -> object:
        """Load one object or snapshot from the database"""
        if isinstance(obj_id_or_snapshot_id, recordsm.SnapshotId):
            return self.load_snapshot(obj_id_or_snapshot_id)

        # OK, assume we're dealing with an object id
        obj_id = self._ensure_obj_id(obj_id_or_snapshot_id)

        # Try getting the object from the our dict of up to date ones
        try:
            return self.get_obj(obj_id)
        except exceptions.NotFound:
            # Going to have to load from the database
            return self._live_depositor._load_object(obj_id)  # pylint: disable=protected-access

    def get(self, obj_id) -> object:
        """Get a live object using the object id"""
        return self._objects.get(obj_id)

    def sync(self, obj: object) -> bool:
        """Update an object with the latest state in the database.
        If there is no new version in the archive then the current version remains
        unchanged including any modifications.

        :return: True if the object was updated, False otherwise
        """
        obj_id = self.get_obj_id(obj)
        if obj_id is None:
            # Never saved so the object is as up to date as can be!
            return False

        record = self._objects.records.get(obj_id)

        if record.is_deleted_record():
            raise exceptions.ObjectDeleted(f"Object with id '{obj_id}' has been deleted")

        if record.version == self.get_snapshot_id(obj).version:
            # Nothing has changed
            return False

        # The one in the archive is newer, so use that
        return self._live_depositor.update_from_record(obj, record)

    def delete(self, *obj_or_identifier, imperative=True) -> result_types.DeleteResult:
        """Delete an object.

        :param imperative: if True, this means that the caller explicitly expects this call to delete the passed objects
            and it should therefore raise if an object cannot be found or has been deleted already.  If False, the
            function will ignore these cases and continue.
        :raises mincepy.NotFound: if the object cannot be found (potentially because it was
            already deleted)
        """
        # We need the current records to be able to build the delete records
        obj_ids = list(map(self._ensure_obj_id, obj_or_identifier))

        # Find the current records
        records = {}  # type: Dict[Any, recordsm.DataRecord]
        left_to_find = set()
        for obj_id in obj_ids:
            try:
                records[obj_id] = self.get_current_record(self.get_obj(obj_id))
            except exceptions.ObjectDeleted:
                if imperative:
                    # Object deleted already so reraise
                    raise
            except exceptions.NotFound:
                left_to_find.add(obj_id)

        # Those that we don't have cached records for and need to look up
        if left_to_find:
            # Have a look in the archive
            for record in self._objects.records.find(recordsm.DataRecord.obj_id.in_(*left_to_find)):
                records[record.obj_id] = record
                left_to_find.remove(record.obj_id)

        if left_to_find and imperative:
            raise exceptions.NotFound(left_to_find)

        deleted = []
        with self.in_transaction() as trans:
            # Mark each object as deleted in the transaction and stage the delete record for insertion
            # in the order that they were passed to us, in case this makes a difference to the caller
            for obj_id in obj_ids:
                record = records.get(obj_id, None)
                if record is None:
                    continue

                builder = recordsm.make_deleted_builder(record)
                deleted_record = self._record_builder_created(builder).build()
                trans.delete(record.obj_id)
                trans.stage(operations.Insert(deleted_record))
                deleted.append(record.obj_id)

        return result_types.DeleteResult(deleted, left_to_find)

    def history(self,
                obj_or_obj_id,
                idx_or_slice='*',
                as_objects=True) -> [Sequence[ObjectEntry], Sequence[recordsm.DataRecord]]:
        """Get a sequence of object ids and instances from the history of the given object.

        :param obj_or_obj_id: The instance or id of the object to get the history for
        :param idx_or_slice: The particular index or a slice of which historical versions to get
        :param as_objects: if True return the object instances, otherwise returns the DataRecords

        Example:
        >>> import mincepy, mincepy.testing
        >>> historian = mincepy.get_historian()
        >>> car = mincepy.testing.Car('ferrari', 'white')
        >>> car_id = historian.save(car)
        >>> car.colour = 'red'
        >>> historian.save(car)
        >>> history = historian.history(car_id)
        >>> len(history)
        2
        >>> history[0].obj.colour == 'white'
        True
        >>> history[1].obj.colour == 'red'
        True
        >>> history[1].obj is car
        """
        obj_id = self._ensure_obj_id(obj_or_obj_id)
        snapshot_ids = self._archive.get_snapshot_ids(obj_id)
        indices = utils.to_slice(idx_or_slice)
        to_get = snapshot_ids[indices]
        if as_objects:
            return [ObjectEntry(sid, self.load(sid)) for sid in to_get]

        return [self._archive.load(ref) for ref in to_get]

    def get_current_record(self, obj: object) -> recordsm.DataRecord:
        """Get the current record that the historian has cached for the passed object"""
        trans = self.current_transaction()
        # Try the transaction first
        if trans:
            try:
                return trans.get_record_for_live_object(obj)
            except exceptions.ObjectDeleted:
                # ObjectDeleted is a specialisation of the NotFound error but it means that we
                # should consider the object as being gone so reraise it
                raise
            except exceptions.NotFound:
                pass

        return self._live_objects.get_record(obj)

    def get_obj_id(self, obj: object) -> Any:
        """Get the object ID for a live object.

        :return: the object id or None if the object is not known to the historian
        """
        trans = self.current_transaction()
        if trans is not None:
            try:
                return trans.get_snapshot_id_for_live_object(obj).obj_id
            except exceptions.NotFound:
                pass

        try:
            obj_id = self._live_objects.get_record(obj).obj_id
            if trans is not None and trans.is_deleted(obj_id):
                # The object has been deleted in the transaction, so it is not known
                return None

            return obj_id
        except exceptions.NotFound:
            return None

    def get_obj(self, obj_id) -> object:
        """Get a currently live object"""
        trans = self.current_transaction()
        if trans:
            try:
                return trans.get_live_object(obj_id)
            except exceptions.ObjectDeleted:
                # ObjectDeleted is a specialisation of the NotFound error but it means that we
                # should consider the object as being gone so reraise it
                raise
            except exceptions.NotFound:
                pass

        return self._live_objects.get_object(obj_id)

    def is_saved(self, obj: object) -> bool:
        """Test if an object is saved with this historian.  This is equivalent to
        `historian.get_obj_id(obj) is not None`."""
        return self.get_obj_id(obj) is not None

    def to_obj_id(self, obj_or_identifier):
        """
        This call will try and get an object id from the passed parameter.  The possibilities are:

        1. Passed an object ID in which case it will be returned unchanged
        2. Passed a snapshot ID, in which case the corresponding object ID will be returned
        2. Passed a live object instance, in which case the id of that object will be returned
        3. Passed a type that can be understood by the archive as an object id e.g. a string of
           version, in which case the archive will attempt to convert it

        Returns None if neither of these cases were True.
        """
        if self.is_obj_id(obj_or_identifier):
            return obj_or_identifier

        if isinstance(obj_or_identifier, recordsm.SnapshotId):
            return obj_or_identifier.obj_id

        try:
            # Try creating it for the user by calling the constructor with the argument passed.
            # This helps for common obj id types which can be constructed from a string
            return self._archive.construct_archive_id(obj_or_identifier)
        except (ValueError, TypeError):
            # Maybe we've been passed an object
            pass

        return self.get_obj_id(obj_or_identifier)

    def get_snapshot_id(self, obj: object) -> recordsm.SnapshotId:
        """Get the current snapshot id for a live object.  Will return the id or raise
        :class:`mincepy.NotFound` exception"""
        trans = self.current_transaction()
        if trans:
            try:
                return trans.get_snapshot_id_for_live_object(obj)
            except exceptions.NotFound:
                pass

        return self._live_objects.get_record(obj).snapshot_id

    def hash(self, obj: object):
        return self._equator.hash(obj)

    def eq(self, one: object, other: object):  # pylint: disable=invalid-name
        return self._equator.eq(one, other)

    # region Types

    @classmethod
    def is_trackable(cls, obj):
        """Determine if an object is trackable i.e. we can treat these as live objects and
        automatically keep track of their history when saving.  Ultimately this is determined by
        whether the type is weak referencable or not.
        """
        try:
            weakref.ref(obj)
            return True
        except TypeError:
            return False

    @property
    def type_registry(self) -> type_registry.TypeRegistry:
        return self._type_registry

    def is_primitive(self, obj) -> bool:
        """Check if the object is one of the primitives and should be saved by value in the
        archive"""
        return obj.__class__ in self.primitives

    def is_obj_id(self, obj_id) -> bool:
        """Check if an object is of the object id type"""
        return isinstance(obj_id, self._archive.get_id_type())

    def register_type(self, obj_class_or_helper: HistorianType) -> helpers.TypeHelper:
        helper = self._type_registry.register_type(obj_class_or_helper)
        self._equator.add_equator(helper)
        return helper

    def register_types(self, obj_clases_or_helpers: Iterable[HistorianType]):
        for item in obj_clases_or_helpers:
            self.register_type(item)

    def get_obj_type_id(self, obj_type):
        return self._type_registry.get_type_id(obj_type)

    def get_obj_type(self, type_id):
        return self.get_helper(type_id).TYPE

    def get_helper(self, type_id_or_type, auto_register=False) -> helpers.TypeHelper:
        if auto_register and issubclass(type_id_or_type, types.SavableObject) and \
                type_id_or_type not in self._type_registry:
            self.register_type(type_id_or_type)

        return self._type_registry.get_helper(type_id_or_type)

    # endregion

    # pylint: disable=redefined-builtin
    def find(self,
             *filter: expr.FilterSpec,
             obj_type=None,
             obj_id=None,
             version: int = -1,
             state=None,
             meta: dict = None,
             sort=None,
             limit=0,
             skip=0) -> frontend.ResultSet[object]:
        """
        .. _MongoDB: https://docs.mongofrontend.com/manual/tutorial/query-documents/

        Find objects.  This call will search the archive for objects matching the given criteria.
        In many cases the main arguments of interest will be `state` and `meta` which allow you to
        apply filters on the stored state of the object and metadata respectively.  To understand
        how the state is stored in the database (and therefore how to apply filters to it) it may
        be necessary to look at the details of the `save_instance_state()` method for that type.
        Metadata is always a dictionary containing primitives (strings, dicts, lists, etc).

        For the most part, the filter syntax of `mincePy` conforms to that of `MongoDB`_ with
        convenience functions locate in :py:mod:`mincepy.qops` that can make it easier to
        to build a query.

        Examples:

        Find all :py:class:`~mincepy.testing.Car`s that are brown or red:

        >>> import mincepy as mpy
        >>> historian = mpy.get_historian()
        >>> historian.find(mpy.testing.Car.colour.in_('brown', 'red'))

        Find all people that are older than 34 and live in Edinburgh:

        >>> historian.find(mpy.testing.Person.age > 34, meta=dict(city='Edinburgh'))

        :param obj_type: the object type to look for
        :param obj_id: an object or multiple object ids to look for
        :param version: the version of the object to retrieve, -1 means latest
        :param state: the criteria on the state of the object to apply
        :type state: must be subclass of historian.primitive
        :param meta: the search criteria to apply on the metadata of the object
        :param sort: the sort criteria
        :param limit: the maximum number of results to return, 0 means unlimited
        :param skip: the page to get results from
        """
        return self._objects.find(
            *filter,
            obj_type=obj_type,
            obj_id=obj_id,
            version=version,
            state=state,
            meta=meta,
            sort=sort,
            limit=limit,
            skip=skip,
        )

    def get_creator(self, obj_or_identifier) -> object:
        """Get the object that created the passed object"""
        if not self.is_obj_id(obj_or_identifier):
            # Object instance, try the staging area
            info = staging.get_info(obj_or_identifier, create=False) or {}
            created_by = info.get(recordsm.ExtraKeys.CREATED_BY, None)
            if created_by is not None:
                return created_by

        creator_id = self.created_by(obj_or_identifier)
        return self.load_one(creator_id)

    def created_by(self, obj_or_identifier):
        """Return the id of the object that created the passed object"""
        try:
            record = self.get_current_record(obj_or_identifier)
        except exceptions.NotFound as exc:
            if not self.is_obj_id(obj_or_identifier):
                raise

            try:
                record = self._objects.records.find(obj_id=obj_or_identifier).one()
            except exceptions.NotOneError:
                raise exc from None

        return record.created_by

    def get_user_info(self) -> dict:
        """Get information about the current user and host"""
        user_info = {}
        if self._user:
            user_info[recordsm.ExtraKeys.USER] = self._user
        if self._hostname:
            user_info[recordsm.ExtraKeys.HOSTNAME] = self._hostname
        return user_info

    def merge(
        self,
        result_set: frontend.ResultSet[object],
        *,
        meta=None,  # pylint: disable=unused-argument
        batch_size=1024,
        progress_callback: Callable[[utils.Progress, Optional[result_types.MergeResult]],
                                    None] = None
    ) -> result_types.MergeResult:
        """Merge a set of objects into this database.

        Given a set of results from another archive this will attempt to merge the corresponding records
        into this historian's archive.

        :param result_set: the set of records to merge from the source historian
        :param meta: option for merging metadata, allowed values:
            None        - Don't merge metadata
            'update'    - Perform dictionary update with existing metadata
            'overwrite' - In the case of an existing metadata dictionary, overwrite it
        """
        # REMOTE
        remote = result_set.historian  # type: Historian
        # Get information about the records that we've been asked to merge
        remote_partial_records = result_set._project(recordsm.OBJ_ID, recordsm.VERSION)  # pylint: disable=protected-access
        remote_snapshot_ids = set(map(recordsm.SnapshotId.from_dict,
                                      remote_partial_records))  # DB HIT

        progress = utils.Progress(len(remote_snapshot_ids))
        if progress_callback is not None:
            progress_callback(progress, result_types.MergeResult())

        result = result_types.MergeResult()
        # get the outgoing snapshot ref. graph
        while remote_snapshot_ids:
            # Get a batch
            batch = set()
            try:
                for _ in range(batch_size):
                    batch.add(remote_snapshot_ids.pop())
            except KeyError:
                pass
            graph = remote.references.get_snapshot_ref_graph(*batch)

            # The graph may contain nodes that are still in our list of remote snapshots to transfer
            # so check and remove these because they will be done in this batch
            extras = set(graph.nodes) - batch
            remote_snapshot_ids.difference_update(extras)

            partial_result = self._merge_batch(remote, graph)
            result.update(partial_result)

            progress.done = progress.total - len(remote_snapshot_ids)
            if progress_callback is not None:
                progress_callback(progress, partial_result)

        return result

    def _merge_batch(self, remote: 'Historian',
                     remote_ref_graph: networkx.DiGraph) -> result_types.MergeResult:
        sid_strings = list(map(str, remote_ref_graph.nodes))

        # REMOTE
        # Get the partial records for all these snapshots indexed by the SID
        remote_partial_records = {}
        for entry in remote.archive.snapshots.find({'_id': qops.in_(*sid_strings)},
                                                   projection={
                                                       recordsm.OBJ_ID: 1,
                                                       recordsm.VERSION: 1,
                                                       recordsm.SNAPSHOT_HASH: 1
                                                   }):  # DB HIT
            remote_partial_records[recordsm.SnapshotId.from_dict(entry)] = entry

        # LOCAL
        # Find the local snapshots along with their hashes
        local_partial_records = {}
        for entry in self.archive.snapshots.find({'_id': qops.in_(*sid_strings)},
                                                 projection={
                                                     recordsm.OBJ_ID: 1,
                                                     recordsm.VERSION: 1,
                                                     recordsm.SNAPSHOT_HASH: 1
                                                 }):  # DB HIT
            local_partial_records[recordsm.SnapshotId.from_dict(entry)] = entry

        # Remove all those that match and log any that have conflicting hashes
        conflicting = []
        for sid, local_partial in local_partial_records.items():
            remote_record = remote_partial_records.pop(sid)
            if remote_record[recordsm.SNAPSHOT_HASH] != local_partial[recordsm.SNAPSHOT_HASH]:
                conflicting.append(sid)

        if conflicting:
            raise exceptions.MergeError(
                'Cannot merge, the following snapshots have conflicting hashes: {}'.format(
                    conflicting))

        # Finally, get all the records to merge and create merge operations
        ops = []
        files_to_transfer = []
        for remote_record in remote.archive.snapshots.find(
            {'_id': qops.in_(*map(str, remote_partial_records.keys()))}):  # DB HIT
            record = recordsm.DataRecord(**remote_record)
            ops.append(operations.Merge(record))
            files_in_record = record.get_files()
            if files_in_record:
                # Extract the second entry in the tuple as this contains the actual state dictionary
                files_to_transfer.extend(entry[1] for entry in files_in_record)

        # and write the new records into our archive
        if ops:
            # Copy the files first.  This way if the user cancels prematurely the files are there but no the objects
            # that refer to them.  The other way around would result in the objects being there but failing when
            # someone tries to load the files
            file_store = self.archive.file_store
            for file_dict in files_to_transfer:
                file_id = file_dict[expr.field_name(files.File.file_id)]
                filename = file_dict[expr.field_name(files.File.filename)] or ''

                with remote.archive.file_store.open_download_stream(file_id) as down_stream:
                    file_store.upload_from_stream_with_id(file_id, filename, down_stream)

            self._archive.bulk_write(ops)  # DB HIT

        return result_types.MergeResult(all_snapshots=remote_ref_graph.nodes,
                                        merged_snapshots=remote_partial_records.keys())

    @contextlib.contextmanager
    def in_transaction(self):
        """This context will either re-use an existing transaction, if one is currently taking place
        or create a new one if not."""
        current = self.current_transaction()
        if current is None:
            ctx = self.transaction()
        else:
            ctx = nullcontext(current)

        with ctx as trans:
            yield trans

    @contextlib.contextmanager
    def transaction(self):
        """Start a new transaction.  Will be nested if there is already one underway"""
        if self._transactions:
            # Start a nested one
            with self._transactions[-1].nested() as nested:
                self._transactions.append(nested)
                try:
                    yield nested
                except Exception:  # Need this so we can have 'else' pylint: disable=try-except-raise
                    raise
                else:
                    self._closing_transaction(nested)
                finally:
                    popped = self._transactions.pop()
                    assert popped is nested
        else:
            # New transaction
            trans = Transaction()
            self._transactions = [trans]

            try:
                yield trans
            except RollbackTransaction:
                pass
            else:
                self._closing_transaction(trans)
                self._commit_transaction(trans)
            finally:
                assert len(self._transactions) == 1
                assert self._transactions[0] is trans
                self._transactions = None

    def current_transaction(self) -> Optional[Transaction]:
        """Get the current transaction if there is one, otherwise returns None"""
        if not self._transactions:
            return None
        return self._transactions[-1]

    def _closing_transaction(self, trans: Transaction):
        conflicting = set()

        # Filter out the deleted records
        del_ops = filter(
            lambda _: isinstance(_, operations.Insert) and _.record.is_deleted_record(),
            trans.staged)

        obj_ids = set(operation.obj_id for operation in del_ops)
        ref_graph = self.references.get_obj_ref_graph(*obj_ids, direction=archives.INCOMING)
        for obj_id in obj_ids:
            for edge in ref_graph.in_edges(obj_id):
                conflicting.add(edge[1])

        if conflicting:
            raise exceptions.ReferenceError('Cannot perform delete', conflicting)

    def _commit_transaction(self, trans: Transaction):
        """Commit a transaction that is finishing"""
        # Perform the database operations first because if these fail we shouldn't update ourselves
        # Save any records that were staged for archiving
        if trans.staged:
            self._archive.bulk_write(trans.staged)

        # Now all is good we can update

        # Live objects
        self._live_objects.update(trans.live_objects)
        # Deleted objects
        for deleted in trans.deleted:
            try:
                self._live_objects.remove(deleted)
            except exceptions.NotFound:
                pass

        # Snapshots
        for ref, obj in trans.snapshots.items():
            self._snapshots_objects[obj] = ref

        # Finally update the metadata as this is least important
        # Metas
        if trans.metas:
            self._archive.meta_set_many(trans.metas)

    def _load_object_from_record(self, record: recordsm.DataRecord):
        depositor = self._live_depositor

        # Try getting the object from the our dict of up to date ones
        obj_id = record.obj_id
        try:
            return self.get_obj(obj_id)
        except exceptions.NotFound:
            pass

        with self.in_transaction() as trans:

            if trans.is_deleted(obj_id):
                raise exceptions.ObjectDeleted(obj_id)

            if record.is_deleted_record():
                raise exceptions.ObjectDeleted(obj_id)

            logger.debug('Loading object from record: %s', record.snapshot_id)
            # Ok, just use the one from the archive
            return depositor.load_from_record(record)

    def _ensure_obj_id(self, obj_or_identifier):
        """
        This call will try and get an object id from the passed parameter.  Uses .to_obj_id() and raises NotFound if it
        is not possible to get the object id.
        """
        obj_id = self.to_obj_id(obj_or_identifier)
        if obj_id is None:
            raise exceptions.NotFound(f"Could not get an object id from '{obj_or_identifier}'")

        return obj_id

    def _prepare_obj_id(self, obj_id):
        if obj_id is None:
            return None

        # Convert object ids to the expected type before passing to archive
        try:
            return self._ensure_obj_id(obj_id)
        except exceptions.NotFound as exc:
            # Maybe it is multiple object ids
            if not isinstance(obj_id, Iterable):  # pylint: disable=isinstance-second-argument-not-valid-type
                raise TypeError(f"Cannot get object id(s) from '{obj_id}'") from exc

            return list(map(self._ensure_obj_id, obj_id))

    def _prepare_type_id(self, obj_type):
        if obj_type is None:
            return None

        try:
            return self.get_obj_type_id(obj_type)
        except TypeError as exc:
            # Maybe it is multiple type ids
            if not isinstance(obj_type, Iterable):  # pylint: disable=isinstance-second-argument-not-valid-type
                raise TypeError(f"Cannot get type id(s) from '{obj_type}'") from exc

            return list(map(self.get_obj_type_id, obj_type))

    def _record_builder_created(self,
                                builder: recordsm.DataRecordBuilder) -> recordsm.DataRecordBuilder:
        """Update a data record builder with standard information."""
        builder.extras.update(self.get_user_info())
        return builder

    def _new_snapshot_depositor(self):
        return depositors.SnapshotLoader(self)


class SnapshotLoadableRecord(recordsm.DataRecord):
    __slots__ = ()

    def __new__(cls, record_dict: dict, snapshot_loader: Callable[[recordsm.DataRecord], object]):
        loadable = super().__new__(cls, **record_dict)
        loadable._snapshot_loader = snapshot_loader
        return loadable

    def load(self) -> object:
        return self._snapshot_loader(self)


class LoadableRecord(recordsm.DataRecord):
    __slots__ = ()
    _obj_loader = None  # type: Optional[Callable[[recordsm.DataRecord], object]]
    _snapshot_loader = None  # type: Optional[Callable[[recordsm.DataRecord], object]]

    def __new__(cls, record_dict: dict, snapshot_loader: Callable[[recordsm.DataRecord], object],
                obj_loader: Callable[[recordsm.DataRecord], object]):
        loadable = super().__new__(cls, **record_dict)
        loadable._obj_loader = obj_loader
        loadable._snapshot_loader = snapshot_loader
        return loadable

    def load_snapshot(self) -> object:
        return self._snapshot_loader(self)  # pylint: disable=not-callable

    def load(self) -> object:
        return self._obj_loader(self)  # pylint: disable=not-callable
