from collections import namedtuple
import contextlib
import copy
import getpass
import logging
import socket
import typing
from typing import MutableMapping, Any, Optional, Mapping, Iterable, Union, Iterator, Dict
import weakref

from . import archives
from . import builtins
from . import defaults
from . import depositors
from . import refs
from . import exceptions
from . import helpers
from . import hist
from . import migrate
from . import operations
from . import process
from . import records
from . import types
from . import type_registry
from . import utils
from .transactions import RollbackTransaction, Transaction, LiveObjects

__all__ = 'Historian', 'ObjectEntry'

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

ObjectEntry = namedtuple('ObjectEntry', 'ref obj')
HistorianType = Union[helpers.TypeHelper, typing.Type[types.SavableObject]]


class Meta:
    """A class for grouping metadata related methods"""

    # Meta is a 'friend' of Historian and so can access privates pylint: disable=protected-access

    def __init__(self, historian):
        self._hist = historian  # type: 'Historian'
        self._sticky = {}

    @property
    def sticky(self) -> dict:
        return self._sticky

    def get(self, obj_or_identifier) -> Optional[dict]:
        """Get the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        """
        results = self.get_many((obj_or_identifier,))
        assert len(results) == 1
        meta = tuple(results.values())[0]
        return meta

    def get_many(self, obj_or_identifiers) -> Dict[Any, dict]:
        obj_ids = set(
            self._hist._ensure_obj_id(obj_or_identifier)
            for obj_or_identifier in obj_or_identifiers)
        trans = self._hist.current_transaction()
        if trans:
            # First, get what we can from the archive
            found = {}
            for obj_id in obj_ids:
                try:
                    found[obj_id] = trans.get_meta(obj_id)
                except exceptions.NotFound:
                    pass

            # Now get anything else from the archive
            obj_ids -= found.keys()
            if obj_ids:
                from_archive = self._hist.archive.meta_get_many(obj_ids)
                # Now put into the transaction so it doesn't look it up again.
                for obj_id in obj_ids:
                    trans.set_meta(obj_id, from_archive[obj_id])
                found.update(from_archive)

            return found

        # No transaction
        return self._hist.archive.meta_get_many(obj_ids)

    def set(self, obj_or_identifier, meta: Optional[Mapping]):
        """Set the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        obj_id = self._hist._ensure_obj_id(obj_or_identifier)
        trans = self._hist.current_transaction()
        if trans:
            return trans.set_meta(obj_id, meta)

        return self._hist.archive.meta_set(obj_id, meta)

    def set_many(self, metas: Mapping[Any, Optional[dict]]):
        mapped = {self._hist._ensure_obj_id(ident): meta for ident, meta in metas.items()}
        trans = self._hist.current_transaction()
        if trans:
            for entry in mapped.items():
                trans.set_meta(*entry)
        else:
            self._hist.archive.meta_set_many(mapped)

    def update(self, obj_or_identifier, meta: Mapping):
        """Update the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        obj_id = self._hist._ensure_obj_id(obj_or_identifier)
        trans = self._hist.current_transaction()
        if trans:
            # Update the metadata in the transaction
            try:
                current = trans.get_meta(obj_id)
            except exceptions.NotFound:
                current = self._hist.archive.meta_get(obj_id)  # Try the archive
                if current is None:
                    current = {}  # Ok, no meta

            current.update(meta)
            trans.set_meta(obj_id, current)
        else:
            self._hist.archive.meta_update(obj_id, meta)

    def update_many(self, metas: Mapping[Any, Optional[dict]]):
        mapped = {self._hist._ensure_obj_id(ident): meta for ident, meta in metas.items()}
        trans = self._hist.current_transaction()
        if trans:
            for entry in mapped.items():
                self.update(*entry)
        else:
            self._hist.archive.meta_update_many(mapped)

    def find(self, filter, obj_id=None):  # pylint: disable=redefined-builtin
        """Find metadata matching the given criteria.  Ever returned metadata dictionary will
        contain an 'obj_id' key which identifies the object it belongs to"""
        return self._hist.archive.meta_find(filter=filter, obj_id=obj_id)

    def create_index(self, keys, unique=False, where_exist=False):
        """Create an index on the metadata.  Takes either a single key or list of (key, direction)
         pairs

         :param keys: the key or keys to create the index on
         :param unique: if True, create a uniqueness constraint on this index
         :param where_exist: if True, only apply this index on documents that contain the key(s)
         """
        self._hist.archive.meta_create_index(keys, unique=unique, where_exist=where_exist)


class Historian:  # pylint: disable=too-many-public-methods, too-many-instance-attributes
    """The historian acts as a go-between between your python objects and the archive which is
    a persistent store of the records.  It will keep track of all live objects (i.e. those that
    have active references to them) that have been loaded and/or saved as well as enabling the
    user to lookup objects in the archive."""

    def __init__(self, archive: archives.Archive, equators=()):
        self._archive = archive
        self._equator = types.Equator(defaults.get_default_equators() + equators)
        # Register default types
        self._type_registry = type_registry.TypeRegistry()
        self.register_type(refs.ObjRef)
        self.register_type(helpers.SnapshotIdHelper())
        self.register_types(archive.get_types())

        # Snapshot objects -> reference. Objects that were loaded from historical snapshots
        self._snapshots_objects = utils.WeakObjectIdDict(
        )  # type: MutableMapping[Any, records.SnapshotId]
        self._live_objects = LiveObjects()

        self._transactions = None

        self._saving_set = set()

        self._user = getpass.getuser()
        self._hostname = socket.gethostname()

        self._live_depositor = depositors.LiveDepositor(self)
        self._meta = Meta(self)
        self._migrate = migrate.Migrations(self)
        self._references = hist.References(self)

    @property
    def archive(self):
        return self._archive

    @property
    def primitives(self) -> tuple:
        """A tuple of all the primitive types"""
        return types.PRIMITIVE_TYPES + (self._archive.get_id_type(),)

    @property
    def migrations(self) -> migrate.Migrations:
        """Access the migration possibilities"""
        return self._migrate

    @property
    def references(self) -> hist.References:
        """Access the references possibilities"""
        return self._references

    def create_file(self, filename: str = None, encoding: str = None) -> builtins.BaseFile:
        """Create a new file.  The historian will supply file type compatible with the archive in
         use."""
        return self._archive.create_file(filename, encoding)

    def save(self, *objs):
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
                        "Supplied tuples can only contain (object, meta), got '{}'".format(entry))
            else:
                entry = (entry,)
            to_save.append(entry)

        ids = []
        with self.transaction():
            for entry in to_save:
                ids.append(self.save_one(*entry))

        if len(objs) == 1:
            return ids[0]

        return ids

    def save_one(self, obj, meta: dict = None):
        """Save the object returning an object id.  If metadata is supplied it will be set on the
        object.

        Developer note: this is the front end point-of-entry for a user/client code saving an object
        however subsequent objects being saved in this transaction will only go through _save_object
        and therefore any code common to all objects being saved should possibly go there.
        """
        if obj in self._snapshots_objects:
            raise exceptions.ModificationError(
                "Cannot save a snapshot object, that would rewrite history!")

        if meta and not isinstance(meta, dict):
            raise TypeError("Metadata must be a dictionary, got type '{}'".format(type(meta)))

        # Save the object and metadata
        with self.transaction():
            record = self._save_object(obj, self._live_depositor)
            if meta:
                self.meta.update(record.obj_id, meta)

        return record.obj_id

    def is_known(self, obj) -> bool:
        """Check if an object has ever been saved and is therefore known to the historian

        :return: True if ever saved, False otherwise
        """
        return self.get_obj_id(obj) is not None

    def replace(self, old, new):
        """Replace a live object with a new version.

        This is especially useful if you have made a copy of an object and modified it but you want
        to continue the history of the object as the original rather than a brand new object.  Then
        just replace the old object with the new one by calling this function.
        """
        assert not self.current_transaction(
        ), "Can't replace during a transaction for the time being"
        assert isinstance(new, type(old)), "Can't replace type '{} with type '{}!".format(
            type(old), type(new))

        # Get the current record and replace the object with the new one
        record = self._live_objects.get_record(old)
        self._live_objects.remove(record.obj_id)
        self._live_objects.insert(new, record)

        # Make sure creators is correct as well
        process.CreatorsRegistry.set_creator(new, process.CreatorsRegistry.get_creator(old))

    def load_snapshot(self, snapshot_id: records.SnapshotId) -> Any:
        return self._new_snapshot_depositor().load(snapshot_id)

    def load(self, *obj_ids_or_refs):
        """Load object(s) or snapshot(s)."""
        loaded = []
        for entry in obj_ids_or_refs:
            loaded.append(self.load_one(entry))

        if len(obj_ids_or_refs) == 1:
            return loaded[0]

        return loaded

    def load_one(self, obj_id_or_ref):
        """Load one object or shot from the database"""
        if isinstance(obj_id_or_ref, records.SnapshotId):
            return self.load_snapshot(obj_id_or_ref)

        return self._load_object(obj_id_or_ref, self._live_depositor)

    def sync(self, obj) -> bool:
        """Update an object with the latest state in the database.
        If there is no new version in the archive then the current version remains
        unchanged including any modifications.

        :return: True if the object was updated, False otherwise
        """
        obj_id = self.get_obj_id(obj)
        if obj_id is None:
            # Never saved so the object is as up to date as can be!
            return False

        sid = self._get_latest_snapshot_reference(obj_id)

        try:
            record = next(self._archive.find(obj_id=obj_id, version=-1))
        except StopIteration:
            raise exceptions.NotFound(obj_id)

        if record.is_deleted_record():
            raise exceptions.ObjectDeleted("Object with id '{}' has been deleted".format(obj_id))

        if sid.version == self.get_snapshot_id(obj).version:
            # Nothing has changed
            return False

        # The one in the archive is newer, so use that
        return self._live_depositor.update_from_record(obj, record)

    def copy(self, obj):
        """Create a shallow copy of the object, save that copy and return it"""
        with self.transaction() as trans:
            record = self._save_object(obj, self._live_depositor)
            copy_builder = record.copy_builder(obj_id=self._archive.create_archive_id())
            self._record_builder_created(copy_builder)

            # Copy the object and record
            obj_copy = copy.copy(obj)
            obj_copy_record = copy_builder.build()

            # Insert all the new objects into the transaction
            trans.insert_live_object(obj_copy, obj_copy_record)
            trans.stage(operations.Insert(obj_copy_record))

        return obj_copy

    def delete(self, obj_or_identifier):
        """Delete an object.

        :raises mincepy.NotFound: if the object cannot be found (potentially because it was
            already deleted)
        """
        # We need a record to be able to build the delete record
        obj_id = self._ensure_obj_id(obj_or_identifier)

        try:
            record = self.get_current_record(self.get_obj(obj_id))
        except exceptions.ObjectDeleted:
            # Object deleted already do reraise
            raise
        except exceptions.NotFound as exc:
            # Have a look in the archive
            results = self.archive.find(obj_id=obj_id, version=-1)
            try:
                record = next(results)
            except StopIteration:
                # not even in the archive
                raise exc

        with self.transaction() as trans:
            builder = records.make_deleted_builder(record)
            deleted_record = self._record_builder_created(builder).build()
            trans.delete(obj_id)
            trans.stage(operations.Insert(deleted_record))

    def history(
            self,
            obj_or_obj_id,
            idx_or_slice='*',
            as_objects=True) -> [typing.Sequence[ObjectEntry], typing.Sequence[records.DataRecord]]:
        """
        Get a sequence of object ids and instances from the history of the given object.

        :param obj_or_obj_id: The instance or id of the object to get the history for
        :param idx_or_slice: The particular index or a slice of which historical versions to get
        :param as_objects: if True return the object instances, otherwise returns the DataRecords

        Example:
        >>> historian = get_historian()
        >>> car = Car('ferrari', 'white')
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
        snapshot_refs = self._archive.get_snapshot_ids(obj_id)
        indices = utils.to_slice(idx_or_slice)
        to_get = snapshot_refs[indices]
        if as_objects:
            return [ObjectEntry(ref, self.load_snapshot(ref)) for ref in to_get]

        return [self._archive.load(ref) for ref in to_get]

    @property
    def meta(self) -> Meta:
        """Access to functions that operate on the metadata"""
        return self._meta

    def get_current_record(self, obj) -> records.DataRecord:
        """Get a record for an object known to the historian"""
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

    def get_obj_id(self, obj) -> Any:
        """Get the object ID for a live object.

        :return: the object id or None if the object is not known to the historian
        """
        trans = self.current_transaction()
        if trans is not None:
            try:
                return trans.get_reference_for_live_object(obj).obj_id
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

    def get_obj(self, obj_id):
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

    def is_saved(self, obj) -> bool:
        """Test if an object is saved with this historian.  This is equivalent to
        `historian.get_obj_id(obj) is not None`."""
        return self.get_obj_id(obj) is not None

    def to_obj_id(self, obj_or_identifier):
        """
        This call will try and get an object id from the passed parameter.  There are three
        possibilities:

        1. Passed an object ID in which case it will be returned unchanged
        2. Passed a live object instance, in which case the id of that object will be returned
        3. Passed a type that can be understood by the archive as an object id e.g. a string of
           version, in which case the archive will attempt to convert it

        Returns None if neither of these cases were True.
        """
        if self.is_obj_id(obj_or_identifier):
            return obj_or_identifier

        try:
            # Try creating it for the user by calling the constructor with the argument passed.
            # This helps for common obj id types which can be constructed from a string
            return self._archive.construct_archive_id(obj_or_identifier)
        except (ValueError, TypeError):
            # Maybe we've been passed an object
            pass

        try:
            return self.get_obj_id(obj_or_identifier)
        except exceptions.NotFound:
            pass

        # Out of options
        return None

    def get_snapshot_id(self, obj) -> records.SnapshotId:
        """Get the current snapshot id for a live object"""
        trans = self.current_transaction()
        if trans:
            try:
                return trans.get_reference_for_live_object(obj)
            except exceptions.NotFound:
                pass

        return self._live_objects.get_record(obj).snapshot_id

    def hash(self, obj):
        return self._equator.hash(obj)

    def eq(self, one, other):  # pylint: disable=invalid-name
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
        return isinstance(obj, self.primitives)

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
        if auto_register and issubclass(type_id_or_type, types.SavableObject):
            self._ensure_compatible(type_id_or_type)

        return self._type_registry.get_helper(type_id_or_type)

    # endregion

    def find(self,
             obj_type=None,
             obj_id=None,
             version: int = -1,
             state=None,
             meta: dict = None,
             sort=None,
             limit=0,
             skip=0) -> Iterator[Any]:
        """
        .. _MongoDB: https://docs.mongodb.com/manual/tutorial/query-documents/

        Find objects.  This call will search the archive for objects matching the given criteria.
        In many cases the main arguments of interest will be `state` and `meta` which allow you to
        apply filters on the stored state of the object and metadata respectively.  To understand
        how the state is stored in the database (and therefore how to apply filters to it) it may
        be necessary to look at the details of the `save_instance_state()` method for that type.
        Metadata is always a dictionary containing primitives (strings, dicts, lists, etc).

        For the most part, the filter syntax of `mincepy` conforms to that of `MongoDB`_ with
        convenience functions locate in :py:mod:`mincepy.qops` that can make it easier to
        to build a query.

        Examples:

        Find all :py:class:`~mincepy.testing.Car`s that are brown or red:

        >>> import mincepy
        >>> historian.find(state=dict(colour=mincepy.q.in_('brown', 'red')))

        Find all people that are older than 34 and live in Edinburgh:

        >>> historian.find(state=dict(age=mincepy.q.gt_(34)), meta=dict(city='Edinburgh'))

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
        # pylint: disable=too-many-arguments
        results = self.find_records(obj_type=obj_type,
                                    obj_id=obj_id,
                                    version=version,
                                    state=state,
                                    meta=meta,
                                    sort=sort,
                                    limit=limit,
                                    skip=skip)
        for result in results:
            yield self.load(result.obj_id)

    def find_records(self,
                     obj_type=None,
                     obj_id=None,
                     version: int = -1,
                     state=None,
                     meta: dict = None,
                     sort=None,
                     limit=0,
                     skip=0) -> Iterator[records.DataRecord]:
        """Find records

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
        # pylint: disable=too-many-arguments
        type_id = obj_type
        if obj_type is not None:
            try:
                type_id = self.get_obj_type_id(obj_type)
            except TypeError:
                pass
        results = self._archive.find(obj_id=obj_id,
                                     type_id=type_id,
                                     state=state,
                                     version=version,
                                     meta=meta,
                                     sort=sort,
                                     limit=limit,
                                     skip=skip)
        yield from results

    def get_creator(self, obj_or_identifier):
        """Get the object that created the passed object"""
        if not self.is_obj_id(obj_or_identifier):
            # Object instance, try our creators cache
            try:
                return process.CreatorsRegistry.get_creator(obj_or_identifier)
            except KeyError:
                pass

        creator_id = self.created_by(obj_or_identifier)
        return self.load_one(creator_id)

    def created_by(self, obj_or_identifier):
        """Return the id of the object that created the passed object"""
        try:
            record = self.get_current_record(obj_or_identifier)
        except exceptions.NotFound:
            if not self.is_obj_id(obj_or_identifier):
                raise

            try:
                record = next(self._archive.find(obj_id=obj_or_identifier, version=-1))
            except StopIteration:
                raise exceptions.NotFound(obj_or_identifier)

        return record.created_by

    def get_user_info(self) -> dict:
        """Get information about the current user and host"""
        user_info = {}
        if self._user:
            user_info[records.ExtraKeys.USER] = self._user
        if self._hostname:
            user_info[records.ExtraKeys.HOSTNAME] = self._hostname
        return user_info

    @contextlib.contextmanager
    def transaction(self):
        """Start a new transaction.  Will be nested if there is already one underway"""
        if self._transactions:
            # Start a nested one
            with self._transactions[-1].nested() as nested:
                self._transactions.append(nested)
                try:
                    yield nested
                finally:
                    self._closing_transaction(nested)
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
        pass

    def _commit_transaction(self, trans: Transaction):
        """Commit a transaction that is finishing"""
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

        # Save any records that were staged for archiving
        if trans.staged:
            self._archive.bulk_write(trans.staged)

        # Metas
        for obj_id, meta in trans.metas.items():
            self._archive.meta_set(obj_id, meta)

    def _get_latest_snapshot_reference(self, obj_id) -> records.SnapshotId:
        """Given an object id this will return a reference to the latest snapshot"""
        try:
            return self._archive.get_snapshot_ids(obj_id)[-1]
        except IndexError:
            raise exceptions.NotFound("Object with id '{}' not found.".format(obj_id))

    def _load_object(self, obj_id, depositor: depositors.LiveDepositor):
        obj_id = self._ensure_obj_id(obj_id)

        with self.transaction() as trans:
            # Try getting the object from the our dict of up to date ones
            try:
                return trans.get_live_object(obj_id)
            except exceptions.NotFound:
                pass

            if trans.is_deleted(obj_id):
                raise exceptions.ObjectDeleted(obj_id)

            # Couldn't find it, so let's check if we have one and check if it is up to date
            results = tuple(self.archive.find(obj_id, version=-1))
            if not results:
                raise exceptions.NotFound(obj_id)
            record = results[0]

            if record.is_deleted_record():
                raise exceptions.ObjectDeleted(obj_id)

            try:
                obj = self._live_objects.get_object(obj_id)
            except exceptions.NotFound:
                logger.debug("Loading object from record: %s", record.snapshot_id)
                # Ok, just use the one from the archive
                return depositor.load_from_record(record)
            else:
                if record.version != self.get_snapshot_id(obj).version:
                    # The one in the archive is newer, so use that
                    logger.debug("Updating object from record: %s", record.snapshot_id)
                    depositor.update_from_record(obj, record)

                return obj

    def _save_object(self, obj, depositor) -> records.DataRecord:
        with self.transaction() as trans:
            try:
                helper = self._ensure_compatible(type(obj))
            except TypeError:
                raise TypeError("Object is incompatible with the historian: {}".format(obj))

            # Check if an object is already being saved in the transaction
            try:
                record = trans.get_record_for_live_object(obj)
                return record
            except exceptions.NotFound:
                pass

            with self._saving(obj):
                # Ok, have to save it
                current_hash = self.hash(obj)

                try:
                    # Let's see if we have a record at all
                    record = self._live_objects.get_record(obj)
                except exceptions.NotFound:
                    # Object being saved for the first time
                    builder = self._create_builder(obj, dict(snapshot_hash=current_hash))
                    record = depositor.save_from_builder(obj, builder)
                    if self.meta.sticky:
                        # Apply the sticky meta
                        trans.set_meta(record.obj_id, self.meta.sticky)
                    return record
                else:
                    if helper.IMMUTABLE:
                        logger.info("Tried to save immutable object with id '%s' again",
                                    record.obj_id)
                        return record

                    # Check if our record is up to date
                    with self.transaction() as transaction:
                        loaded_obj = self._new_snapshot_depositor().load_from_record(record)

                        if current_hash == record.snapshot_hash and self.eq(obj, loaded_obj):
                            # Objects identical
                            transaction.rollback()
                        else:
                            builder = record.child_builder(snapshot_hash=current_hash)
                            self._record_builder_created(builder)
                            record = depositor.save_from_builder(obj, builder)

                    return record

    def _create_builder(self, obj, additional=None):
        additional = additional or {}
        helper = self._ensure_compatible(type(obj))
        builder = records.DataRecord.new_builder(type_id=helper.TYPE_ID,
                                                 obj_id=self._archive.create_archive_id(),
                                                 version=0)
        self._record_builder_created(builder)
        builder.update(additional)
        return builder

    def _ensure_obj_id(self, obj_or_identifier):
        """
        This call will try and get an object id from the passed parameter.  There are three
        possibilities:
            1. Passed an object ID in which case it will be returned unchanged
            2. Passed a live object instance, in which case the id of that object will be returned
            3. Passed a type that can be understood by the archive as an object id e.g. a string of
               version, in which case the archive will attempt to convert it
        """
        obj_id = self.to_obj_id(obj_or_identifier)
        if obj_id is None:
            raise exceptions.NotFound(
                "Could not get an object id from '{}'".format(obj_or_identifier))

        return obj_id

    def _ensure_compatible(self, obj_type) -> helpers.TypeHelper:
        if obj_type not in self._type_registry:
            return self.register_type(obj_type)

        return self._type_registry.get_helper_from_obj_type(obj_type)

    @contextlib.contextmanager
    def _saving(self, obj):
        obj_id = id(obj)
        if obj_id in self._saving_set:
            raise RuntimeError(
                "The object is already being saved, this cannot be called twice and suggests "
                "a circular reference is being made")
        self._saving_set.add(obj_id)
        try:
            yield
        finally:
            self._saving_set.remove(obj_id)

    def _record_builder_created(self,
                                builder: records.DataRecordBuilder) -> records.DataRecordBuilder:
        """Update a data record builder with standard information."""
        builder.extras.update(self.get_user_info())
        return builder

    def _new_snapshot_depositor(self):
        return depositors.SnapshotLoader(self)
