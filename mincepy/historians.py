from collections import namedtuple
import contextlib
import copy
import getpass
import logging
import socket
import typing
from typing import MutableMapping, Any, Optional, Mapping, Iterable, Union
import weakref

import deprecation

from . import archives
from . import builtins
from . import defaults
from . import depositors
from . import refs
from . import exceptions
from . import helpers
from . import process
from . import records
from . import types
from . import type_registry
from . import version
from . import utils
from .transactions import RollbackTransaction, Transaction, LiveObjects

__all__ = 'Historian', 'ObjectEntry'

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

ObjectEntry = namedtuple('ObjectEntry', 'ref obj')
HistorianType = Union[helpers.TypeHelper, typing.Type[types.SavableObject]]


class Meta:
    """A class for grouping metadata related methods"""

    def __init__(self, historian):
        self._historian = historian
        self._sticky = {}

    @property
    def sticky(self) -> dict:
        return self._sticky

    def get(self, obj_or_identifier) -> Optional[dict]:
        """Get the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        """
        return self._historian.get_meta(obj_or_identifier)

    def set(self, obj_or_identifier, meta: Optional[Mapping]):
        """Set the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        return self._historian.set_meta(obj_or_identifier, meta)

    def update(self, obj_or_identifier, meta: Mapping):
        """Update the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        return self._historian.update_meta(obj_or_identifier, meta)

    def find(self, filter):
        """Find metadata matching the given criteria.  Ever returned metadata dictionary will
        contain an 'obj_id' key which identifies the object it belongs to"""
        return self._historian.archive.find_meta(filter=filter)

    def efind(self, **filter):
        """Easy find.  Doesn't have any of the more complete options that find() has but allows
        the filter to be specified as keyword value pairs which is often more convenient if the
        user is happy to get all results"""
        return self.find(filter=filter)


class Historian:

    def __init__(self, archive: archives.Archive, equators=()):
        self._archive = archive
        self._equator = types.Equator(defaults.get_default_equators() + equators)
        # Register default types
        self._type_registry = type_registry.TypeRegistry()
        self.register_type(refs.ObjRef)
        self.register_type(helpers.SnapshotRefHelper())
        self.register_types(archive.get_types())

        # Snapshot objects -> reference. Objects that were loaded from historical snapshots
        self._snapshots_objects = utils.WeakObjectIdDict()  # type: MutableMapping[Any, records.Ref]
        self._live_objects = LiveObjects()

        # Staged objects that have been created but not saved
        self._creators = utils.WeakObjectIdDict()  # type: MutableMapping[typing.Any, Any]
        self._type_ids = {}

        self._transactions = None

        self._saving_set = set()

        self._user = getpass.getuser()
        self._hostname = socket.gethostname()

        self._meta = Meta(self)

    @deprecation.deprecated(deprecated_in="0.10.3",
                            removed_in="0.11.0",
                            current_version=version.__version__,
                            details="Use .load or .load_one instead")
    def load_object(self, obj_id):
        return self._load_object(obj_id, depositors.LiveDepositor(self))

    @deprecation.deprecated(deprecated_in="0.10.3",
                            removed_in="0.11.0",
                            current_version=version.__version__,
                            details="Use .save or .saved_one instead")
    def save_object(self, obj) -> records.DataRecord:
        return self._save_object(obj, depositors.LiveDepositor(self))

    @deprecation.deprecated(deprecated_in="0.10.3",
                            removed_in="1.0",
                            current_version=version.__version__,
                            details="Use .archive property instead")
    def get_archive(self):
        return self._archive

    @property
    @deprecation.deprecated(deprecated_in="0.10.3",
                            removed_in="0.11.0",
                            current_version=version.__version__,
                            details="Use .meta.sticky instead")
    def sticky_meta(self) -> dict:
        """Sticky metadata that is set on any object being saved for the first time.
        If the user supplies metadata at save time this will take priority but in the following
        way: the stick meta will be used as a base but updated with the user supplied meta i.e.

            meta = deepcopy(stick_meta.copy)
            meta.update(user_meta)
        """
        return self.meta.sticky

    @property
    def archive(self):
        return self._archive

    def created(self, obj):
        """Called when an object is created.  The historian tracks the creator for saving when
        the object is saved"""
        creator = process.Process.current_process()
        if creator is not None:
            self._creators[obj] = creator

    def create_file(self, filename: str = None, encoding: str = None) -> builtins.BaseFile:
        """Create a new file.  The historian will supply file type compatible with the archive in
         use."""
        return self._archive.create_file(filename, encoding)

    def save(self, *objs, with_meta=None, return_sref=False):
        """Save or more objects in the history producing corresponding object identifiers

        :param objs: the object(s) to save
        :param with_meta: the object(s) metadata (has to be a sequence if more than one)
        :param return_sref: if True will return a snapshot reference, otherwise just the object id
        """
        if with_meta is not None:
            if len(objs) == 1:
                with_meta = (with_meta,)
            else:
                assert len(objs) == len(with_meta), \
                    "The metadata should be a sequence with the same number of entries as the " \
                    "number of objects"
        else:
            with_meta = [None] * len(objs)

        with self.transaction():
            ids = []
            for obj, meta in zip(objs, with_meta):
                ids.append(self.save_one(obj, meta, return_sref))
            if len(ids) == 1:
                return ids[0]

            return ids

    def save_one(self, obj, with_meta=None, return_sref=False):
        """Save the object in the history producing a unique id.

        Developer note: this is the front end point-of-entry for a user/client code saving an object
        however subsequent objects being saved in this transaction will only go through _save_object
        and therefore any code common to all objects being saved should possibly go there.
        """
        if obj in self._snapshots_objects:
            raise exceptions.ModificationError(
                "Cannot save a snapshot object, that would rewrite history!")

        if with_meta and not isinstance(with_meta, dict):
            raise TypeError("Metadata must be a dictionary, got type '{}'".format(type(with_meta)))

        # Save the object and metadata
        with self.transaction():
            record = self._save_object(obj, depositors.LiveDepositor(self))
            if with_meta:
                self.meta.update(record.obj_id, with_meta)

        if return_sref:
            return record.get_reference()

        return record.obj_id

    def is_known(self, obj) -> bool:
        """Check if an object has ever been saved and is therefore known to the historian

        :return: True if ever saved, False otherwise
        """
        return self.get_obj_id(obj) is not None

    def replace(self, old, new):
        """Replace a live object with a new version.

        This is especially useful if you have made a copy of an object and modified it but you want to
        continue the history of the object as the original rather than a brand new object.  Then just
        replace the old object with the new one by calling this function.
        """
        assert not self.current_transaction(
        ), "Can't replace during a transaction for the time being"
        assert isinstance(new, type(old)), "Can't replace type '{} with type '{}!".format(
            type(old), type(new))

        # Get the current record and replace the object with the new one
        record = self._live_objects.get_record(old)
        self._live_objects.delete(old)
        self._live_objects.insert(new, record)

        # Make sure creators is correct as well
        try:
            self._creators[new] = self._creators.pop(old)
        except KeyError:
            pass

    def load_snapshot(self, reference: records.Ref) -> Any:
        return depositors.SnapshotLoader(self).load(reference)

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
        if isinstance(obj_id_or_ref, records.Ref):
            return self.load_snapshot(obj_id_or_ref)

        return self._load_object(obj_id_or_ref, depositors.LiveDepositor(self))

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

        ref = self._get_latest_snapshot_reference(obj_id)
        archive_record = self._archive.load(ref)
        if archive_record.is_deleted_record():
            raise exceptions.ObjectDeleted("Object with id '{}' has been deleted".format(obj_id))

        if ref.version == self.get_snapshot_ref(obj).version:
            # Nothing has changed
            return False

        # The one in the archive is newer, so use that
        depositor = depositors.LiveDepositor(self)
        return depositor.update_from_record(obj, archive_record)

    def copy(self, obj):
        """Create a shallow copy of the object, save that copy and return it"""
        with self.transaction() as trans:
            record = self._save_object(obj, depositors.LiveDepositor(self))
            copy_builder = record.copy_builder(obj_id=self._archive.create_archive_id())
            self._record_builder_created(copy_builder)

            # Copy the object and record
            obj_copy = copy.copy(obj)
            obj_copy_record = copy_builder.build()

            # Insert all the new objects into the transaction
            trans.insert_live_object(obj_copy, obj_copy_record)
            trans.stage(obj_copy_record)

        return obj_copy

    def delete(self, obj):
        """Delete a live object"""
        record = self.get_current_record(obj)
        with self.transaction() as trans:
            builder = records.make_deleted_builder(record)
            deleted_record = self._record_builder_created(builder).build()
            trans.stage(deleted_record)
        self._live_objects.delete(obj)

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
        snapshot_refs = self._archive.get_snapshot_refs(obj_id)
        indices = utils.to_slice(idx_or_slice)
        to_get = snapshot_refs[indices]
        if as_objects:
            return [ObjectEntry(ref, self.load_snapshot(ref)) for ref in to_get]

        return [self._archive.load(ref) for ref in to_get]

    # region Metadata

    @property
    def meta(self):
        return self._meta

    def get_meta(self, obj_or_identifier) -> dict:
        """Get the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        """
        obj_id = self._ensure_obj_id(obj_or_identifier)
        trans = self.current_transaction()
        if trans:
            try:
                return trans.get_meta(obj_id)
            except exceptions.NotFound:
                current = self._archive.get_meta(obj_id)  # Try the archive
                if current is None:
                    current = {}  # Ok, no meta
                trans.set_meta(obj_id, current)  # Cache the meta in the transaction
                return current
        else:
            return self.archive.get_meta(obj_id)

    def set_meta(self, obj_or_identifier, meta: Optional[Mapping]):
        """Set the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        obj_id = self._ensure_obj_id(obj_or_identifier)
        trans = self.current_transaction()
        if trans:
            return trans.set_meta(obj_id, meta)
        else:
            return self.archive.set_meta(obj_id, meta)

    def update_meta(self, obj_or_identifier, meta: Mapping):
        """Update the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        obj_id = self._ensure_obj_id(obj_or_identifier)
        trans = self.current_transaction()
        if trans:
            # Update the metadata in the transaction
            try:
                current = trans.get_meta(obj_id)
            except exceptions.NotFound:
                current = self._archive.get_meta(obj_id)  # Try the archive
                if current is None:
                    current = {}  # Ok, no meta

            current.update(meta)
            trans.set_meta(obj_id, current)
        else:
            self.archive.update_meta(obj_id, meta)

    # endregion

    def get_current_record(self, obj) -> records.DataRecord:
        """Get a record for an object known to the historian"""
        trans = self.current_transaction()
        # Try the transaction first
        if trans:
            try:
                return trans.get_record_for_live_object(obj)
            except exceptions.NotFound:
                pass

        return self._live_objects.get_record(obj)

    def get_obj_id(self, obj):
        """Get the object ID for a live object.

        :return: the object id or None if never saved
        """
        try:
            return self.get_current_record(obj).obj_id
        except exceptions.NotFound:
            return None

    def get_obj(self, obj_id):
        """Get a currently live object"""
        trans = self.current_transaction()
        if trans:
            try:
                return trans.get_live_object(obj_id)
            except ValueError:
                pass

        return self._live_objects.get_object(obj_id)

    def get_snapshot_ref(self, obj):
        """Get the current snapshot reference for a live object"""
        trans = self.current_transaction()
        if trans:
            try:
                return trans.get_reference_for_live_object(obj)
            except exceptions.NotFound:
                pass

        return self._live_objects.get_record(obj).get_reference()

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

    def is_primitive(self, obj) -> bool:
        """Check if the object is one of the primitives and should be saved by value in the
        archive"""
        primitives = types.PRIMITIVE_TYPES + (
            self._archive.get_id_type(),) + self._archive.get_extra_primitives()
        return isinstance(obj, primitives)

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
             version: int = -1,
             state: dict = None,
             meta: dict = None,
             sort=None,
             limit=0,
             skip=0,
             as_objects=True):
        """Find entries in the archive

        :param obj_type: the object type to look for
        :param version: the version of the object to retrieve, -1 means latest
        :param state: the criteria on the state of the object to apply
        :param meta: the search criteria to apply on the metadata of the object
        :param sort: the sort criteria
        :param limit: the maximum number of results to return, 0 means unlimited
        :param skip: the page to get results from
        :param as_objects: if True returns the live object instances, False returns the DataRecords
        """
        type_id = obj_type
        if obj_type is not None:
            try:
                type_id = self.get_obj_type_id(obj_type)
            except TypeError:
                pass
        results = self._archive.find(type_id=type_id,
                                     state=state,
                                     version=version,
                                     meta=meta,
                                     sort=sort,
                                     limit=limit,
                                     skip=skip)
        if as_objects:
            for result in results:
                yield self.load(result.obj_id)
        else:
            yield from results

    def get_creator(self, obj_or_identifier):
        """Get the object that created the passed object"""
        if not self.is_obj_id(obj_or_identifier):
            # Object instance, try our creators cache
            try:
                return self._creators[obj_or_identifier]
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
            record = self._archive.load(self._get_latest_snapshot_reference(obj_or_identifier))

        return record.created_by

    def get_user_info(self) -> dict:
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

        # Snapshots
        for ref, obj in trans.snapshots.items():
            self._snapshots_objects[obj] = ref

        # Save any records that were staged for archiving
        if trans.staged:
            self._archive.save_many(trans.staged)

        # Metas
        for obj_id, meta in trans.metas.items():
            self._archive.set_meta(obj_id, meta)

    def _get_latest_snapshot_reference(self, obj_id) -> records.Ref:
        """Given an object id this will return a reference to the latest snapshot"""
        try:
            return self._archive.get_snapshot_refs(obj_id)[-1]
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

            # Couldn't find it, so let's check if we have one and check if it is up to date
            ref = self._get_latest_snapshot_reference(obj_id)
            record = self._archive.load(ref)
            if record.is_deleted_record():
                raise exceptions.ObjectDeleted(
                    "Object with id '{}' has been deleted".format(obj_id))

            try:
                obj = self._live_objects.get_object(obj_id)
            except exceptions.NotFound:
                # Ok, just use the one from the archive
                return depositor.load_from_record(record)
            else:
                if record.version != self.get_snapshot_ref(obj).version:
                    # The one in the archive is newer, so use that
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
                        loaded_obj = depositors.SnapshotLoader(self).load_from_record(record)

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
            1. It is passed an object ID in which case it will be returned directly
            2. It is passed an object instance, in which case it will try and get the id from
               memory
            3. It is passed a type that can be used as a constructor argument to the object id type
               in which case it will construct it and return the result
        """
        if self.is_obj_id(obj_or_identifier):
            obj_id = obj_or_identifier

        else:
            try:
                # Try creating it for the user by calling the constructor with the argument passed.
                # This helps for common obj id types which can be constructed from a string
                obj_id = self._archive.construct_archive_id(obj_or_identifier)
            except (ValueError, TypeError):
                # Maybe we've been passed an object
                obj_id = self.get_obj_id(obj_or_identifier)

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
