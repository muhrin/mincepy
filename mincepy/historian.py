from collections import namedtuple
import contextlib
import copy
import typing
from typing import MutableMapping, Any, Optional, Iterable, Mapping
import weakref

from . import archive
from . import defaults
from . import depositors
from . import exceptions
from . import helpers
from . import process
from . import types
from . import type_registry
from . import utils
from .transactions import RollbackTransaction, Transaction, LiveObjects

__all__ = 'Historian', 'ObjectEntry'

ObjectEntry = namedtuple('ObjectEntry', 'ref obj')


class Historian:

    def __init__(self, archive: archive.Archive, equators=()):
        self._archive = archive
        self._equator = types.Equator(defaults.get_default_equators() + equators)

        # Snapshot objects -> reference. Objects that were loaded from historical snapshots
        self._snapshots_objects = utils.WeakObjectIdDict()  # type: MutableMapping[Any, archive.Ref]

        self._live_objects = LiveObjects()

        self._type_registry = type_registry.TypeRegistry()

        # Staged objects that have been created but not saved
        self._creators = utils.WeakObjectIdDict()  # type: MutableMapping[typing.Any, Any]
        self._type_ids = {}

        self._transactions = None

        self.register_types(archive.get_types())

    def get_archive(self):
        return self._archive

    def created(self, obj):
        """Called when an object is created.  The historian tracks the creator for saving when
        the object is saved"""
        creator = process.Process.current_process()
        if creator is not None:
            self._creators[obj] = creator

    def create_file(self, filename: str = None, encoding: str = None):
        """Create a new file.  The historian will supply file type compatible with the archive in use."""
        return self._archive.create_file(filename, encoding)

    def save(self, *objs, with_meta=None):
        """Save or more objects in the history producing corresponding object identifiers"""
        if with_meta is not None:
            if len(objs) == 1:
                with_meta = (with_meta,)
            else:
                assert len(objs) == len(with_meta), \
                    "The metadata should be a sequence with the same number of entries as the number of objects"
        else:
            with_meta = [None] * len(objs)

        with self.transaction():
            ids = []
            for obj, meta in zip(objs, with_meta):
                ids.append(self.save_one(obj, meta))
            if len(ids) == 1:
                return ids[0]

            return ids

    def save_one(self, obj, with_meta=None):
        """Save the object in the history producing a unique id"""
        if obj in self._snapshots_objects:
            raise exceptions.ModificationError("Cannot save a snapshot object, that would rewrite history!")

        record = self.save_object(obj)
        if with_meta is not None:
            self._archive.set_meta(record.obj_id, with_meta)
        return record.obj_id

    def replace(self, old, new):
        """Replace a live object with a new version.

        This is especially useful if you have made a copy of an object and modified it but you want to
        continue the history of the object as the original rather than a brand new object.  Then just
        replace the old object with the new one by calling this function.
        """
        assert not self.current_transaction(), "Can't replace during a transaction for the time being"
        assert isinstance(new, type(old)), "Can't replace type '{} with type '{}!".format(type(old), type(new))

        # Get the current record and replace the object with the new one
        record = self._live_objects.get_record(old)
        self._live_objects.delete(old)
        self._live_objects.insert(new, record)

        # Make sure creators is correct as well
        try:
            self._creators[new] = self._creators.pop(old)
        except KeyError:
            pass

    def save_snapshot(self, obj, with_meta=None) -> archive.Ref:
        """
        Save a snapshot of the current state of the object.  Returns a reference that can
        then be used with load_snapshot()
        """
        record = self.save_object(obj)
        if with_meta is not None:
            self._archive.set_meta(record.obj_id, with_meta)
        return record.get_reference()

    def load_snapshot(self, reference: archive.Ref) -> Any:
        return self._load_snapshot(reference, depositors.SnapshotDepositor(self))

    def load(self, obj_id):
        """Load an object."""
        return self.load_object(obj_id)

    def copy(self, obj):
        """Create a shallow copy of the object, save that copy and return it"""
        with self.transaction() as trans:
            record = self._save_object(obj, depositors.LiveDepositor(self))
            copy_builder = record.copy_builder(obj_id=self._archive.create_archive_id())

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
            deleted_record = archive.make_deleted_record(record)
            trans.stage(deleted_record)
        self._live_objects.delete(obj)

    def history(self,
                obj_or_obj_id,
                idx_or_slice='*',
                as_objects=True) -> [typing.Sequence[ObjectEntry], typing.Sequence[archive.DataRecord]]:
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

    def load_object(self, obj_id):
        return self._load_object(obj_id, depositors.LiveDepositor(self))

    def save_object(self, obj) -> archive.DataRecord:
        return self._save_object(obj, depositors.LiveDepositor(self))

    # region Metadata

    def get_meta(self, obj_or_identifier) -> dict:
        """Get the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        """
        if isinstance(obj_or_identifier, self._archive.get_id_type()):
            obj_id = obj_or_identifier
        else:
            obj_id = self.get_obj_id(obj_or_identifier)

        return self._archive.get_meta(obj_id)

    def set_meta(self, obj_or_identifier, meta: Optional[Mapping]):
        """Set the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        if isinstance(obj_or_identifier, self._archive.get_id_type()):
            obj_id = obj_or_identifier
        else:
            obj_id = self.get_obj_id(obj_or_identifier)

        self._archive.set_meta(obj_id, meta)

    def update_meta(self, obj_or_identifier, meta: Mapping):
        """Update the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        obj_id = self._ensure_obj_id(obj_or_identifier)
        self._archive.update_meta(obj_id, meta)

    # endregion

    def get_current_record(self, obj) -> archive.DataRecord:
        """Get a record for an object known to the historian"""
        trans = self.current_transaction()
        # Try the transaction first
        if trans:
            try:
                return trans.live_objects.get_record(obj)
            except exceptions.NotFound:
                pass

        return self._live_objects.get_record(obj)

    def get_obj_id(self, obj):
        """Get the object ID for a live object"""
        return self.get_current_record(obj).obj_id

    def get_obj(self, obj_id):
        """Get an object known to the historian"""
        trans = self.current_transaction()
        if trans:
            try:
                trans.get_live_object(obj_id)
            except ValueError:
                pass

        self._live_objects.get_object(obj_id)

    def get_ref(self, obj):
        """Get the current reference for a live object"""
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
        """Determine if an object is trackable i.e. we can treat these as live objects and automatically
        keep track of their history when saving.  Ultimately this is determined by whether the type is
        weak referencable or not.
        """
        try:
            weakref.ref(obj)
            return True
        except TypeError:
            return False

    def is_primitive(self, obj):
        """Check if the object is one of the primitives and should be saved by value in the archive"""
        primitives = types.PRIMITIVE_TYPES + (self._archive.get_id_type(),) + self._archive.get_extra_primitives()
        return isinstance(obj, primitives)

    def is_obj_id(self, obj_id):
        return isinstance(obj_id, self._archive.get_id_type())

    def register_type(self, obj_class_or_helper: [helpers.TypeHelper, typing.Type[types.SavableObject]]):
        helper = self._type_registry.register_type(obj_class_or_helper)
        self._equator.add_equator(helper)

    def register_types(self, obj_claases_or_helpers: Iterable):
        for item in obj_claases_or_helpers:
            self.register_type(item)

    def get_obj_type_id(self, obj_type):
        return self._type_registry.get_type_id(obj_type)

    def get_obj_type(self, type_id):
        return self.get_helper(type_id).TYPE

    def get_helper(self, type_id) -> helpers.TypeHelper:
        return self._type_registry.get_helper_from_type_id(type_id)

    def get_helper_from_obj_type(self, obj_type) -> helpers.TypeHelper:
        return self._type_registry.get_helper_from_obj_type(obj_type)

    # endregion

    def find(self, obj_type=None, criteria=None, version=-1, meta=None, limit=0, as_objects=True):
        """Find entries in the archive

        :param obj_type: the object type to look for
        :param criteria: the criteria on the state of the object to apply
        :param version: the version of the object to retrieve, -1 means latests
        :param meta: the search criteria to apply on the metadata of the object
        :param limit: the maximum number of results to return, 0 means unlimited
        :param as_objects: if True returns the live object instances, False returns the DataRecords
        """
        type_id = self.get_obj_type_id(obj_type) if obj_type is not None else None
        results = self._archive.find(type_id=type_id, state=criteria, version=version, meta=meta, limit=limit)
        if as_objects:
            for result in results:
                yield self.load(result.obj_id)
        else:
            yield from results

    def created_in(self, obj_or_identifier):
        """Return the id of the object that created the passed object"""
        try:
            return self.get_current_record(obj_or_identifier).created_by
        except exceptions.NotFound:
            return self._archive.load(self._get_latest_snapshot_reference(obj_or_identifier)).created_by

    def two_step_save(self, obj, builder, depositor):
        """Save a live object"""
        assert builder.snapshot_hash is not None, "The snapshot hash must be set on the builder before saving"

        with self.transaction() as trans:
            # Insert the object into the transaction so others can refer to it
            ref = archive.Ref(builder.obj_id, builder.version)
            trans.insert_live_object_reference(ref, obj)

            # Now ask the object to save itself and create the record
            if isinstance(obj, types.Primitive):
                saved_state = obj
            else:
                saved_state = depositor.save_instance_state(obj)

            builder.update(dict(type_id=builder.type_id, state=saved_state))
            record = builder.build()

            # Insert the record into the transaction
            trans.insert_live_object(obj, record)
            trans.stage(record)
        return record

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
                # Commit the transaction
                # Live objects
                self._live_objects.update(trans.live_objects)

                # Snapshots
                for ref, obj in trans.snapshots.items():
                    self._snapshots_objects[obj] = ref

                # Save any records that were staged for archiving
                if trans.staged:
                    self._archive.save_many(trans.staged)
            finally:
                assert len(self._transactions) == 1
                assert self._transactions[0] is trans
                self._transactions = None

    def current_transaction(self) -> Optional[Transaction]:
        """Get the current transaction if there is one, otherwise returns None"""
        if not self._transactions:
            return None
        return self._transactions[-1]

    def _get_latest_snapshot_reference(self, obj_id) -> archive.Ref:
        """Given an object id this will return a refernce to the latest snapshot"""
        try:
            return self._archive.get_snapshot_refs(obj_id)[-1]
        except IndexError:
            raise exceptions.NotFound("Object with id '{}' not found.".format(obj_id))

    def _load_object(self, obj_id, depositor: depositors.Depositor):
        obj_id = self._ensure_obj_id(obj_id)

        with self.transaction() as trans:
            # Try getting the object from the our dict of up to date ones
            try:
                return trans.get_live_object(obj_id)
            except exceptions.NotFound:
                pass

            # Couldn't find it, so let's check if we have one and check if it is up to date
            ref = self._get_latest_snapshot_reference(obj_id)
            archive_record = self._archive.load(ref)
            if archive_record.is_deleted_record():
                raise exceptions.ObjectDeleted("Object with id '{}' has been deleted".format(obj_id))

            try:
                obj = self._live_objects.get_object(obj_id)
            except exceptions.NotFound:
                # Ok, just use the one from the archive
                return depositor.load(archive_record)
            else:
                if archive_record.version == self._live_objects.get_record(obj).version:
                    # We're still up to date
                    return obj

                # The one in the archive is newer, so use that
                return depositor.load(archive_record)

    def _save_object(self, obj, depositor) -> archive.DataRecord:
        with self.transaction() as trans:
            # Check if an object is already being saved in the transaction
            try:
                record = trans.get_record_for_live_object(obj)
                return record
            except exceptions.NotFound:
                pass

            # Ok, have to save it
            current_hash = self.hash(obj)

            try:
                # Let's see if we have a record at all
                record = self._live_objects.get_record(obj)
            except exceptions.NotFound:
                # Ok, brand new object
                try:
                    creator = self._creators[obj]
                except KeyError:
                    # Try getting the current process as the creator - this may not stricly
                    # be true as this is just the point the object is being saved...
                    current = process.Process.current_process()
                    creator = current if current is not obj else None

                created_by = None
                if creator is not None:
                    # Have to get an object id for the creator, either from existing record or newly saved one
                    try:
                        created_by = self.get_current_record(creator).obj_id
                    except exceptions.NotFound:
                        created_by = self._save_object(creator, depositor).obj_id

                builder = self._create_builder(obj, dict(created_by=created_by, snapshot_hash=current_hash))
                return self.two_step_save(obj, builder, depositor)
            else:
                # Check if our record is up to date
                with self.transaction() as transaction:
                    loaded_obj = depositors.SnapshotDepositor(self).load(record)

                    if current_hash == record.snapshot_hash and self.eq(obj, loaded_obj):
                        # Objects identical
                        transaction.rollback()
                    else:
                        builder = record.child_builder(snapshot_hash=current_hash)
                        record = self.two_step_save(obj, builder, depositor)

                return record

    def _create_builder(self, obj, additional=None):
        additional = additional or {}
        helper = self._ensure_compatible(obj)
        builder = archive.DataRecord.new_builder(type_id=helper.TYPE_ID,
                                                 obj_id=self._archive.create_archive_id(),
                                                 version=0)
        builder.update(additional)
        return builder

    def _load_snapshot(self, reference: archive.Ref, depositor):
        """Load a snapshot of the object using a reference."""
        with self.transaction() as trans:
            # Try getting the object from the transaction
            try:
                trans.get_snapshot(reference)
            except exceptions.NotFound:
                pass

            # Fine...load from storage
            record = self._archive.load(reference)
            if record.is_deleted_record():
                return None

            return depositor.load(record)

    def _ensure_obj_id(self, obj_or_identifier):
        """
        This call will try and get an object id from the passed parameter.  There are three possibilities:
            1) It is passed an object ID in which case it will be returned directly
            2) It is passed an object instance, in which case it will try and get the id from memory
            3) It is passed a type that can be used as a constructor argument to the object id type in which
                case it will construct it and return the result
        """
        if self.is_obj_id(obj_or_identifier):
            return obj_or_identifier

        # Maybe we've been passed an object
        try:
            return self.get_obj_id(obj_or_identifier)
        except exceptions.NotFound:
            # Try creating it for the user by calling the constructor with the argument passed.
            # This helps for common obj id types which can be constructed from a string
            return self._archive.get_id_type()(obj_or_identifier)

    def _ensure_compatible(self, obj):
        obj_type = type(obj)
        if obj_type not in self._type_registry:
            self._type_registry.register_type(obj_type)

        return self._type_registry.get_helper_from_obj_type(obj_type)
