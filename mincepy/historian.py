from collections import namedtuple
import contextlib
import copy
import typing
from typing import MutableMapping, Any, Optional

from . import archive
from . import defaults
from . import depositors
from . import exceptions
from . import helpers
from . import inmemory
from . import process
from . import types
from . import utils
from .transactions import RollbackTransaction, Transaction, LiveObjects

__all__ = ('Historian', 'set_historian', 'get_historian', 'INHERIT')

INHERIT = 'INHERIT'

CURRENT_HISTORIAN = None

ObjectEntry = namedtuple('ObjectEntry', 'ref obj')


class Historian:  # (depositor.Referencer):

    def __init__(self, archive: archive.Archive, equators=()):
        self._archive = archive
        self._equator = types.Equator(defaults.get_default_equators() + equators)

        # Snapshot objects -> reference. Objects that were loaded from historical snapshots
        self._snapshots_objects = utils.WeakObjectIdDict()  # type: MutableMapping[Any, archive.Ref]

        self._live_objects = LiveObjects()

        self._type_registry = {}  # type: MutableMapping[typing.Type, types.TypeHelper]
        self._type_ids = {}

        self._transactions = None

        id_type_helper = archive.get_id_type_helper()
        if id_type_helper is not None:
            self.register_type(id_type_helper)

    def save(self, obj, with_meta=None):
        """Save the object in the history producing a unique id"""
        if obj in self._snapshots_objects:
            raise exceptions.ModificationError("Cannot save a snapshot object, that would rewrite history!")

        record = self.save_object(obj)
        if with_meta is not None:
            self._archive.set_meta(record.obj_id, with_meta)
        return record.obj_id

    # def save_as(self, obj, obj_id, with_meta=None):
    #     """Save an object with a given id.  Will write to the history of an object if the id already exists"""
    #     with self._live_transaction() as trans:
    #         # Do we have any records with that id?
    #         current_obj = None
    #         current_record = None
    #         for stored, record in self._records.items():
    #             if record.obj_id == obj_id:
    #                 current_obj, current_record = stored, record
    #                 break
    #
    #         if current_obj is not None:
    #             self._records.pop(current_obj)
    #         else:
    #             # Check the archive
    #             try:
    #                 current_record = self._archive.history(obj_id, -1)
    #             except exceptions.NotFound:
    #                 pass
    #
    #         if current_record is not None:
    #             self._records[obj] = current_record
    #             self._objects[record.version] = obj
    #         # Now save the thing
    #         record = self.save_object(obj, LatestReferencer(self))
    #
    #     if with_meta is not None:
    #         self._archive.set_meta(record.obj_id, with_meta)
    #
    #     return obj_id

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
        if not isinstance(obj_id, self._archive.get_id_type()):
            raise TypeError("Object id must be of type '{}'".format(self._archive.get_id_type()))

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
                obj_id,
                idx_or_slice='*',
                as_objects=True) -> [typing.Sequence[ObjectEntry], typing.Sequence[archive.DataRecord]]:
        """
        Get a sequence of object ids and instances from the history of the given object.

        :param obj_id: The id of the object to get the history for
        :param idx_or_slice: The particular index or a slice of which historical versions to get
        :param as_objects: if True return the object instances, otherwise returns the DataRecords

        Example:

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

    def get_meta(self, obj_id):
        if isinstance(obj_id, archive.Ref):
            obj_id = obj_id.obj_id
        return self._archive.get_meta(obj_id)

    def set_meta(self, obj_id, meta):
        if isinstance(obj_id, archive.Ref):
            obj_id = obj_id.obj_id
        self._archive.set_meta(obj_id, meta)

    # region Registry

    def get_obj_type_id(self, obj_type):
        return self._type_registry[obj_type].TYPE_ID

    def get_helper(self, type_id) -> helpers.TypeHelper:
        return self.get_helper_from_obj_type(self._type_ids[type_id])

    def get_helper_from_obj_type(self, obj_type) -> helpers.TypeHelper:
        try:
            return self._type_registry[obj_type]
        except KeyError:
            raise ValueError("Type '{}' has not been registered".format(obj_type))

    # endregion

    def get_current_record(self, obj) -> archive.DataRecord:
        """Get a record for an object known to the historian"""
        trans = self._current_transaction()
        # Try the transaction first
        if trans:
            try:
                return trans.live_objects.get_record(obj)
            except exceptions.NotFound:
                pass

        return self._live_objects.get_record(obj)

    def get_obj(self, obj_id):
        """Get an object known to the historian"""
        trans = self._current_transaction()
        if trans:
            try:
                trans.get_live_object(obj_id)
            except ValueError:
                pass

        self._live_objects.get_object(obj_id)

    def get_ref(self, obj):
        """Get the current reference for a live object"""
        trans = self._current_transaction()
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

    def register_type(self, obj_class_or_helper: [helpers.TypeHelper, typing.Type[types.SavableComparable]]):
        if isinstance(obj_class_or_helper, helpers.TypeHelper):
            helper = obj_class_or_helper
        else:
            if not issubclass(obj_class_or_helper, types.SavableComparable):
                raise TypeError("Type '{}' is nether a TypeHelper nor a SavableComparable".format(obj_class_or_helper))
            helper = helpers.WrapperHelper(obj_class_or_helper)

        self._type_registry[helper.TYPE] = helper
        self._type_ids[helper.TYPE_ID] = helper.TYPE
        self._equator.add_equator(helper)

    def find(self, obj_type=None, criteria=None, version=-1, limit=0, as_objects=True):
        """Find entries in the archive"""
        type_id = self.get_obj_type_id(obj_type) if obj_type is not None else None
        results = self._archive.find(type_id=type_id, state=criteria, version=version, limit=limit)
        if as_objects:
            return [self.load(result.obj_id) for result in results]

        return results

    def created_in(self, obj_or_identifier):
        """Return the id of the object that created the passed object"""
        try:
            return self.get_current_record(obj_or_identifier).created_in
        except exceptions.NotFound:
            return self._archive.load(self._get_latest_snapshot_reference(obj_or_identifier)).created_in

    def two_step_save(self, obj, builder, depositor):
        """Save a live object"""
        with self.transaction() as trans:
            # Insert the object into the transaction so others can refer to it
            ref = archive.Ref(builder.obj_id, builder.version)
            trans.insert_live_object_reference(ref, obj)

            # Now ask the object to save itself and create the record
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

    def get_primitive_types(self) -> tuple:
        """Get a tuple of the primitive types"""
        return types.BASE_TYPES + (self._archive.get_id_type(),)

    def _get_latest_snapshot_reference(self, obj_id) -> archive.Ref:
        """Given an object id this will return a refernce to the latest snapshot"""
        try:
            return self._archive.get_snapshot_refs(obj_id)[-1]
        except IndexError:
            raise exceptions.NotFound("Object with id '{}' not found.".format(obj_id))

    def _load_object(self, obj_id, depositor: depositors.Depositor):
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
                with depositor.create_from(archive_record) as obj:
                    trans.insert_live_object(obj, archive_record)
                    return obj
            else:
                if archive_record.version == self._live_objects.get_record(obj).version:
                    # We're still up to date
                    return obj

                # The one in the archive is newer, so use that
                with depositor.create_from(archive_record) as obj:
                    trans.insert_live_object(obj, archive_record)
                    return obj

    def _load_snapshot(self, reference: archive.Ref, depositor):
        """Load a snapshot of the object using a reference."""
        # Try getting the object from the transaction
        with self.transaction() as trans:
            # Load from storage
            record = self._archive.load(reference)
            if record.is_deleted_record():
                return None

            with depositor.create_from(record) as obj:
                trans.insert_snapshot(obj, record.get_reference())
                return obj

    def _save_object(self, obj, depositor) -> archive.DataRecord:
        with self.transaction() as trans:
            # Check if an object is already being saved in the transaction
            try:
                return trans.get_record_for_live_object(obj)
            except exceptions.NotFound:
                pass

            # Ok, have to save it
            current_hash = self.hash(obj)

            try:
                # Let's see if we have a record at all
                record = self._live_objects.get_record(obj)
            except exceptions.NotFound:
                # Completely new
                try:
                    created_in = self.get_current_record(process.Process.current_process()).obj_id
                except exceptions.NotFound:
                    created_in = None

                helper = self._ensure_compatible(type(obj))
                builder = archive.DataRecord.get_builder(type_id=helper.TYPE_ID,
                                                         obj_id=self._archive.create_archive_id(),
                                                         created_in=created_in,
                                                         version=0,
                                                         snapshot_hash=current_hash)
                return self.two_step_save(obj, builder, depositor)
            else:
                # Check if our record is up to date
                with self.transaction() as transaction:
                    with depositor.create_from(record) as loaded_obj:
                        pass

                    if current_hash == record.snapshot_hash and self.eq(obj, loaded_obj):
                        # Objects identical
                        transaction.rollback()
                    else:
                        builder = record.child_builder()
                        builder.snapshot_hash = current_hash
                        record = self.two_step_save(obj, builder, depositor)

                return record

    def _current_transaction(self) -> Optional[Transaction]:
        """Get the current transaction if there is one, otherwise returns None"""
        if not self._transactions:
            return None
        return self._transactions[-1]

    def _ensure_compatible(self, obj_type: typing.Type):
        if obj_type not in self._type_registry:
            if issubclass(obj_type, types.SavableComparable):
                # Make a wrapper
                self.register_type(helpers.WrapperHelper(obj_type))
            else:
                raise TypeError(
                    "Object type '{}' is incompatible with the historian, either subclass from SavableComparable or "
                    "provide a helper".format(obj_type))

        return self._type_registry[obj_type]


def create_default_historian() -> Historian:
    return Historian(inmemory.InMemory())


def get_historian() -> Historian:
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    if CURRENT_HISTORIAN is None:
        CURRENT_HISTORIAN = create_default_historian()
    return CURRENT_HISTORIAN


def set_historian(historian: Historian):
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    CURRENT_HISTORIAN = historian
