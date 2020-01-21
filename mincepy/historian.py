from collections import namedtuple
import contextlib
import copy
import typing
from typing import MutableMapping, Any
import weakref

from . import archive
from . import defaults
from . import depositor
from . import exceptions
from . import inmemory
from . import process
from . import types
from . import utils
from .transactions import RollbackTransaction, Transaction, NestedTransaction

__all__ = ('Historian', 'set_historian', 'get_historian', 'INHERIT')

INHERIT = 'INHERIT'

CURRENT_HISTORIAN = None

ObjectEntry = namedtuple('ObjectEntry', 'ref obj')

# Keys used in to store the state state of an object when encoding/decoding
TYPE_KEY = '!!type'
STATE_KEY = '!!state'


class WrapperHelper(types.TypeHelper):
    """Wraps up an object type to perform the necessary Historian actions"""
    # pylint: disable=invalid-name
    TYPE = None
    TYPE_ID = None

    def __init__(self, obj_type: typing.Type[types.SavableComparable]):
        self.TYPE = obj_type
        self.TYPE_ID = obj_type.TYPE_ID
        super(WrapperHelper, self).__init__()

    def yield_hashables(self, obj, hasher):
        yield from self.TYPE.yield_hashables(obj, hasher)

    def eq(self, one, other) -> bool:
        return self.TYPE.__eq__(one, other)

    def save_instance_state(self, obj: types.Savable, referencer):
        return self.TYPE.save_instance_state(obj, referencer)

    def load_instance_state(self, obj, saved_state: types.Savable, referencer):
        return self.TYPE.load_instance_state(obj, saved_state, referencer)


class Historian:  # (depositor.Referencer):

    def __init__(self, archive: archive.Archive, equators=()):
        self._archive = archive
        self._equator = types.Equator(defaults.get_default_equators() + equators)

        # Live object -> data records
        self._records = utils.WeakObjectIdDict()  # type: MutableMapping[Any, archive.DataRecord]
        # Reference -> object
        self._objects = weakref.WeakValueDictionary()  # type: MutableMapping[archive.Ref, Any]

        # Snapshot objects -> reference. Objects that were loaded from historical snapshots
        self._snapshots_objects = utils.WeakObjectIdDict()  # type: MutableMapping[Any, archive.Ref]

        self._type_registry = {}  # type: MutableMapping[typing.Type, types.TypeHelper]
        self._type_ids = {}

        self._transactions = None

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
        with self._snapshot_transaction():
            return self._load_snapshot(reference, SnapshotReferencer(self))

    def load(self, obj_id):
        """Load an object."""
        if not isinstance(obj_id, self._archive.get_id_type()):
            raise TypeError("Object id must be of type '{}'".format(self._archive.get_id_type()))

        ref = self._get_latest_snapshot_reference(obj_id)
        return self.load_object(ref, LiveReferencer(self))

    def copy(self, obj):
        with self._live_transaction() as trans:
            record = self._save_object(obj, LiveReferencer(self))
            copy_builder = record.copy_builder(obj_id=self._archive.create_archive_id())
            obj_copy = copy.copy(obj)
            obj_copy_record = copy_builder.build()
            trans.insert_object_and_record(obj_copy, obj_copy_record)
            trans.stage(obj_copy_record)
        return obj_copy

    def delete(self, obj):
        """Delete an object"""
        record = self.get_current_record(obj)
        with self._live_transaction() as trans:
            deleted_record = archive.make_deleted_record(record)
            trans.stage(deleted_record)
        del self._objects[record.get_reference()]
        del self._records[obj]

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

    def load_object(self, reference, referencer):
        with self._live_transaction():
            return self._load_object(reference, referencer)

    def save_object(self, obj) -> archive.DataRecord:
        with self._live_transaction():
            return self._save_object(obj, LiveReferencer(self))

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

    def get_helper(self, type_id) -> types.TypeHelper:
        return self.get_helper_from_obj_type(self._type_ids[type_id])

    def get_helper_from_obj_type(self, obj_type) -> types.TypeHelper:
        return self._type_registry[obj_type]

    # endregion

    def get_current_record(self, obj) -> archive.DataRecord:
        """Get a record for an object known to the historian"""
        trans = self._current_transaction()
        # Try the transaction first
        if trans:
            try:
                return trans.records[obj]
            except KeyError:
                pass
        try:
            return self._records[obj]
        except KeyError:
            raise exceptions.NotFound("Unknown object '{}'".format(obj))

    def get_obj_id(self, obj):
        """Get the object id for an object known to the historian"""
        trans = self._current_transaction()
        # Try the transaction first
        if trans:
            for cached, ref in trans.objects.items():
                if obj is cached:
                    return ref.obj_id

        try:
            for local, ref in self._objects.items():
                if obj is local:
                    return ref.obj_id
        except KeyError:
            raise exceptions.NotFound("Unknown object '{}'".format(obj))

    def hash(self, obj):
        return self._equator.hash(obj)

    def eq(self, one, other):  # pylint: disable=invalid-name
        return self._equator.eq(one, other)

    def register_type(self, obj_class_or_helper: [types.TypeHelper, typing.Type[types.SavableComparable]]):
        if isinstance(obj_class_or_helper, types.TypeHelper):
            helper = obj_class_or_helper
        else:
            if not issubclass(obj_class_or_helper, types.SavableComparable):
                raise TypeError("Type '{}' is nether a TypeHelper nor a SavableComparable".format(obj_class_or_helper))
            helper = WrapperHelper(obj_class_or_helper)

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

    def _get_latest_snapshot_reference(self, obj_id) -> archive.Ref:
        """Given an object id this will return a refernce to the latest snapshot"""
        try:
            return self._archive.get_snapshot_refs(obj_id)[-1]
        except IndexError:
            raise exceptions.NotFound("Object with id '{}' not found.".format(obj_id))

    def encode(self, obj, referencer):
        obj_type = type(obj)
        if obj_type in self._get_primitive_types():
            # Deal with the special containers by encoding their values if need be
            if isinstance(obj, list):
                return [self.encode(entry, referencer) for entry in obj]
            if isinstance(obj, dict):
                return {key: self.encode(value, referencer) for key, value in obj.items()}

            return obj

        # Non-primitives should always be converted to encoded dictionaries
        helper = self._ensure_compatible(obj_type)
        saved_state = helper.save_instance_state(obj, referencer)
        return {
            TYPE_KEY: helper.TYPE_ID,
            STATE_KEY: self.encode(saved_state, referencer)
        }

    def decode(self, encoded, referencer: depositor.Referencer):
        """Decode the saved state recreating any saved objects within."""
        enc_type = type(encoded)
        if enc_type not in self._get_primitive_types():
            raise TypeError("Encoded type must be one of '{}', got '{}'".format(self._get_primitive_types(), enc_type))

        if enc_type is dict:
            # It could be an encoded object
            if TYPE_KEY in encoded and STATE_KEY in encoded:
                # Assume object encoded as dictionary, decode it as such
                type_id = encoded[TYPE_KEY]
                helper = self.get_helper(type_id)
                # saved_state = self.decode(encoded[STATE_KEY], referencer)
                with self.create_from(encoded[STATE_KEY], helper, referencer) as obj:
                    return obj
            else:
                return {key: self.decode(value, referencer) for key, value in encoded.items()}
        if enc_type is list:
            return [self.decode(value, referencer) for value in encoded]

        # No decoding to be done
        return encoded

    @contextlib.contextmanager
    def create_from(self, encoded_saved_state, helper: types.TypeHelper, referencer: depositor.Referencer):
        """
        Loading of an object takes place in two steps, analogously to the way python
        creates objects.  First a 'blank' object is created and and yielded by this
        context manager.  Then loading is finished in load_instance_state.  Naturally,
        the state of the object should not be relied upon until the context exits.
        """
        new_obj = helper.new(encoded_saved_state)
        try:
            yield new_obj
        finally:
            decoded = self.decode(encoded_saved_state, referencer)
            helper.load_instance_state(new_obj, decoded, referencer)

    def two_step_load(self, record: archive.DataRecord, referencer):
        try:
            helper = self.get_helper(record.type_id)
        except KeyError:
            raise ValueError("Type with id '{}' has not been registered".format(record.type_id))

        with self._nested_transaction() as trans:
            with self.create_from(record.state, helper, referencer) as obj:
                trans.insert_object_and_record(obj, record)
        return obj

    def two_step_save(self, obj, builder, referencer):
        with self._nested_transaction() as trans:
            # Create the reference and insert it into the transaction
            ref = archive.Ref(builder.obj_id, builder.version)
            trans.insert_object(obj, ref)

            # Now ask the object to save itself and create the record
            encoded = self.encode(obj, referencer)
            builder.update(dict(type_id=encoded[TYPE_KEY], state=encoded[STATE_KEY]))
            record = builder.build()

            # Insert the record into the transaction
            trans.insert_object_and_record(obj, record)
            trans.stage(record)
        return record

    def _load_snapshot(self, reference: archive.Ref, referencer=None):
        """Load a snapshot of the object using a reference."""
        referencer = referencer or SnapshotReferencer(self)
        trans = self._current_transaction()

        # Try getting the object from the transaction
        try:
            return trans.get_object(reference)
        except ValueError:
            # Couldn't find it, so let's load it from storage
            record = self._archive.load(reference)
            if record.is_deleted_record():
                return record.state

            # Ok, just use the one from storage
            return self.two_step_load(record, referencer)

    def _load_object(self, reference, referencer):
        trans = self._current_transaction()

        # Try getting the object from the our dict of up to date ones
        try:
            return trans.get_object(reference)
        except ValueError:
            pass

        # Couldn't find it, so let's check if we have one and check if it is up to date
        record = self._archive.load(reference)
        if record.is_deleted_record():
            raise exceptions.ObjectDeleted("Object with id '{}' has been deleted".format(reference.obj_id))

        try:
            obj = self._objects[reference]
        except KeyError:
            # Ok, just use the one from storage
            return self.two_step_load(record, referencer)
        else:
            # Need to check if the version we have is up to date
            with self._nested_transaction() as nested:
                loaded_obj = self.two_step_load(record, referencer)

                if self.hash(obj) == self.hash(loaded_obj) and self.eq(obj, loaded_obj):
                    # Objects identical, keep the one we have
                    nested.rollback()
                else:
                    obj = loaded_obj

            return obj

    def _save_object(self, obj, referencer) -> archive.DataRecord:
        trans = self._current_transaction()

        # Check if we already have an up to date record
        try:
            return trans.get_record(obj)
        except ValueError:
            pass

        # Ok, have to save it
        helper = self._ensure_compatible(type(obj))
        current_hash = self.hash(obj)

        try:
            # Let's see if we have a record at all
            record = self._records[obj]
        except KeyError:
            # Completely new
            try:
                created_in = self.get_current_record(process.Process.current_process()).obj_id
            except exceptions.NotFound:
                created_in = None

            builder = archive.DataRecord.get_builder(type_id=helper.TYPE_ID,
                                                     obj_id=self._archive.create_archive_id(),
                                                     created_in=created_in,
                                                     version=0,
                                                     snapshot_hash=current_hash)
            return self.two_step_save(obj, builder, referencer)
        else:
            # Check if our record is up to date
            with self._nested_transaction() as transaction:
                loaded_obj = self.two_step_load(record, referencer)
                if current_hash == record.snapshot_hash and self.eq(obj, loaded_obj):
                    # Objects identical
                    transaction.rollback()
                else:
                    builder = record.child_builder()
                    builder.snapshot_hash = current_hash
                    record = self.two_step_save(obj, builder, referencer)

            return record

    @contextlib.contextmanager
    def _live_transaction(self):
        """Start a new transaction on live objects"""
        assert not self._transactions, "Can't start a new transaction, one is already in progress"
        # Start a transaction
        trans = Transaction()
        self._transactions = [trans]

        try:
            yield trans
        except RollbackTransaction:
            pass
        else:
            # Commit the transaction
            self._objects.update(trans.objects)
            self._records.update(trans.records)
            # Save any records that were staged for archiving
            if trans.staged:
                self._archive.save_many(trans.staged)
        finally:
            assert self._current_transaction() is trans
            self._transactions = None

    @contextlib.contextmanager
    def _snapshot_transaction(self):
        """Start a new transaction on snapshot objects"""
        assert not self._transactions, "Can't start a new transaction, one is already in progress"
        # Start a transaction
        trans = Transaction()
        self._transactions = [trans]

        try:
            yield trans
        except RollbackTransaction:
            pass
        else:
            assert not trans.staged, "Cannot stage objects during a snapshot transaction"
            # Commit the transaction
            for ref, obj in trans.objects.items():
                self._snapshots_objects[obj] = ref
        finally:
            assert self._current_transaction() is trans
            self._transactions = None

    @contextlib.contextmanager
    def _nested_transaction(self):
        with self._transactions[-1].nested() as nested:
            try:
                self._transactions.append(nested)
                yield nested
            finally:
                assert self._transactions[-1] is nested
                self._transactions.pop()

    def _current_transaction(self) -> Transaction:
        if not self._transactions:
            return None
        return self._transactions[-1]

    def _get_primitive_types(self) -> tuple:
        """Get a tuple of the primitive types"""
        return types.BASE_TYPES + (self._archive.get_id_type(),)

    def _ensure_compatible(self, obj_type: typing.Type):
        if obj_type not in self._type_registry:
            if issubclass(obj_type, types.SavableComparable):
                # Make a wrapper
                self.register_type(WrapperHelper(obj_type))
            else:
                raise TypeError(
                    "Object type '{}' is incompatible with the historian, either subclass from SavableComparable or "
                    "provide a helper".format(obj_type))

        return self._type_registry[obj_type]


class LiveReferencer(depositor.Referencer):

    def __init__(self, historian: Historian):
        self._historian = historian

    def ref(self, obj):
        if obj is None:
            return None

        return self._historian._save_object(obj, self).get_reference()

    def deref(self, reference):
        if reference is None:
            return None

        # Always get the latest version
        ref = self._historian._get_latest_snapshot_reference(reference.obj_id)
        return self._historian._load_object(ref, self)


class SnapshotReferencer(depositor.Referencer):

    def __init__(self, historian: Historian):
        self._historian = historian

    def ref(self, obj):  # pylint: disable=no-self-use
        raise RuntimeError("Cannot get a reference to an object during snapshot transactions")

    def deref(self, reference):
        if reference is None:
            return None

        return self._historian._load_snapshot(reference)


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
