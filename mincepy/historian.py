from collections import namedtuple
import contextlib
import copy
import typing
import weakref

from . import archive
from . import defaults
from . import depositor
from . import exceptions
from . import inmemory
from . import process
from . import types
from . import utils

__all__ = ('Historian', 'set_historian', 'get_historian', 'INHERIT')

INHERIT = 'INHERIT'

CURRENT_HISTORIAN = None


class WrapperHelper(types.TypeHelper):
    """Wraps up an object type to perform the necessary Historian actions"""
    TYPE = None
    TYPE_ID = None

    def __init__(self, obj_type):
        self.TYPE = obj_type
        self.TYPE_ID = obj_type.TYPE_ID
        super(WrapperHelper, self).__init__()

    def yield_hashables(self, value, hasher):
        yield from self.TYPE.yield_hashables(value, hasher)

    def eq(self, one, other) -> bool:
        return self.TYPE.__eq__(one, other)

    def save_instance_state(self, obj: types.Savable, referencer):
        return self.TYPE.save_instance_state(obj, referencer)

    def load_instance_state(self, obj, saved_state: types.Savable, referencer):
        return self.TYPE.load_instance_state(obj, saved_state, referencer)


class Historian(depositor.Referencer):
    def __init__(self, archive: archive.Archive, equators=()):
        self._archive = archive
        self._equator = types.Equator(defaults.get_default_equators() + equators)

        self._up_to_date_ids = utils.WeakObjectIdDict()  # type: typing.MutableMapping[typing.Any, typing.Any]
        self._records = utils.WeakObjectIdDict()  # Object to record
        self._objects = weakref.WeakValueDictionary()  # Snapshot id to object
        self._staged = []
        self._transaction_count = 0

        self._type_registry = {}
        self._type_ids = {}

    def save(self, obj, with_meta=None):
        """Save the object in the history producing a unique id"""
        record = self.save_object(obj)
        if with_meta is not None:
            self._archive.set_meta(record.snapshot_id, with_meta)
        return record.obj_id

    def save_get_snapshot_id(self, obj, with_meta=None):
        """
        Convenience function that is equivalent to:
        ```
            historian.save(obj)
            sid = historian.get_last_snapshot_id(obj)
        ```
        """
        self.save(obj, with_meta)
        return self.get_record(obj).snapshot_id

    def get_last_snapshot_id(self, obj):
        return self.get_record(obj).snapshot_id

    def load(self, identifier):
        """Load an object.  Identifier can be an object id or a snapshot id."""
        sid = self._get_snapshot_id(identifier)
        return self.load_object(sid)

    def load_object(self, snapshot_id):
        # Try getting the object from the our dict of up to date ones
        for obj, sid in self._up_to_date_ids.items():
            if snapshot_id == sid:
                return obj

        # Couldn't find it, so let's check if we have one and check if it is up to date
        record = self._archive.load(snapshot_id)
        try:
            obj = self._objects[snapshot_id]
        except KeyError:
            # Ok, just use the one from storage
            return self.two_step_load(record)
        else:
            # Need to check if the version we have is up to date
            with self._transaction() as transaction:
                loaded_obj = self.two_step_load(record)

                if self.hash(obj) == record.snapshot_hash and self.eq(obj, loaded_obj):
                    # Objects identical
                    transaction.rollback()
                else:
                    obj = loaded_obj

            return obj

    def save_object(self, obj) -> archive.DataRecord:
        self.register_type(type(obj))

        # Check if we already have an up to date record
        if obj in self._up_to_date_ids:
            return self._records[obj]

        # Ok, have to save it
        current_hash = self.hash(obj)
        helper = self.get_helper_from_obj_type(type(obj))

        try:
            # Let's see if we have a record at all
            record = self._records[obj]
        except KeyError:
            # Completely new
            try:
                created_in = self.get_record(process.Process.current_process()).obj_id
            except exceptions.NotFound:
                created_in = None

            builder = archive.DataRecord.get_builder(
                type_id=helper.TYPE_ID,
                obj_id=self._archive.create_archive_id(),
                created_in=created_in,
                snapshot_id=self._archive.create_archive_id(),
                ancestor_id=None,
                snapshot_hash=current_hash
            )
            record = self.two_step_save(obj, builder)
            return record
        else:
            # Check if our record is up to date
            with self._transaction() as transaction:
                loaded_obj = self.two_step_load(record)
                if current_hash == record.snapshot_hash and self.eq(obj, loaded_obj):
                    # Objects identical
                    transaction.rollback()
                else:
                    builder = record.child_builder()
                    builder.snapshot_id = self._archive.create_archive_id()
                    builder.snapshot_hash = current_hash
                    record = self.two_step_save(obj, builder)

            return record

    def get_meta(self, identifier):
        sid = self._get_snapshot_id(identifier)
        return self._archive.get_meta(sid)

    def set_meta(self, identifier, meta):
        sid = self._get_snapshot_id(identifier)
        self._archive.set_meta(sid, meta)

    def get_obj_type_id(self, obj_type):
        return self._type_registry[obj_type].TYPE_ID

    def get_helper(self, type_id) -> types.TypeHelper:
        return self.get_helper_from_obj_type(self._type_ids[type_id])

    def get_helper_from_obj_type(self, obj_type) -> types.TypeHelper:
        return self._type_registry[obj_type]

    def get_record(self, obj) -> archive.DataRecord:
        try:
            return self._records[obj]
        except KeyError:
            raise exceptions.NotFound("Unknown object '{}'".format(obj))

    def hash(self, obj):
        return self._equator.hash(obj)

    def eq(self, one, other):
        return self._equator.eq(one, other)

    def register_type(self, obj_class_or_helper):
        if isinstance(obj_class_or_helper, types.TypeHelper):
            helper = obj_class_or_helper
        else:
            if not hasattr(obj_class_or_helper, 'TYPE_ID'):
                raise TypeError("Type '{}' does not declare a TYPE_ID and is therefore incompatible")
            helper = WrapperHelper(obj_class_or_helper)

        self._type_registry[helper.TYPE] = helper
        self._type_ids[helper.TYPE_ID] = helper.TYPE
        self._equator.add_equator(helper)

    def find(self, obj_type=None, filter=None, limit=0):
        """Find entries in the archive"""
        obj_type_id = self.get_obj_type_id(obj_type) if obj_type is not None else None
        results = self._archive.find(obj_type_id=obj_type_id, filter=filter, limit=limit)
        return [self.load(result.obj_id) for result in results]

    def created_in(self, obj_or_identifier):
        """Return the id of the object that created the passed object"""
        try:
            return self.get_record(obj_or_identifier).created_in
        except exceptions.NotFound:
            return self._archive.load(self._get_snapshot_id(obj_or_identifier)).created_in

    def copy(self, obj):
        obj_copy = copy.copy(obj)

        return obj_copy

    def _get_snapshot_id(self, identifier):
        """Given an object id this will return the id of the latest snapshot, otherwise just returns the identifier"""
        try:
            # Assume it's an object id
            return self._archive.get_snapshot_ids(identifier)[-1]
        except IndexError:
            # Ok, maybe a snapshot id then
            return identifier

    def ref(self, obj):
        """Get a reference id to an object.  Returns a snapshot id."""
        if obj is None:
            return None

        try:
            return self._up_to_date_ids[obj]
        except KeyError:
            return self.save_object(obj).snapshot_id

    def deref(self, snapshot_id):
        """Get the object from a reference"""
        if snapshot_id is None:
            return None

        for obj, sid in self._up_to_date_ids.items():
            if snapshot_id == sid:
                return obj

        # Ok, have to load it
        return self.load_object(snapshot_id)

    def to_dict(self, obj: types.Savable) -> dict:
        obj_type = type(obj)
        helper = self._type_registry[obj_type]
        saved_state = helper.save_instance_state(obj, self)
        return {
            archive.TYPE_ID: helper.TYPE_ID,
            archive.STATE: self.encode(saved_state)
        }

    def from_dict(self, encoded: dict):
        type_id = encoded[archive.TYPE_ID]
        helper = self.get_helper(type_id)
        obj = helper.create_blank()
        helper.load_instance_state(obj, encoded[archive.STATE], self)
        return obj

    def encode(self, obj):
        obj_type = type(obj)
        if obj_type in self._get_primitive_types():
            # Deal with the special containers by encoding their values if need be
            if isinstance(obj, list):
                return [self.encode(entry) for entry in obj]
            elif isinstance(obj, dict):
                return {key: self.encode(value) for key, value in obj.items()}

            return obj
        else:
            # Non base types should always be converted to encoded dictionaries
            return self.to_dict(obj)

    def decode(self, encoded):
        enc_type = type(encoded)
        primitives = self._get_primitive_types()
        if enc_type not in primitives:
            raise TypeError("Encoded type must be one of '{}', got '{}'".format(primitives, enc_type))

        if enc_type is dict:
            if archive.TYPE_ID in encoded:
                return self.from_dict(encoded)
            return {key: self.decode(value) for key, value in encoded.items()}
        if enc_type is list:
            return [self.decode(value) for value in encoded]

        return encoded

    def two_step_load(self, record: archive.DataRecord):
        try:
            helper = self.get_helper(record.type_id)
        except KeyError:
            raise ValueError("Type with id '{}' has not been registered".format(record.type_id))

        with self._transaction():
            obj = helper.create_blank()
            self._up_to_date_ids[obj] = record.snapshot_id
            self._objects[record.snapshot_id] = obj
            self._records[obj] = record
            helper.load_instance_state(obj, record.state, self)
        return obj

    def two_step_save(self, obj, builder):
        with self._transaction():
            self._up_to_date_ids[obj] = builder.snapshot_id
            self._objects[builder.snapshot_id] = obj
            builder.update(self.to_dict(obj))
            record = builder.build()
            self._records[obj] = record
            self._staged.append(record)
        return record

    @contextlib.contextmanager
    def _transaction(self):
        """Carry out a transaction.  A checkpoint it created at the beginning so that the state can be rolled back
        if need be, otherwise the state changes are committed at the end of the context.

        e.g.:
        ```
        with self._transaction() as transaction:
            # Do stuff
        # Changes committed
        ```
        or
        ```
        with self._transaction() as transaction:
            # Do stuff
            transaction.rollback()
        # Changes cancelled
        """
        initial_records = copy.copy(self._records)
        initial_snapshot_ids = copy.copy(self._objects)
        initial_up_to_date_ids = copy.copy(self._up_to_date_ids)
        transaction = Transaction()
        self._transaction_count += 1
        try:
            yield transaction
        except RollbackTransaction:
            self._records = initial_records
            self._objects = initial_snapshot_ids
            self._up_to_date_ids = initial_up_to_date_ids
        finally:

            self._transaction_count -= 1
            if not self._transaction_count:
                self._up_to_date_ids = utils.WeakObjectIdDict()
                # Save any records that were staged for archiving
                if self._staged:
                    self._archive.save_many(self._staged)
                    self._staged = []

    def _get_primitive_types(self) -> tuple:
        """Get a tuple of the primitive types"""
        return types.BASE_TYPES + (self._archive.get_id_type(),)


class RollbackTransaction(Exception):
    pass


class Transaction:
    def rollback(self):
        raise RollbackTransaction


def create_default_historian() -> Historian:
    return Historian(inmemory.InMemory())


def get_historian() -> Historian:
    global CURRENT_HISTORIAN
    if CURRENT_HISTORIAN is None:
        CURRENT_HISTORIAN = create_default_historian()
    return CURRENT_HISTORIAN


def set_historian(historian: Historian):
    global CURRENT_HISTORIAN
    CURRENT_HISTORIAN = historian
