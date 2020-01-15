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

ObjectEntry = namedtuple('ObjectEntry', 'snapshot_id obj')


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


class Historian(depositor.Referencer):

    def __init__(self, archive: archive.Archive, equators=()):
        self._archive = archive
        self._equator = types.Equator(defaults.get_default_equators() + equators)

        self._up_to_date_ids = utils.WeakObjectIdDict()  # type: typing.MutableMapping[typing.Any, typing.Any]
        self._records = utils.WeakObjectIdDict()  # Object to record
        self._objects = weakref.WeakValueDictionary()  # Snapshot id to object
        self._staged = []
        self._transaction_count = 0

        self._type_registry = {}  # type: typing.MutableMapping[typing.Type, types.TypeHelper]
        self._type_ids = {}

    def save(self, obj, with_meta=None):
        """Save the object in the history producing a unique id"""
        record = self.save_object(obj)
        if with_meta is not None:
            self._archive.set_meta(record.snapshot_id, with_meta)
        return record.obj_id

    def save_as(self, obj, obj_id, with_meta=None):
        """Save an object with a given id.  Will write to the history of an object if the id already exists"""
        with self._transaction():
            # Do we have any records with that id
            current_obj = None
            current_record = None
            for stored, record in self._records.items():
                if record.obj_id == obj_id:
                    current_obj, current_record = stored, record

            if current_obj is not None:
                self._records.pop(current_obj)
            else:
                # Check the archive
                try:
                    current_record = self._archive.history(obj_id, -1)
                except exceptions.NotFound:
                    pass

            if current_record is not None:
                self._records[obj] = current_record
                self._objects[record.snapshot_id] = obj
            # Now save the thing
            record = self.save_object(obj)

        if with_meta is not None:
            self._archive.set_meta(record.snapshot_id, with_meta)

        return obj_id

    def save_get_snapshot_id(self, obj, with_meta=None):
        """
        Convenience function that is equivalent to:
        ```
            historian.save(obj)
            sid = historian.get_last_snapshot_id(obj)
        ```
        """
        self.save(obj, with_meta)
        return self.get_current_record(obj).snapshot_id

    def get_last_snapshot_id(self, obj):
        return self.get_current_record(obj).snapshot_id

    def load(self, identifier):
        """Load an object.  Identifier can be an object id or a snapshot id."""
        sid = self._get_snapshot_id(identifier)
        return self.load_object(sid)

    def history(self, obj_id, idx_or_slice='*') -> typing.Sequence[ObjectEntry]:
        """
        Get a sequence of object ids and instances from the history of the given object.

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
        snapshot_ids = self._archive.get_snapshot_ids(obj_id)
        indices = utils.to_slice(idx_or_slice)
        to_get = snapshot_ids[indices]
        return [ObjectEntry(sid, self.load_object(sid)) for sid in to_get]

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
        # Check if we already have an up to date record
        if obj in self._up_to_date_ids:
            return self._records[obj]

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
                                                     snapshot_id=self._archive.create_archive_id(),
                                                     ancestor_id=None,
                                                     snapshot_hash=current_hash)
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

    def get_current_record(self, obj) -> archive.DataRecord:
        try:
            return self._records[obj]
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

    def find(self, obj_type=None, criteria=None, limit=0):
        """Find entries in the archive"""
        obj_type_id = self.get_obj_type_id(obj_type) if obj_type is not None else None
        results = self._archive.find(obj_type_id=obj_type_id, criteria=criteria, limit=limit)
        return [self.load(result.obj_id) for result in results]

    def created_in(self, obj_or_identifier):
        """Return the id of the object that created the passed object"""
        try:
            return self.get_current_record(obj_or_identifier).created_in
        except exceptions.NotFound:
            return self._archive.load(self._get_snapshot_id(obj_or_identifier)).created_in

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

        return self.save_object(obj).snapshot_id

    def deref(self, snapshot_id):
        """Get the object from a reference"""
        if snapshot_id is None:
            return None

        return self.load_object(snapshot_id)

    def encode(self, obj):
        obj_type = type(obj)
        if obj_type in self._get_primitive_types():
            # Deal with the special containers by encoding their values if need be
            if isinstance(obj, list):
                return [self.encode(entry) for entry in obj]
            if isinstance(obj, dict):
                return {key: self.encode(value) for key, value in obj.items()}

            return obj

        # Non base types should always be converted to encoded dictionaries
        return self.to_dict(obj)

    def to_dict(self, obj: types.Savable) -> dict:
        obj_type = type(obj)
        helper = self._ensure_compatible(obj_type)
        saved_state = helper.save_instance_state(obj, self)
        return {archive.TYPE_ID: helper.TYPE_ID, archive.STATE: self.encode(saved_state)}

    def decode(self, encoded):
        """Decode the saved state recreating any saved objects within."""
        enc_type = type(encoded)
        primitives = self._get_primitive_types()
        if enc_type not in primitives:
            raise TypeError("Encoded type must be one of '{}', got '{}'".format(primitives, enc_type))

        if enc_type is dict:
            if archive.TYPE_ID in encoded:
                return self.from_dict(encoded)
            else:
                return {key: self.decode(value) for key, value in encoded.items()}
        if enc_type is list:
            return [self.decode(value) for value in encoded]

        # No decoding to be done
        return encoded

    def from_dict(self, encoded: dict):
        type_id = encoded[archive.TYPE_ID]
        helper = self.get_helper(type_id)
        saved_state = self.decode(encoded[archive.STATE])
        with helper.load(saved_state, self) as obj:
            return obj

    def two_step_load(self, record: archive.DataRecord):
        try:
            helper = self.get_helper(record.type_id)
        except KeyError:
            raise ValueError("Type with id '{}' has not been registered".format(record.type_id))

        with self._transaction():
            with helper.load(record.state, self) as obj:
                self._up_to_date_ids[obj] = record.snapshot_id
                self._objects[record.snapshot_id] = obj
                self._records[obj] = record
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
        """
        Carry out a transaction.  A checkpoint it created at the beginning so that the state can be rolled back
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


class RollbackTransaction(Exception):
    pass


class Transaction:

    @staticmethod
    def rollback():
        raise RollbackTransaction


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
