import contextlib
import copy
import typing
import weakref

import bson

from . import archive
from . import defaults
from . import depositor
from . import inmemory
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

    def save_instance_state(self, obj, referencer):
        return self.TYPE.save_instance_state(obj, referencer)

    def load_instance_state(self, obj, saved_state, referencer):
        return self.TYPE.load_instance_state(obj, saved_state, referencer)


class Historian:
    def __init__(self, archive: archive.Archive, equators=None):
        self._records = utils.WeakObjectIdDict()  # Object to record
        self._ids = weakref.WeakValueDictionary()  # Archive id to object
        self._archive = archive
        equators = equators or defaults.get_default_equators()
        self._equator = types.Equator(equators)
        self._type_registry = {}
        self._type_ids = {}

    class UpdateAction(depositor.Referencer):
        """Create a series of data records to update the state of the historian"""

        def __init__(self, historian):
            self._historian: Historian = historian
            self._up_to_date_ids = utils.WeakObjectIdDict()
            self._records = utils.WeakObjectIdDict()

        def get_record(self, obj):
            """
            Get the up to date record for this update.  Raises RuntimeError is there is not an up to date
            record.
            """
            if obj not in self._up_to_date_ids:
                raise ValueError("We do not have an up to date record for that object")
            try:
                return self._records[obj]
            except KeyError:
                try:
                    return self._historian.get_record(obj)
                except ValueError:
                    raise RuntimeError(
                        "We have marked an object as up to date but have no record for it, this should never happen")

        def save_object(self, obj) -> archive.DataRecord:
            # TODO: Remove this later, need a more general way to register the type
            self._historian.register_type(type(obj))
            arch = self._historian._archive
            helper = self.get_helper(obj)

            try:
                # Check if we have an up to date record
                return self.get_record(obj)
            except ValueError:
                pass  # Have to check it

            # Check if the record already exists in the historian
            obj_hash = self._historian.hash(obj)
            ancestor_id = None

            try:
                record = self._historian.get_record(obj)
            except ValueError:
                pass  # Brand new record
            else:
                # Now have to check if the historian's record is up to date
                saved_ids = copy.copy(self._up_to_date_ids)
                saved_records = copy.copy(self._records)

                with self.load_instance_state(record.type_id, record.state) as loaded_obj:
                    self._up_to_date_ids[loaded_obj] = record.persistent_id
                    self._records[loaded_obj] = record

                # Check the object to see if it's up to date
                if obj_hash == record.obj_hash and self._historian.eq(obj, loaded_obj):
                    # No change, revert
                    self._up_to_date_ids = saved_ids
                    self._records = saved_records
                    self._up_to_date_ids[obj] = record.persistent_id
                    return record
                else:
                    # Record is not up to date, this is a new version
                    ancestor_id = record.persistent_id

            # First update the archive ID so if it's required during encoding it's available
            archive_id = arch.create_archive_id()
            self._up_to_date_ids[obj] = archive_id
            state = self.save_instance_state(obj)
            # TODO: Check that the ancestor is getting set correctly
            record = archive.DataRecord(
                archive_id,
                helper.TYPE_ID,
                ancestor_id,
                state,
                obj_hash,
            )
            self._records[obj] = record
            return record

        def load_object(self, archive_id):
            # Check if we have an up to date version
            try:
                return self.get_obj(archive_id)
            except ValueError:
                pass  # Have to load it

            record = self._historian._archive.load(archive_id)

            # First check the historian
            try:
                obj = self._historian.get_obj(archive_id)
            except ValueError:
                # Have to load from storage
                with self.load_instance_state(record.type_id, record.state) as loaded_obj:
                    self._up_to_date_ids[loaded_obj] = archive_id
                    self._records[loaded_obj] = record
                return loaded_obj
            else:
                saved_ids = copy.copy(self._up_to_date_ids)
                saved_records = copy.copy(self._records)

                with self.load_instance_state(record.type_id, record.state) as loaded_obj:
                    self._up_to_date_ids[loaded_obj] = record.persistent_id
                    self._records[loaded_obj] = record

                # Check the object to see if it's up to date
                if self._historian.hash(obj) == record.obj_hash and self._historian.eq(obj, loaded_obj):
                    # Not new, revert
                    self._up_to_date_ids = saved_ids
                    self._records = saved_records
                    self._up_to_date_ids[obj] = archive_id
                    return obj
                else:
                    return loaded_obj

        def get_archive_id(self, obj):
            """Get the archive id from the object"""
            try:
                return self._up_to_date_ids[obj]
            except KeyError:
                raise ValueError("Don't have up to date ID for '{}'".format(obj))

        def get_obj(self, archive_id):
            """Get the object form the archive id"""
            for obj, aid in self._up_to_date_ids.items():
                if archive_id == aid:
                    return obj

            raise ValueError("Don't have up to date object for AID '{}'".format(archive_id))

        def ref(self, obj) -> [bson.ObjectId, type(None)]:
            """Get a reference id to an object"""
            if obj is None:
                return None

            try:
                # Try in this update action
                return self.get_archive_id(obj)
            except ValueError:
                return self.save_object(obj).persistent_id

        def deref(self, persistent_id):
            """Get the object from a reference"""
            if persistent_id is None:
                return None

            try:
                self.get_obj(persistent_id)
            except ValueError:
                return self.load_object(persistent_id)

        def get_records(self):
            return self._records

        def save_instance_state(self, obj):
            obj_type = type(obj)
            historian = self._historian
            if obj_type not in historian._type_registry:
                historian.register_type(obj_type)

            helper = historian._type_registry[obj_type]
            return helper.save_instance_state(obj, self)

        def get_helper(self, obj):
            obj_type = type(obj)
            historian = self._historian
            if obj_type not in historian._type_registry:
                historian.register_type(obj_type)

            return historian._type_registry[obj_type]

        @contextlib.contextmanager
        def load_instance_state(self, type_id, saved_state):
            historian = self._historian
            try:
                obj_type = historian._type_ids[type_id]
                helper = historian._type_registry[obj_type]
            except KeyError:
                raise ValueError("Type with id '{}' has not been registered".format(type_id))

            obj = obj_type.__new__(obj_type)
            yield obj
            helper.load_instance_state(obj, saved_state, self)

    def _update_records(self, records: typing.Mapping[typing.Any, archive.DataRecord]):
        for obj, record in records.items():
            self._records[obj] = record
            self._ids[record.persistent_id] = obj

    def save(self, obj, with_meta=None):
        """Save the object in the history producing a unique id"""
        update = self.UpdateAction(self)
        update.save_object(obj)

        new_records = update.get_records()
        if new_records:
            # Flush to the archive
            self._archive.save_many(new_records.values())
            # Update our in memory instances
            self._update_records(new_records)

            persistent_id = self.get_record(obj).persistent_id
            if with_meta is not None:
                self.set_meta(persistent_id, with_meta)

        # We know the records are up to date now
        return self.get_record(obj).persistent_id

    def load(self, persistent_id):
        """Load an object form its unique id"""
        update = self.UpdateAction(self)
        loaded = update.load_object(persistent_id)
        # Update ourselves
        self._update_records(update.get_records())
        return loaded

    def get_meta(self, persistent_id):
        return self._archive.get_meta(persistent_id)

    def set_meta(self, persistent_id, meta):
        self._archive.set_meta(persistent_id, meta)

    def get_obj(self, archive_id):
        try:
            return self._ids[archive_id]
        except KeyError:
            raise ValueError("Unknown object id '{}'".format(archive_id))

    def get_obj_type_id(self, obj_type):
        return self._type_registry[obj_type].TYPE_ID

    def get_record(self, obj) -> archive.DataRecord:
        try:
            return self._records[obj]
        except KeyError:
            raise ValueError("Unknown object '{}'".format(obj))

    def hash(self, obj):
        return self._equator.hash(obj)

    def eq(self, one, other):
        return self._equator.eq(one, other)

    def register_type(self, obj_class_or_helper):
        if isinstance(obj_class_or_helper, types.TypeHelper):
            helper = obj_class_or_helper
        else:
            helper = WrapperHelper(obj_class_or_helper)
        self._type_registry[helper.TYPE] = helper
        self._type_ids[helper.TYPE_ID] = helper.TYPE

    def find(self, obj_type=None, filter=None, limit=0):
        """Find entries in the archive"""
        obj_type_id = self.get_obj_type_id(obj_type) if obj_type is not None else None
        results = self._archive.find(obj_type_id=obj_type_id, filter=filter, limit=limit)
        return [self.load(result.persistent_id) for result in results]

    def copy(self, obj):
        obj_copy = copy.copy(obj)

        return obj_copy

    def get_latest(self, archive_id):
        # TODO: Change this from using the persistent_id to using the record directly
        # This will require the updater to be changed
        return [self.load(record.persistent_id) for record in self._archive.get_leaves(archive_id)]


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
