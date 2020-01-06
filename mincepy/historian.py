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

    def save_instance_state(self, obj, referencer):
        return self.TYPE.save_instance_state(obj, referencer)

    def load_instance_state(self, obj, saved_state, referencer):
        return self.TYPE.load_instance_state(obj, saved_state, referencer)


class Historian:
    def __init__(self, archive: archive.Archive, equators=None):
        self._records = utils.WeakObjectIdDict()  # Object to record
        self._ids = weakref.WeakValueDictionary()  # Snapshot id to object
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
                except exceptions.NotFound:
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
            current_hash = self._historian.hash(obj)

            try:
                record = self._historian.get_record(obj)
            except exceptions.NotFound:
                try:
                    pid = self._historian.get_record(process.Process.current_process()).obj_id
                except exceptions.NotFound:
                    pid = None

                builder = archive.DataRecord.get_builder(
                    obj_id=arch.create_archive_id(),
                    ancestor_id=None,
                    type_id=helper.TYPE_ID,
                    created_in=pid
                )
            else:
                # Now have to check if the historian's record is up to date
                saved_ids = copy.copy(self._up_to_date_ids)
                saved_records = copy.copy(self._records)

                with self.load_instance_state(record.type_id, record.state) as loaded_obj:
                    self._up_to_date_ids[loaded_obj] = record.obj_id
                    self._records[loaded_obj] = record

                # Check the object to see if it's up to date
                if current_hash == record.snapshot_hash and self._historian.eq(obj, loaded_obj):
                    # No change, revert
                    self._up_to_date_ids = saved_ids
                    self._records = saved_records
                    self._up_to_date_ids[obj] = record.obj_id
                    return record
                else:
                    # Record is not up to date, this is a new version
                    builder = record.child_builder()

            builder.snapshot_id = arch.create_archive_id()
            builder.snapshot_hash = current_hash

            # First update the archive ID so if it's required during saving it's available
            self._up_to_date_ids[obj] = builder.snapshot_id
            builder.state = self.save_instance_state(obj)
            record = builder.build()
            self._records[obj] = record
            return record

        def load_object(self, snapshot_id):
            # Check if we have an up to date version
            try:
                return self.get_obj(snapshot_id)
            except ValueError:
                pass  # Have to load it

            record = self._historian._archive.load(snapshot_id)

            # First check the historian
            try:
                obj = self._historian.get_obj(snapshot_id)
            except ValueError:
                # Have to load from storage
                with self.load_instance_state(record.type_id, record.state) as loaded_obj:
                    self._up_to_date_ids[loaded_obj] = snapshot_id
                    self._records[loaded_obj] = record
                return loaded_obj
            else:
                saved_ids = copy.copy(self._up_to_date_ids)
                saved_records = copy.copy(self._records)

                with self.load_instance_state(record.type_id, record.state) as loaded_obj:
                    self._up_to_date_ids[loaded_obj] = record.snapshot_id
                    self._records[loaded_obj] = record

                # Check the object to see if it's up to date
                if self._historian.hash(obj) == record.snapshot_hash and self._historian.eq(obj, loaded_obj):
                    # Not new, revert
                    self._up_to_date_ids = saved_ids
                    self._records = saved_records
                    self._up_to_date_ids[obj] = snapshot_id
                    return obj
                else:
                    return loaded_obj

        def get_archive_id(self, obj):
            """Get the archive id from the object"""
            try:
                return self._up_to_date_ids[obj]
            except KeyError:
                raise ValueError("Don't have up to date ID for '{}'".format(obj))

        def get_obj(self, snapshot_id):
            """Get the object form the archive id"""
            for obj, sid in self._up_to_date_ids.items():
                if snapshot_id == sid:
                    return obj

            raise ValueError("Don't have up to date object for SID '{}'".format(snapshot_id))

        def ref(self, obj):
            """Get a reference id to an object.  Returns a snapshot id."""
            if obj is None:
                return None

            try:
                # Try in this update action
                return self.get_archive_id(obj)
            except ValueError:
                return self.save_object(obj).snapshot_id

        def deref(self, snapshot_id):
            """Get the object from a reference"""
            if snapshot_id is None:
                return None

            try:
                self.get_obj(snapshot_id)
            except ValueError:
                return self.load_object(snapshot_id)

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
            self._ids[record.snapshot_id] = obj

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

            sid = self.get_record(obj).snapshot_id
            if with_meta is not None:
                self._archive.set_meta(sid, with_meta)

        # We know the records are up to date now
        return self.get_record(obj).obj_id

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

        update = self.UpdateAction(self)
        loaded = update.load_object(sid)
        # Update ourselves
        self._update_records(update.get_records())
        return loaded

    def get_meta(self, identifier):
        sid = self._get_snapshot_id(identifier)
        return self._archive.get_meta(sid)

    def set_meta(self, identifier, meta):
        sid = self._get_snapshot_id(identifier)
        self._archive.set_meta(sid, meta)

    def get_obj(self, snapshot_id):
        try:
            return self._ids[snapshot_id]
        except KeyError:
            raise ValueError("Unknown object id '{}'".format(snapshot_id))

    def get_obj_type_id(self, obj_type):
        return self._type_registry[obj_type].TYPE_ID

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
            helper = WrapperHelper(obj_class_or_helper)
        self._type_registry[helper.TYPE] = helper
        self._type_ids[helper.TYPE_ID] = helper.TYPE

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