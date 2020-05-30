from abc import ABCMeta, abstractmethod
import logging
from typing import Optional, MutableMapping, Any

from pytray import tree

import mincepy
from . import archives
from . import exceptions
from . import operations
from . import records

__all__ = 'Saver', 'Loader', 'SnapshotLoader', 'LiveDepositor'

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Base(metaclass=ABCMeta):
    """Common base for loader and saver"""

    def __init__(self, historian):
        self._historian = historian  # type: mincepy.Historian

    def get_archive(self) -> archives.Archive:
        return self._historian.archive

    def get_historian(self):
        """
        Get the owning historian.

        :return: the historian
        :rtype: mincepy.Historian
        """
        return self._historian


class Saver(Base, metaclass=ABCMeta):
    """A depositor that knows how to save records into the archive"""

    @abstractmethod
    def ref(self, obj) -> records.SnapshotId:
        """Get a persistent reference for the given object"""

    def encode(self, obj, schema=None, path=()):
        """Encode a type for archiving"""
        if schema is None:
            schema = []

        historian = self.get_historian()

        if historian.is_primitive(obj):
            # Deal with the special containers by encoding their values if need be
            return tree.transform(self.encode, obj, path, schema=schema)

        # Store by value
        helper = historian.get_helper(type(obj), auto_register=True)
        save_state = helper.save_instance_state(obj, self)
        if not historian.is_primitive(save_state):
            raise RuntimeError("Saved state must be one of the primitive types")

        schema_entry = [path, helper.TYPE_ID]
        version = helper.get_version()
        if version is not None:
            schema_entry.append(version)

        schema.append(schema_entry)
        return self.encode(save_state, schema, path)

    def save_state(self, obj):
        """Save the state of an object and return the encoded state ready to be archived in a
        record"""
        state_types = []
        saved_state = self.encode(obj, state_types)
        return {records.STATE: saved_state, records.STATE_TYPES: state_types}


class Loader(Base, metaclass=ABCMeta):
    """A loader that knows how to load objects from the archive"""

    def decode(self,
               encoded,
               schema: records.StateSchema = None,
               path=(),
               created_callback=None,
               migrated=None):
        """Given the encoded state and an optional schema that defines the type of the encoded
        objects this method will decode the saved state and load the object."""
        try:
            entry = schema[path]
        except KeyError:
            return self._recursive_unpack(encoded, schema, path, created_callback)
        else:
            saved_state = encoded
            helper = self.get_historian().get_helper(entry.type_id)
            if helper.IMMUTABLE:
                saved_state = self._recursive_unpack(encoded, schema, path, created_callback)

            new_obj = helper.new(saved_state)
            if new_obj is None:
                raise RuntimeError("Helper '{}' failed to create a class given state '{}'".format(
                    helper.__class__, saved_state))

            if created_callback is not None:
                created_callback(path, new_obj)

            if not helper.IMMUTABLE:
                saved_state = self._recursive_unpack(encoded, schema, path, created_callback,
                                                     migrated)

            updated = helper.ensure_up_to_date(saved_state, entry.version, self)
            if updated is not None:
                saved_state = updated
                if migrated is not None:
                    migrated[path] = updated

            helper.load_instance_state(new_obj, saved_state, self)
            return new_obj

    def _recursive_unpack(self,
                          encoded_saved_state,
                          schema: records.StateSchema = None,
                          path=(),
                          created_callback=None,
                          migrated=None):
        """Unpack a saved state expanding any contained objects"""
        return tree.transform(self.decode,
                              encoded_saved_state,
                              path,
                              schema=schema,
                              created_callback=created_callback,
                              migrated=migrated)


class LiveDepositor(Saver, Loader):
    """Depositor with strategy that all objects that get referenced should be saved"""

    def ref(self, obj) -> Optional[records.SnapshotId]:
        if obj is None:
            return None

        try:
            # Try getting it from the transaction as there may be one from an in-progress save.
            # We can't use historian.get_ref here because we _only_ want one that's currently
            # being saved or we should try saving it as below to ensure it's up to date
            return self._historian.current_transaction().get_reference_for_live_object(obj)
        except exceptions.NotFound:
            # Then we have to save it and get the resulting reference
            return self._historian._save_object(obj, self).snapshot_id

    def load(self, reference: records.SnapshotId):
        try:
            return self._historian.get_obj(reference.obj_id)
        except exceptions.NotFound:
            return self._historian._load_object(reference.obj_id, self)

    def load_from_record(self, record: records.DataRecord):
        """Load an object from a record"""
        with self._historian.transaction() as trans:

            def created(path, new_obj):
                """Called each time an object is created whilst decoding"""
                # For the root object, put it into the transaction as a live object
                if not path:
                    trans.insert_live_object(new_obj, record)

            migrated = {}
            loaded = self.decode(record.state,
                                 record.get_state_schema(),
                                 created_callback=created,
                                 migrated=migrated)

            if migrated:
                logger.info("Snapshot %s has been migrated to the latest version",
                            record.snapshot_id)
                new_schema = []
                new_state = self.encode(loaded, new_schema)
                trans.stage(
                    operations.Update(record.snapshot_id, {
                        records.STATE: new_state,
                        records.STATE_TYPES: new_schema
                    }))

            return loaded

    def update_from_record(self, obj, record: records.DataRecord) -> bool:
        """Do an in-place update of a object from a record"""
        historian = self.get_historian()
        helper = historian.get_helper(type(obj))
        with historian.transaction() as trans:
            # Make sure the record is in the transaction with the object
            trans.insert_live_object(obj, record)

            saved_state = self._recursive_unpack(record.state, record.get_state_schema())
            helper.load_instance_state(obj, saved_state, self)
            return True

    def save_from_builder(self, obj, builder: records.DataRecordBuilder):
        """Save a live object"""
        from . import process

        assert builder.snapshot_hash is not None, \
            "The snapshot hash must be set on the builder before saving"
        historian = self.get_historian()

        with historian.transaction() as trans:
            # Insert the object into the transaction so others can refer to it
            ref = records.SnapshotId(builder.obj_id, builder.version)
            trans.insert_live_object_reference(ref, obj)

            # Deal with a possible object creator
            if builder.version == 0:
                creator = process.CreatorsRegistry.get_creator(obj)
                if creator is not None:
                    # Found one
                    builder.extras[records.ExtraKeys.CREATED_BY] = self.ref(creator).obj_id

            # Now ask the object to save itself and create the record
            builder.update(self.save_state(obj))
            record = builder.build()

            # Insert the record into the transaction
            trans.insert_live_object(obj, record)
            trans.stage(operations.Insert(record))  # Stage it for being saved

        return record


class SnapshotLoader(Loader):
    """Responsible for loading snapshots.  This object should not be reused and only
    one external call to `load` should be made.  This is because it keeps an internal
    cache."""

    def __init__(self, historian):
        super().__init__(historian)
        self._snapshots = {}  # type: MutableMapping[records.SnapshotId, Any]

    def load(self, ref: records.SnapshotId):
        if not isinstance(ref, records.SnapshotId):
            raise TypeError(ref)

        try:
            return self._snapshots[ref]
        except KeyError:
            record = self.get_archive().load(ref)
            if record.is_deleted_record():
                snapshot = None
            else:
                snapshot = self.load_from_record(record)

            # Cache it
            self._snapshots[ref] = snapshot
            return snapshot

    def load_from_record(self, record: records.DataRecord):
        with self._historian.transaction() as trans:
            obj = self.decode(record.state, record.get_state_schema())
            trans.insert_snapshot(obj, record.snapshot_id)
            return obj
