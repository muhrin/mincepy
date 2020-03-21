from abc import ABCMeta, abstractmethod
from typing import Optional, MutableMapping, Any

from pytray import tree

from . import archives
from . import exceptions
from . import records

__all__ = 'Saver', 'Loader', 'SnapshotLoader', 'LiveDepositor'


class Base(metaclass=ABCMeta):
    """Common base for loader and saver"""

    def __init__(self, historian):
        self._historian = historian

    def get_archive(self) -> archives.Archive:
        return self._historian.archive

    def get_historian(self):
        return self._historian


class Saver(Base, metaclass=ABCMeta):

    @abstractmethod
    def ref(self, obj) -> records.Ref:
        """Get a persistent reference for the given object"""

    def encode(self, obj, types_schema=None, path=()):
        """Encode a type for archiving"""
        if types_schema is None:
            types_schema = []

        historian = self.get_historian()

        if historian.is_primitive(obj):
            # Deal with the special containers by encoding their values if need be
            return tree.transform(self.encode, obj, path, types_schema=types_schema)

        # Store by value
        helper = historian.get_helper(type(obj), auto_register=True)
        save_state = helper.save_instance_state(obj, self)
        assert historian.is_primitive(save_state), "Saved state must be one of the primitive types"
        types_schema.append((path, helper.TYPE_ID))
        return self.encode(save_state, types_schema, path)

    def save_state(self, obj):
        """Save the state of an object and return the encoded state ready to be archived in a
        record"""
        state_types = []
        saved_state = self.encode(obj, state_types)
        return {records.STATE: saved_state, records.STATE_TYPES: state_types}


class Loader(Base, metaclass=ABCMeta):

    def decode(self, encoded, type_schema: dict = None, path=(), created_callback=None):
        try:
            type_id = type_schema[path]
        except KeyError:
            return self._unpack(encoded, type_schema, path, created_callback)
        else:
            saved_state = encoded
            helper = self.get_historian().get_helper(type_id)
            if helper.IMMUTABLE:
                saved_state = self._unpack(encoded, type_schema, path, created_callback)

            new_obj = helper.new(saved_state)
            assert new_obj is not None, \
                "Helper '{}' failed to create a class given state '{}'".format(
                    helper.__class__, saved_state)
            if created_callback is not None:
                created_callback(path, new_obj)

            if not helper.IMMUTABLE:
                saved_state = self._unpack(encoded, type_schema, path, created_callback)

            helper.load_instance_state(new_obj, saved_state, self)
            return new_obj

    def _unpack(self,
                encoded_saved_state,
                type_schema: dict = None,
                path=(),
                created_callback=None):
        """Unpack a saved state expanding any contained objects"""
        return tree.transform(self.decode,
                              encoded_saved_state,
                              path,
                              type_schema=type_schema,
                              created_callback=created_callback)


class LiveDepositor(Saver, Loader):
    """Depositor with strategy that all objects that get referenced should be saved"""

    def ref(self, obj) -> Optional[records.Ref]:
        if obj is None:
            return None

        try:
            # Try getting it from the transaction as there may be one from an in-progress save.
            # We can't use historian.get_ref here because we _only_ want one that's currently
            # being saved or we should try saving it as below to ensure it's up to date
            return self._historian.current_transaction().get_reference_for_live_object(obj)
        except exceptions.NotFound:
            # Then we have to save it and get the resulting reference
            return self._historian._save_object(obj, self).get_reference()

    def load(self, reference: records.Ref):
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

            norm_schema = {tuple(path): type_id for path, type_id in record.state_types}
            return self.decode(record.state, norm_schema, created_callback=created)

    def update_from_record(self, obj, record: records.DataRecord) -> bool:
        """Do an in-place update of a object from a record"""
        historian = self.get_historian()
        helper = historian.get_helper(type(obj))
        with historian.transaction() as trans:
            # Make sure the record is in the transaction with the object
            trans.insert_live_object(obj, record)

            norm_schema = {tuple(path): type_id for path, type_id in record.state_types}
            saved_state = self._unpack(record.state, norm_schema)
            helper.load_instance_state(obj, saved_state, self)
            return True

    def save_from_builder(self, obj, builder):
        """Save a live object"""
        assert builder.snapshot_hash is not None, \
            "The snapshot hash must be set on the builder before saving"
        historian = self.get_historian()

        with historian.transaction() as trans:
            # Insert the object into the transaction so others can refer to it
            ref = records.Ref(builder.obj_id, builder.version)
            trans.insert_live_object_reference(ref, obj)

            # Deal with a possible object creator
            if builder.version == 0:
                try:
                    creator = historian.get_creator(obj)
                except exceptions.NotFound:
                    pass
                else:
                    # Found one
                    builder.extras[records.ExtraKeys.CREATED_BY] = self.ref(creator).obj_id

            # Now ask the object to save itself and create the record
            builder.update(self.save_state(obj))
            record = builder.build()

            # Insert the record into the transaction
            trans.insert_live_object(obj, record)
            trans.stage(record)  # Stage it for being saved

        return record


class SnapshotLoader(Loader):
    """Responsible for loading snapshots.  This object should not be reused and only
    one external call to `load` should be made.  This is because it keeps an internal
    cache."""

    def __init__(self, historian):
        super().__init__(historian)
        self._snapshots = {}  # type: MutableMapping[archives.Ref, Any]

    def load(self, ref: records.Ref):
        if not isinstance(ref, records.Ref):
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
            norm_schema = {tuple(path): type_id for path, type_id in record.state_types}
            obj = self.decode(record.state, norm_schema)
            trans.insert_snapshot(obj, record.get_reference())
            return obj
