from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from typing import Optional, MutableMapping, Any

from . import archive
from . import exceptions
from . import refs
from . import types

__all__ = 'Saver', 'Loader', 'SnapshotLoader', 'LiveDepositor'

# Keys used in to store the state state of an object when encoding/decoding
TYPE_KEY = '!!type_id'
STATE_KEY = '!!state'


class Base(metaclass=ABCMeta):
    """Common base for loader and saver"""

    def __init__(self, historian):
        self._historian = historian

    def get_archive(self) -> archive.Archive:
        return self._historian.get_archive()

    def get_historian(self):
        return self._historian


class Saver(Base, metaclass=ABCMeta):

    @abstractmethod
    def ref(self, obj) -> archive.Ref:
        """Get a persistent reference for the given object"""

    def encode(self, obj):
        """Encode a type for archiving"""
        if self._historian.is_primitive(obj):
            # Deal with the special containers by encoding their values if need be
            if isinstance(obj, list):
                return [self.encode(entry) for entry in obj]
            if isinstance(obj, dict):
                return {key: self.encode(value) for key, value in obj.items()}

            return obj

        # Store by value
        return self.to_dict(obj)

    def to_dict(self, savable: types.Savable) -> dict:
        obj_type = type(savable)
        return {TYPE_KEY: self._historian.get_obj_type_id(obj_type), STATE_KEY: self.save_instance_state(savable)}

    def save_instance_state(self, obj):
        """Save the state of an object and return the encoded state ready to be archived in a record"""
        obj_type = type(obj)
        helper = self._historian.get_helper_from_obj_type(obj_type)
        return self.encode(helper.save_instance_state(obj, self))


class Loader(Base, metaclass=ABCMeta):

    def decode(self, encoded):
        """Decode the saved state recreating any saved objects within."""
        enc_type = type(encoded)
        if not self._historian.is_primitive(encoded):
            raise TypeError("Encoded type is not one of the primitives, got '{}'".format(enc_type))

        if enc_type is dict:
            # Maybe it's a value dictionary
            try:
                decoded = self.from_dict(encoded)
                if isinstance(decoded, refs.ObjRef) and decoded.auto:
                    decoded = decoded()  # Dereference
                return decoded
            except ValueError:
                return {key: self.decode(value) for key, value in encoded.items()}

        if enc_type is list:
            return [self.decode(entry) for entry in encoded]

        # No decoding to be done
        return encoded

    def from_dict(self, state_dict: dict):
        if not isinstance(state_dict, dict):
            raise TypeError("State dict is of type '{}', should be dictionary!".format(type(state_dict)))
        if not (TYPE_KEY in state_dict and STATE_KEY in state_dict):
            raise ValueError("Passed non-state-dictionary: '{}'".format(state_dict))

        type_id, saved_state = state_dict[TYPE_KEY], state_dict[STATE_KEY]
        with self._create_from(type_id, saved_state) as obj:
            assert obj is not None, "Helper '{}' failed to create a class given state '{}'".format(
                type(obj), saved_state)
            return obj

    def create_from(self, obj_type, saved_state):
        """Given a type to create and the saved state, recreate the object"""
        type_id = self._historian.get_obj_type_id(obj_type)
        with self._create_from(type_id, saved_state) as obj:
            return obj

    @contextmanager
    def _create_from(self, type_id, saved_state):
        """
        Loading of an object takes place in two steps, analogously to the way python
        creates objects.  First a 'blank' object is created and and yielded by this
        context manager.  Then loading is finished in load_instance_state.  Naturally,
        the state of the object should not be relied upon until the context exits.
        """
        if isinstance(saved_state, types.Primitive):
            # No decoding to be done
            yield saved_state
            return

        helper = self._historian.get_helper(type_id)
        if helper.IMMUTABLE:
            # Decode straight away
            saved_state = self.decode(saved_state)

        new_obj = helper.new(saved_state)
        assert new_obj is not None, "Helper '{}' failed to create a class given state '{}'".format(
            helper.__class__, saved_state)

        try:
            yield new_obj
        finally:
            if not helper.IMMUTABLE:
                # Decode only after the yield
                saved_state = self.decode(saved_state)
            helper.load_instance_state(new_obj, saved_state, self)


class LiveDepositor(Saver, Loader):
    """Depositor with strategy that all objects that get referenced should be saved"""

    def ref(self, obj) -> Optional[archive.Ref]:
        if obj is None:
            return None

        try:
            # Try getting it from the transaction as there may be one from an in-progress save.
            # We can't use historian.get_ref here because we _only_ want one that's currently being saved
            # or we should try saving it as below to ensure it's up to date
            return self._historian.current_transaction().get_reference_for_live_object(obj)
        except exceptions.NotFound:
            # Then we have to save it and get the resulting reference
            return self._historian._save_object(obj, self).get_reference()

    def load(self, reference: archive.Ref):
        try:
            return self._historian.get_obj(reference.obj_id)
        except exceptions.NotFound:
            return self._historian._load_object(reference.obj_id, self)

    def load_from_record(self, record):
        with self._historian.transaction() as trans:
            with self._create_from(record.type_id, record.state) as obj:
                trans.insert_live_object(obj, record)
                return obj

    def save_from_builder(self, obj, builder):
        """Save a live object"""
        assert builder.snapshot_hash is not None, "The snapshot hash must be set on the builder before saving"

        with self.get_historian().transaction() as trans:
            # Insert the object into the transaction so others can refer to it
            ref = archive.Ref(builder.obj_id, builder.version)
            trans.insert_live_object_reference(ref, obj)

            # Now ask the object to save itself and create the record
            if self._historian.is_primitive(obj):
                saved_state = obj
            else:
                saved_state = self.save_instance_state(obj)

            builder.update(dict(type_id=builder.type_id, state=saved_state))
            record = builder.build()

            # Insert the record into the transaction
            trans.insert_live_object(obj, record)
            trans.stage(record)

        return record


class SnapshotLoader(Loader):
    """Responsible for loading snapshots.  This object should not be reused and only
    one external call to `load` should be made.  This is because it keeps an internal
    cache."""

    def __init__(self, historian):
        super().__init__(historian)
        self._snapshots = {}  # type: MutableMapping[archive.Ref, Any]

    def load(self, ref: archive.Ref):
        if not isinstance(ref, archive.Ref):
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

    def load_from_record(self, record: archive.DataRecord):
        with self._historian.transaction() as trans:
            with self._create_from(record.type_id, record.state) as obj:
                self._snapshots[record.get_reference()] = obj
                trans.insert_snapshot(obj, record.get_reference())
                return obj
