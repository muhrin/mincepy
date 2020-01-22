from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from typing import Optional

from . import archive
from . import exceptions
from . import types

__all__ = ('Depositor',)

# Keys used in to store the state state of an object when encoding/decoding
TYPE_KEY = '!!type'
STATE_KEY = '!!state'


class Depositor(metaclass=ABCMeta):
    """
    Class responsible for encoding and decoding objects to be deposited and retrieved from the archive
    """

    def __init__(self, historian):
        self._historian = historian

    @abstractmethod
    def ref(self, obj) -> archive.Ref:
        """Get a persistent reference for the given object"""

    @abstractmethod
    def deref(self, reference: archive.Ref):
        """Retrieve an object given a persistent reference"""

    def encode(self, obj):
        """Encode a type for archiving"""
        obj_type = type(obj)
        primitives = self._historian.get_primitive_types()

        if obj_type in primitives:
            # Deal with the special containers by encoding their values if need be
            if isinstance(obj, list):
                return [self.encode(entry) for entry in obj]
            if isinstance(obj, dict):
                return {key: self.encode(value) for key, value in obj.items()}

            return obj

        # Assume that we should create a reference
        reference = self.ref(obj)
        return reference.to_dict()

    def decode(self, encoded):
        """Decode the saved state recreating any saved objects within."""
        enc_type = type(encoded)
        primitives = self._historian.get_primitive_types()
        if enc_type not in primitives:
            raise TypeError("Encoded type must be one of '{}', got '{}'".format(primitives, enc_type))

        if enc_type is dict:
            # It could be a reference dictionary
            try:
                ref = archive.Ref.from_dict(encoded)
            except (ValueError, TypeError):
                return {key: self.decode(value) for key, value in encoded.items()}
            else:
                return self.deref(ref)
        if enc_type is list:
            return [self.decode(value) for value in encoded]

        # No decoding to be done
        return encoded

    def to_dict(self, savable: types.Savable) -> dict:
        obj_type = type(savable)

        return {TYPE_KEY: self._historian.get_obj_type_id(obj_type), STATE_KEY: self.save_instance_state(savable)}

    def from_dict(self, state_dict: dict):
        if not isinstance(state_dict, dict):
            raise TypeError("State dict is of type '{}', should be dictionary!".format(type(state_dict)))
        if not (TYPE_KEY in state_dict and STATE_KEY in state_dict):
            raise ValueError("Passed non-state-dictionary: '{}'".format(state_dict))

        return self.create(state_dict[STATE_KEY])

    def save_instance_state(self, obj):
        """Save the state of an object and return the encoded state ready to be archived in a record"""
        obj_type = type(obj)
        helper = self._historian.get_helper_from_obj_type(obj_type)
        return self.encode(helper.save_instance_state(obj, self))

    @contextmanager
    def create_from(self, record: archive.DataRecord):
        """
        Loading of an object takes place in two steps, analogously to the way python
        creates objects.  First a 'blank' object is created and and yielded by this
        context manager.  Then loading is finished in load_instance_state.  Naturally,
        the state of the object should not be relied upon until the context exits.
        """
        with self._create_from(record.type_id, record.state) as obj:
            yield obj

    @contextmanager
    def _create_from(self, type_id, saved_state):
        helper = self._historian.get_helper(type_id)
        new_obj = helper.new(saved_state)
        assert new_obj is not None, "Helper '{}' failed to create a class given state '{}'".format(
            helper.__class__, saved_state)
        try:
            yield new_obj
        finally:
            decoded = self.decode(saved_state)
            helper.load_instance_state(new_obj, decoded, self)

    def create(self, record: archive.DataRecord):
        with self.create_from(record) as obj:
            return obj


class LiveDepositor(Depositor):
    """Depositor with strategy that all objects that get referenced should be saved"""

    def ref(self, obj) -> Optional[archive.Ref]:
        if obj is None:
            return None

        try:
            return self._historian.get_ref(obj)
        except exceptions.NotFound:
            # Then we have to save it and get the resulting reference
            return self._historian._save_object(obj, self).get_reference()

    def deref(self, reference: Optional[archive.Ref]):
        if reference is None:
            return None

        if not isinstance(reference, archive.Ref):
            raise TypeError(reference)

        try:
            return self._historian.get_obj(reference.obj_id)
        except exceptions.NotFound:
            return self._historian._load_object(reference.obj_id, self)


class SnapshotDepositor(Depositor):
    """Depositor with strategy that all objects that get referenced are snapshots"""

    def ref(self, obj) -> Optional[archive.Ref]:  # pylint: disable=no-self-use
        raise RuntimeError("Cannot get a reference to an object during snapshot transactions")

    def deref(self, reference: Optional[archive.Ref]):
        if reference is None:
            return None

        if not isinstance(reference, archive.Ref):
            raise TypeError(reference)

        # Always load a snapshot
        return self._historian._load_snapshot(reference, self)
