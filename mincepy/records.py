import copy
import datetime
import typing
from collections import namedtuple

from . import utils

__all__ = 'OBJ_ID', 'TYPE_ID', 'CREATION_TIME', 'VERSION', 'STATE', 'SNAPSHOT_TIME', \
          'SNAPSHOT_HASH', 'EXTRAS', 'ExtraKeys', 'DELETED', 'DataRecord', 'Ref', \
          'DataRecordBuilder'

OBJ_ID = 'obj_id'
TYPE_ID = 'type_id'
CREATION_TIME = 'creation_time'
VERSION = 'version'
STATE = 'state'
STATE_TYPES = 'state_types'
SNAPSHOT_HASH = 'snapshot_hash'
SNAPSHOT_TIME = 'snapshot_time'
EXTRAS = 'extras'


class ExtraKeys:
    CREATED_BY = '_created_by'  # The ID of the process the data was created in
    COPIED_FROM = '_copied_from'  # The reference to the snapshot that this object was copied from
    USER = '_user'  # The user that saved this snapshot
    HOSTNAME = '_hostname'  # The hostname of the computer this snapshot was saved on


DELETED = '!!deleted'  # Special state to denote a deleted record

IdT = typing.TypeVar('IdT')  # The archive ID type


class Ref(typing.Generic[IdT]):
    """Reference information for looking up a snapshot"""

    def __init__(self, obj_id, version):
        super(Ref, self).__init__()
        self._obj_id = obj_id
        self._version = version

    def __str__(self):
        return "{}#{}".format(self._obj_id, self._version)

    def __hash__(self):
        return (self.obj_id, self.version).__hash__()

    def __eq__(self, other):
        if not isinstance(other, Ref):
            return False

        return self.obj_id == other.obj_id and self.version == other.version

    @property
    def obj_id(self) -> IdT:
        return self._obj_id

    @property
    def version(self):
        return self._version

    def to_list(self) -> list:
        """Convenience to represent the reference as a list"""
        return [self.obj_id, self.version]


class DataRecord(
        namedtuple(
            'DataRecord',
            (
                # Object properties
                OBJ_ID,  # The ID of the object (spanning all snapshots)
                TYPE_ID,  # The type ID of this object
                CREATION_TIME,  # The time this object was created
                # Snapshot properties
                VERSION,  # The ID of this particular snapshot of the object
                STATE,  # The saved state of the object
                STATE_TYPES,  # The historian types saved in the state
                SNAPSHOT_HASH,  # The hash of the state
                SNAPSHOT_TIME,  # The time this snapshot was created
                EXTRAS,  # Additional data stored with the snapshot
            ))):
    """An immutable record that describes a snapshot of an object"""

    @classmethod
    def defaults(cls) -> dict:
        """Returns a dictionary of default values, the caller owns the dict and is free to modify it"""
        return {
            CREATION_TIME: None,
            SNAPSHOT_TIME: None,
            EXTRAS: {},
        }

    @classmethod
    def new_builder(cls, **kwargs) -> utils.NamedTupleBuilder:
        """Get a builder for a new data record, the version will be set to 0"""
        values = cls.defaults()
        values.update({
            CREATION_TIME: utils.DefaultFromCall(datetime.datetime.now),
            VERSION: 0,
            SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
        })
        values.update(kwargs)
        return utils.NamedTupleBuilder(cls, values)

    @property
    def created_by(self):
        """Convenience property to get the creator from the extras"""
        return self.get_extra(ExtraKeys.CREATED_BY)

    def is_deleted_record(self):
        """Does this record represent the object having been deleted"""
        return self.state == DELETED

    def get_reference(self) -> Ref:
        """Get a reference for this data record"""
        return Ref(self.obj_id, self.version)

    def get_copied_from(self) -> Ref:
        """Get the reference of the data record this object was originally copied from"""
        obj_ref = self.get_extra(ExtraKeys.COPIED_FROM)
        if obj_ref is None:
            return None

        return Ref(*obj_ref)

    def get_extra(self, name):
        """Convenience function to get an extra from the record, returns None if the extra doesn't exist"""
        return self.extras.get(name, None)

    def copy_builder(self, **kwargs) -> utils.NamedTupleBuilder:
        """Get a copy builder from this DataRecord instance.  The following attributes will be copied over:
            * type_id
            * state [deepcopy]
            * snapshot_hash
        the version will be set to 0 and the creation time to now
        """
        defaults = self.defaults()
        defaults.update({
            TYPE_ID: self.type_id,
            CREATION_TIME: utils.DefaultFromCall(datetime.datetime.now),
            STATE: copy.deepcopy(self.state),
            STATE_TYPES: copy.deepcopy(self.state_types),
            SNAPSHOT_HASH: self.snapshot_hash,
            VERSION: 0,
            SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
        })
        defaults[EXTRAS][ExtraKeys.COPIED_FROM] = self.get_reference().to_list()
        defaults.update(kwargs)
        return utils.NamedTupleBuilder(type(self), defaults)

    def child_builder(self, **kwargs) -> utils.NamedTupleBuilder:
        """
        Get a child builder from this DataRecord instance.  The following attributes will be copied over:
            * obj_id
            * type_id
            * creation_time
            * created_by
        and version will be incremented by one.
        """
        defaults = self.defaults()
        defaults.update({
            OBJ_ID: self.obj_id,
            TYPE_ID: self.type_id,
            CREATION_TIME: self.creation_time,
            VERSION: self.version + 1,
            SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
            EXTRAS: self.extras.copy(),
        })
        defaults.update(kwargs)
        return utils.NamedTupleBuilder(type(self), defaults)


DataRecordBuilder = utils.NamedTupleBuilder[DataRecord]


def make_deleted_builder(last_record: DataRecord) -> DataRecordBuilder:
    """Get a record that represents the deletion of this object"""
    return last_record.child_builder(state=DELETED, state_types=None, snapshot_hash=None)
