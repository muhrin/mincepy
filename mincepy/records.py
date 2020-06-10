"""This module defines the data record and other objects and functions related to storing things
in an archive."""

import copy
from collections import namedtuple
import datetime
import typing
from typing import Optional, Iterable, Sequence, Union, Tuple, Any, Mapping
import uuid

import pytray.tree

from . import utils

__all__ = 'OBJ_ID', 'TYPE_ID', 'CREATION_TIME', 'VERSION', 'STATE', 'SNAPSHOT_TIME', \
          'SNAPSHOT_HASH', 'EXTRAS', 'ExtraKeys', 'DELETED', 'DataRecord', 'SnapshotRef', \
          'DataRecordBuilder', 'StateSchema', 'SnapshotId'

OBJ_ID = 'obj_id'
TYPE_ID = 'type_id'
CREATION_TIME = 'creation_time'
VERSION = 'version'
STATE = 'state'
STATE_TYPES = 'state_types'
SNAPSHOT_HASH = 'snapshot_hash'
SNAPSHOT_TIME = 'snapshot_time'
EXTRAS = 'extras'

SchemaEntry = namedtuple('SchemaEntry', 'type_id version')


class ExtraKeys:
    # pylint: disable=too-few-public-methods
    CREATED_BY = '_created_by'  # The ID of the process the data was created in
    COPIED_FROM = '_copied_from'  # The reference to the snapshot that this object was copied from
    USER = '_user'  # The user that saved this snapshot
    HOSTNAME = '_hostname'  # The hostname of the computer this snapshot was saved on


DELETED = '!!deleted'  # Special state to denote a deleted record

IdT = typing.TypeVar('IdT')  # The archive ID type

#: The type ID - this is typically a UUID but can be something else in different contexts
TypeId = Any  # pylint: disable=invalid-name
#: A path to a field in in the record.  This is used when traversing a series of containers that can
#: be either dictionaries or lists and are therefore indexed by strings or integers
EntryPath = Sequence[Union[str, int]]
#: Type that represents a path to an entry in the record state and the corresponding type id
EntryInfo = Tuple[EntryPath, TypeId]


class SnapshotId(typing.Generic[IdT]):
    """A snapshot id identifies a particular version of an object (and the corresponding record), it
    it therefore composed of the object id and the version number."""

    #: The type id for references
    TYPE_ID = uuid.UUID('633c7035-64fe-4d87-a91e-3b7abd8a6a28')

    def __init__(self, obj_id, version: int):
        """Create a snapshot id by passing an object id and version"""
        super().__init__()
        self._obj_id = obj_id
        self._version = version

    def __str__(self):
        return "{}#{}".format(self._obj_id, self._version)

    def __repr__(self):
        return "SnapshotId({}, {})".format(self.obj_id, self.version)

    def __hash__(self):
        return (self.obj_id, self.version).__hash__()

    def __eq__(self, other):
        if not isinstance(other, SnapshotId):
            return False

        return self.obj_id == other.obj_id and self.version == other.version

    @property
    def obj_id(self) -> IdT:
        return self._obj_id

    @property
    def version(self) -> int:
        return self._version

    def to_dict(self) -> dict:
        r"""Convenience function to get a dictionary representation.
        Can be passed to constructor as \*\*kwargs"""
        return {'obj_id': self.obj_id, 'version': self.version}


SnapshotRef = SnapshotId


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
        """Returns a dictionary of default values, the caller owns the dict and is free to modify
         it"""
        return {
            CREATION_TIME: None,
            SNAPSHOT_TIME: None,
            EXTRAS: {},
        }

    @classmethod
    def new_builder(cls, **kwargs) -> 'DataRecordBuilder':
        """Get a builder for a new data record, the version will be set to 0"""
        values = cls.defaults()
        values.update({
            CREATION_TIME: utils.DefaultFromCall(datetime.datetime.now),
            VERSION: 0,
            SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
        })
        values.update(kwargs)
        return DataRecordBuilder(cls, values)

    @property
    def created_by(self) -> IdT:
        """Convenience property to get the creator from the extras"""
        return self.get_extra(ExtraKeys.CREATED_BY)

    def is_deleted_record(self) -> bool:
        """Does this record represent the object having been deleted"""
        return self.state == DELETED

    @property
    def snapshot_id(self) -> SnapshotId:
        """The snapshot id for this record"""
        return SnapshotId(self.obj_id, self.version)

    def get_copied_from(self) -> Optional[SnapshotId]:
        """Get the reference of the data record this object was originally copied from"""
        obj_ref = self.get_extra(ExtraKeys.COPIED_FROM)
        if obj_ref is None:
            return None

        return SnapshotId(**obj_ref)

    def get_extra(self, name):
        """Convenience function to get an extra from the record, returns None if the extra doesn't
        exist"""
        return self.extras.get(name, None)

    def copy_builder(self, **kwargs) -> 'DataRecordBuilder':
        """Get a copy builder from this DataRecord instance. The following attributes will be
        copied over:

        * type_id
        * state [deepcopy]
        * snapshot_hash
        * extras [deepcopy] - the COPIED_FROM entry will be set to a reference to this object

        the version will be set to 0 and the creation time to now.
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
            EXTRAS: copy.deepcopy(self.extras)
        })
        defaults[EXTRAS][ExtraKeys.COPIED_FROM] = self.snapshot_id.to_dict()
        defaults.update(kwargs)

        return DataRecordBuilder(type(self), defaults)

    def child_builder(self, **kwargs) -> 'DataRecordBuilder':
        """
        Get a child builder from this DataRecord instance.  The following attributes will be copied
        over:

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
            EXTRAS: copy.deepcopy(self.extras),
        })
        defaults.update(kwargs)
        return DataRecordBuilder(type(self), defaults)

    def get_references(self) -> Iterable[Tuple[EntryPath, SnapshotId]]:
        """Get all the references to other objects contained in this record"""
        references = []
        if self.state_types is not None and self.state is not None:
            for entry_info in filter(lambda entry: entry[1] == SnapshotId.TYPE_ID,
                                     self.state_types):
                path = entry_info[0]
                sid_info = pytray.tree.get_by_path(self.state, path)
                if sid_info is not None:
                    sid = SnapshotId(**sid_info)
                    references.append((path, sid))
        return references

    def get_state_schema(self) -> 'StateSchema':
        """Get the schema for the state.  This contains the types and versions for each member of
        the state"""
        schema = {}
        for entry in self.state_types:
            path = tuple(entry[0])
            type_id = entry[1]
            version = None
            if len(entry) > 2:
                version = entry[2]
            schema[path] = SchemaEntry(type_id, version)

        return schema


StateSchema = Mapping[tuple, SchemaEntry]
DataRecordBuilder = utils.NamedTupleBuilder[DataRecord]  # pylint: disable=invalid-name


def make_deleted_builder(last_record: DataRecord) -> DataRecordBuilder:
    """Get a record that represents the deletion of this object"""
    return last_record.child_builder(state=DELETED, state_types=None, snapshot_hash=None)
