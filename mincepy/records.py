# -*- coding: utf-8 -*-
"""This module defines the data record and other objects and functions related to storing things
in an archive."""

import copy
import collections
import datetime
import operator
from typing import (
    Optional,
    Iterable,
    Sequence,
    Union,
    Tuple,
    Any,
    Mapping,
    TypeVar,
    Generic,
    List,
)

import deprecation
import pytray.tree

from . import type_ids
from . import fields
from . import utils
from . import version as version_mod

__all__ = (
    "OBJ_ID",
    "TYPE_ID",
    "CREATION_TIME",
    "VERSION",
    "STATE",
    "SNAPSHOT_TIME",
    "SNAPSHOT_HASH",
    "EXTRAS",
    "ExtraKeys",
    "DELETED",
    "DataRecord",
    "SnapshotRef",
    "DataRecordBuilder",
    "StateSchema",
    "SnapshotId",
)

OBJ_ID = "obj_id"
TYPE_ID = "type_id"
CREATION_TIME = "creation_time"
VERSION = "version"
STATE = "state"
STATE_TYPES = "state_types"
SNAPSHOT_HASH = "snapshot_hash"
SNAPSHOT_TIME = "snapshot_time"
EXTRAS = "extras"

# An ordered tuple of the fields
DATA_RECORD_FIELDS = (
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
)

SchemaEntry = collections.namedtuple("SchemaEntry", "type_id version")


class ExtraKeys:
    # pylint: disable=too-few-public-methods
    CREATED_BY = "_created_by"  # The ID of the process the data was created in
    COPIED_FROM = (
        "_copied_from"  # The reference to the snapshot that this object was copied from
    )
    USER = "_user"  # The user that saved this snapshot
    HOSTNAME = "_hostname"  # The hostname of the computer this snapshot was saved on


DELETED = "!!deleted"  # Special state to denote a deleted record

IdT = TypeVar("IdT")  # The archive ID type

#: The type ID - this is typically a UUID but can be something else in different contexts
TypeId = Any
#: A path to a field in the record.  This is used when traversing a series of containers that can
#: be either dictionaries or lists and are therefore indexed by strings or integers
EntryPath = Sequence[Union[str, int]]
#: Type that represents a path to an entry in the record state and the corresponding type id
EntryInfo = Tuple[EntryPath, TypeId]


class SnapshotId(Generic[IdT]):
    """A snapshot id identifies a particular version of an object (and the corresponding record),
    it therefore composed of the object id and the version number."""

    __slots__ = "_obj_id", "_version"

    @classmethod
    def from_dict(cls, sid_dict: dict) -> "SnapshotId":
        """Build a snapshot ID from a dictionary.  Uses OBJ_ID and VERSION keys but ignores any additional keys making
        it useful when passing **sid_dict to the constructor would fail because of the presence of unexpected keys."""
        return cls(sid_dict[OBJ_ID], sid_dict[VERSION])

    def __init__(self, obj_id, version: int):
        """Create a snapshot id by passing an object id and version"""
        super().__init__()
        self._obj_id = obj_id
        self._version = version

    def __str__(self):
        return f"{self._obj_id}#{self._version}"

    def __repr__(self):
        return f"SnapshotId({self.obj_id}, {self.version})"

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
        return {"obj_id": self.obj_id, "version": self.version}


SnapshotRef = SnapshotId


def readonly_field(field_name: str, **kwargs) -> fields.Field:
    properties = dict(
        fget=operator.itemgetter(DATA_RECORD_FIELDS.index(field_name)), doc=field_name
    )
    return fields.field(**kwargs)(**properties)


class DataRecord(tuple, fields.WithFields):
    """An immutable record that describes a snapshot of an object"""

    __slots__ = ()

    _fields = (
        OBJ_ID,
        TYPE_ID,
        CREATION_TIME,
        VERSION,
        STATE,
        STATE_TYPES,
        SNAPSHOT_HASH,
        SNAPSHOT_TIME,
        EXTRAS,
    )

    # Object properties
    obj_id = readonly_field(OBJ_ID)
    type_id = readonly_field(TYPE_ID)
    creation_time = readonly_field(CREATION_TIME, store_as="ctime")
    # Snapshot properties
    version = readonly_field(VERSION, store_as="ver")
    state = readonly_field(STATE)
    state_types = readonly_field(STATE_TYPES)
    snapshot_hash = readonly_field(SNAPSHOT_HASH, store_as="hash")
    snapshot_time = readonly_field(SNAPSHOT_TIME, store_as="stime")
    extras = readonly_field(EXTRAS)

    @deprecation.deprecated(
        deprecated_in="0.15.20",
        removed_in="0.17.0",
        current_version=version_mod.__version__,
        details="Use make_child_builder free function instead",
    )
    def child_builder(self, **kwargs) -> "DataRecordBuilder":
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
        defaults.update(
            {
                OBJ_ID: self.obj_id,
                TYPE_ID: self.type_id,
                CREATION_TIME: self.creation_time,
                VERSION: self.version + 1,
                SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
                EXTRAS: copy.deepcopy(self.extras),
            }
        )
        defaults.update(kwargs)
        return DataRecordBuilder(DataRecord, defaults)

    # pylint: disable=too-many-arguments
    def __new__(
        cls,
        obj_id,
        type_id,
        creation_time,
        version,
        state,
        state_types,
        snapshot_hash,
        snapshot_time,
        extras,
    ):
        return tuple.__new__(
            cls,
            (
                obj_id,
                type_id,
                creation_time,
                version,
                state,
                state_types,
                snapshot_hash,
                snapshot_time,
                extras,
            ),
        )

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
    def new_builder(cls, **kwargs) -> "DataRecordBuilder":
        """Get a builder for a new data record, the version will be set to 0"""
        values = cls.defaults()
        values.update(
            {
                CREATION_TIME: utils.DefaultFromCall(datetime.datetime.now),
                VERSION: 0,
                SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
            }
        )
        values.update(kwargs)
        return DataRecordBuilder(cls, values)

    __init__ = object.__init__

    @property
    def __dict__(self):
        """A new OrderedDict mapping field names to their values"""
        return collections.OrderedDict(zip(self._fields, self))

    def __str__(self):
        my_dict = self.__dict__
        key_column_width = max(map(len, my_dict.keys()))
        lines = [f"{key:<{key_column_width}} {value}" for key, value in my_dict.items()]
        return "\n".join(lines)

    def _asdict(self):
        """Return a new OrderedDict which maps field names to their values.
        This method is obsolete.  Use vars(record) or record.__dict__ instead.
        """
        return self.__dict__

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

    def get_references(self) -> Iterable[Tuple[EntryPath, SnapshotId]]:
        """Get the snapshot ids of all objects referenced by this record"""
        references = []
        if self.state_types is not None and self.state is not None:
            for entry_info in filter(
                lambda entry: entry[1] == type_ids.OBJ_REF_TYPE_ID, self.state_types
            ):
                path = entry_info[0]
                sid_info = pytray.tree.get_by_path(self.state, path)
                if sid_info is not None:
                    sid = SnapshotId(**sid_info)
                    references.append((path, sid))
        return references

    def get_files(self) -> List[Tuple[EntryPath, dict]]:
        """Get the state dictionaries for all the files contained in this record (if any)"""
        results = []
        if self.state_types is not None and self.state is not None:
            for entry_info in filter(
                lambda entry: entry[1] == type_ids.FILE_TYPE_ID, self.state_types
            ):
                path = entry_info[0]
                file_dict = pytray.tree.get_by_path(self.state, path)
                results.append((path, file_dict))
        return results

    def get_state_schema(self) -> "StateSchema":
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


# State schema is a mapping where they key is a tuple containing a path to the entry in the schema
# and the value is information about that entry, including its type
# The reason a tuple used (instead of, say, a string) is that this way we can preserve the python
# type of they key e.g. (str, int, int str) could be used to reference a dictionary, then index in
# list, then index in list and then a string in a dictionary.
StateSchema = Mapping[tuple, SchemaEntry]
DataRecordBuilder = utils.NamedTupleBuilder[DataRecord]


def make_child_builder(record: DataRecord, **kwargs) -> "DataRecordBuilder":
    """
    Get a child builder from this DataRecord instance.  The following attributes will be copied
    over:

    * obj_id
    * type_id
    * creation_time
    * created_by

    and version will be incremented by one.
    """
    defaults = record.defaults()
    defaults.update(
        {
            OBJ_ID: record.obj_id,
            TYPE_ID: record.type_id,
            CREATION_TIME: record.creation_time,
            VERSION: record.version + 1,
            SNAPSHOT_TIME: utils.DefaultFromCall(datetime.datetime.now),
            EXTRAS: copy.deepcopy(record.extras),
        }
    )
    defaults.update(kwargs)
    return DataRecordBuilder(DataRecord, defaults)


def make_deleted_builder(record: DataRecord) -> DataRecordBuilder:
    """Get a record that represents the deletion of this object"""
    return make_child_builder(
        record, state=DELETED, state_types=None, snapshot_hash=None
    )
