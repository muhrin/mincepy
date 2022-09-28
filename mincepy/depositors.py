# -*- coding: utf-8 -*-
"""This module contains various strategies for loading, saving and migrating objects in the archive
"""

from abc import ABCMeta, abstractmethod
import contextlib
import logging
from typing import Optional, Dict, Any, Iterable, Sequence

import deprecation
from pytray import tree

import mincepy
from . import archives
from . import exceptions
from . import operations
from . import records
from . import staging
from . import transactions  # pylint: disable=unused-import
from . import version as version_mod

__all__ = "Saver", "Loader", "SnapshotLoader", "LiveDepositor", "Migrator"

logger = logging.getLogger(__name__)

CONTAINERS = list, dict


class Base(metaclass=ABCMeta):
    """Common base for loader and saver"""

    def __init__(self, historian):
        self._historian: "mincepy.Historian" = historian

    @property
    def historian(self) -> "mincepy.Historian":
        """Get the owning historian"""
        return self._historian

    @property
    def archive(self) -> archives.Archive:
        """Get the archive of the owning historian"""
        return self._historian.archive

    def get_historian(self) -> "mincepy.Historian":
        """Get the owning historian"""
        return self._historian

    def get_archive(self) -> archives.Archive:
        """Get the archive of the owning historian"""
        return self._historian.archive


class Saver(Base, metaclass=ABCMeta):
    """A depositor that knows how to save records to the archive"""

    _extras: Dict[str, Dict] = {}

    @deprecation.deprecated(
        deprecated_in="0.14.2",
        removed_in="0.16.0",
        current_version=version_mod.__version__,
        details="Use get_snapshot_id() instead",
    )
    def ref(self, obj) -> records.SnapshotId:
        """Get a persistent reference for the given object"""
        return self.get_snapshot_id(obj)

    @abstractmethod
    def get_snapshot_id(self, obj) -> records.SnapshotId:
        """Get a persistent reference for the given object"""

    def encode(self, obj, schema=None, path=()):
        """Encode a type for archiving"""
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

        if schema is not None:
            schema.append(schema_entry)

        return self.encode(save_state, schema, path)

    def save_state(self, obj) -> dict:
        """Save the state of an object and return the encoded state ready to be archived in a
        record"""
        schema = []
        saved_state = self.encode(obj, schema)
        return {records.STATE: saved_state, records.STATE_TYPES: schema}

    def set_extras(self, namespace: str, extras):
        pass


class Loader(Base, metaclass=ABCMeta):
    """A loader that knows how to load objects from the archive"""

    @abstractmethod
    def load(self, snapshot_id: records.SnapshotId):
        """Load an object"""

    def decode(
        self,
        encoded,
        schema: records.StateSchema = None,
        path=(),
        created_callback=None,
        updates=None,
    ):
        """Given the encoded state and an optional schema that defines the type of the encoded
        objects this method will decode the saved state and load the object."""
        try:
            entry = schema[path]
        except KeyError:
            # There is no schema entry so this is a primitive type and only containers need to (potentially)
            # decoded further
            if isinstance(encoded, CONTAINERS):
                return self._recursive_unpack(encoded, schema, path, created_callback)

            # Fully decoded
            return encoded
        else:
            saved_state = encoded
            helper = self.get_historian().get_helper(entry.type_id)
            if helper.IMMUTABLE:
                saved_state = self._recursive_unpack(
                    encoded, schema, path, created_callback
                )

            new_obj = helper.new(saved_state)
            if new_obj is None:
                raise RuntimeError(
                    f"Helper '{helper.__class__}' failed to create a class given state '{saved_state}'"
                )

            if created_callback is not None:
                created_callback(path, new_obj)

            if not helper.IMMUTABLE:
                saved_state = self._recursive_unpack(
                    encoded, schema, path, created_callback, updates
                )

            updated = helper.ensure_up_to_date(saved_state, entry.version, self)
            if updated is not None:
                # Use the current version of the record
                saved_state = updated
                if updates is not None:
                    updates[path] = updated

            helper.load_instance_state(new_obj, saved_state, self)
            return new_obj

    def _recursive_unpack(
        self,
        encoded_saved_state,
        schema: records.StateSchema = None,
        path=(),
        created_callback=None,
        updates=None,
    ):
        """Unpack a saved state expanding any contained objects"""
        return tree.transform(
            self.decode,
            encoded_saved_state,
            path,
            schema=schema,
            created_callback=created_callback,
            updates=updates,
        )


class LiveDepositor(Saver, Loader):
    """Depositor with strategy that all objects that get referenced should be saved"""

    def __init__(self, *args, **kwargs):
        # Just patch through
        super().__init__(*args, **kwargs)
        self._saving_set = set()

    def get_snapshot_id(self, obj) -> Optional[records.SnapshotId]:
        if obj is None:
            return None

        try:
            # Try getting it from the transaction as there may be one from an in-progress save.
            # We can't use historian.get_snapshot_id here because we _only_ want one that's
            # currently being saved or we should try saving it as below to ensure it's up to date
            return self._get_current_snapshot_id(obj)
        except exceptions.NotFound:
            # Then we have to save it and get the resulting reference
            return self._save_object(
                obj
            ).snapshot_id  # pylint: disable=protected-access

    def _get_current_snapshot_id(self, obj) -> records.SnapshotId:
        """Get the current snapshot id of an object"""
        return self._historian.current_transaction().get_snapshot_id_for_live_object(
            obj
        )

    def load(self, snapshot_id: records.SnapshotId):
        try:
            return self._historian.get_obj(snapshot_id.obj_id)
        except exceptions.NotFound:
            return self._load_object(snapshot_id.obj_id)

    def load_from_record(self, record: records.DataRecord) -> object:
        """Load an object from a record"""
        with self._historian.in_transaction() as trans:

            def created(path, new_obj):
                """Called each time an object is created whilst decoding"""
                # For the root object, put it into the transaction as a live object
                if not path:
                    trans.insert_live_object(new_obj, record)

            updates = {}
            loaded = self.decode(
                record.state,
                record.get_state_schema(),
                created_callback=created,
                updates=updates,
            )

            if updates:
                logger.warning(
                    "Object snapshot '%s' is at an older version that your current codebase.  It "
                    "can be migrated by using `mince migrate` from the command line.  If this "
                    "object is saved the new entry will use the new version.",
                    record.snapshot_id,
                )

            return loaded

    def _load_object(self, obj_id) -> object:
        """Load an object form the database.  This method is deliberately private as it should
        only be used by the the depositor and the historian"""
        historian = self.get_historian()
        archive = self.get_archive()
        with historian.in_transaction() as trans:
            if trans.is_deleted(obj_id):
                raise exceptions.ObjectDeleted(obj_id)

            # Get the record from the database
            record = self._create_record(archive.objects.get(obj_id))  # DB HIT
            assert not record.is_deleted_record(), (
                f"Found a deleted record in the objects collection ({record.snapshot_id}), "
                f"this should never happen!"
            )

            try:
                obj = historian._live_objects.get_object(
                    obj_id
                )  # pylint: disable=protected-access
            except exceptions.NotFound:
                logger.debug("Loading object from record: %s", record.snapshot_id)
                # Ok, just use the one from the archive
                return self.load_from_record(record)
            else:
                # Compare with the current, live, version
                live_record = historian._live_objects.get_record(
                    obj
                )  # pylint: disable=protected-access
                if record.version != live_record.version:
                    # The one in the archive is newer, so use that
                    logger.debug("Updating object from record: %s", record.snapshot_id)
                    self.update_from_record(obj, record)

                return obj

    def update_from_record(self, obj: object, record: records.DataRecord) -> bool:
        """Do an in-place update of an object from a record"""
        historian = self.get_historian()
        helper = historian.get_helper(type(obj))
        with historian.in_transaction() as trans:
            # Make sure the record is in the transaction with the object
            trans.insert_live_object(obj, record)

            saved_state = self._recursive_unpack(
                record.state, record.get_state_schema()
            )
            helper.load_instance_state(obj, saved_state, self)
            return True

    def _save_object(self, obj: object) -> records.DataRecord:
        historian = self._historian

        try:
            helper = historian.get_helper(type(obj), auto_register=True)
        except ValueError:
            raise TypeError(
                f"Type is incompatible with the historian: {type(obj).__name__}"
            ) from None

        with historian.in_transaction() as trans:
            # Check if an object is already being saved in the transaction
            try:
                record = trans.get_record_for_live_object(obj)
                return record
            except exceptions.NotFound:
                pass

            with self._cycle_protection(obj):
                # Ok, have to save it
                current_hash = historian.hash(obj)

                try:
                    # Let's see if we have a record at all
                    record = historian._live_objects.get_record(
                        obj
                    )  # pylint: disable=protected-access
                except exceptions.NotFound:
                    # Object being saved for the first time
                    builder = self._create_builder(helper, snapshot_hash=current_hash)
                    record = self._save_from_builder(obj, builder)
                    if historian.meta.sticky:
                        # Apply the sticky meta
                        historian.meta.update(record.obj_id, historian.meta.sticky)
                    return record
                else:
                    if helper.IMMUTABLE:
                        logger.info(
                            "Tried to save immutable object with id '%s' again",
                            record.obj_id,
                        )
                        return record

                    # Check if our record is up-to-date
                    with historian.transaction() as nested:
                        loaded_obj = SnapshotLoader(historian).load_from_record(record)

                        if current_hash == record.snapshot_hash and historian.eq(
                            obj, loaded_obj
                        ):
                            # Objects identical
                            nested.rollback()
                        else:
                            builder = records.make_child_builder(
                                record, snapshot_hash=current_hash
                            )
                            record = self._save_from_builder(obj, builder)

                    return record

    def _save_from_builder(self, obj, builder: records.DataRecordBuilder):
        """Save a live object"""
        assert (
            builder.snapshot_hash is not None
        ), "The snapshot hash must be set on the builder before saving"
        historian = self.get_historian()

        with historian.in_transaction() as trans:  # type: transactions.Transaction
            # Insert the object into the transaction so others can refer to it
            sid = records.SnapshotId(builder.obj_id, builder.version)
            with trans.prepare_for_saving(sid, obj):
                # Inject the extras
                builder.extras.update(
                    self._get_extras(obj, builder.obj_id, builder.version)
                )

                # Now ask the object to save itself and create the record
                builder.update(self.save_state(obj))
                record = builder.build()

                # Insert the record into the transaction
                trans.insert_live_object(obj, record)
                trans.stage(operations.Insert(record))  # Stage it for being saved

        return record

    def _get_extras(self, obj, obj_id, version: int) -> dict:
        """Create the extras dictionary for a object that is going to be saved"""
        historian = self.get_historian()
        extras = self.get_historian().get_user_info()

        if version == 0:
            # Stuff to be done the first time an object is saved
            obj_info = staging.get_info(obj)
            if obj_info:
                # Deal with a possible object creator
                created_by = obj_info.get(records.ExtraKeys.CREATED_BY, None)
                if created_by is not None:
                    try:
                        sid = historian.get_snapshot_id(created_by)
                        extras[records.ExtraKeys.CREATED_BY] = sid.obj_id
                    except exceptions.NotFound:
                        logger.info(
                            "Object with id '%s' is being saved but information about the "
                            "object it was created by will not be in the record because "
                            "the original object has not been saved yet and therefore has "
                            "no id.",
                            obj_id,
                        )

                # Deal with possible copied from
                copied_from = obj_info.get(records.ExtraKeys.COPIED_FROM, None)
                if copied_from is not None:
                    try:
                        sid = historian.get_snapshot_id(copied_from)
                        extras[records.ExtraKeys.COPIED_FROM] = sid.to_dict()
                    except exceptions.NotFound:
                        logger.info(
                            "Object with id '%s' is being saved but information about the "
                            "object it was copied from will not be in the record because "
                            "the original object has not been saved yet and therefore has "
                            "no id.",
                            obj_id,
                        )

        return extras

    def _create_builder(self, helper, **additional) -> records.DataRecordBuilder:
        """Create a record builder for a new object object"""
        additional = additional or {}

        builder = records.DataRecord.new_builder(
            type_id=helper.TYPE_ID,
            obj_id=self.get_archive().create_archive_id(),
            version=0,
        )
        builder.update(additional)
        return builder

    @contextlib.contextmanager
    def _cycle_protection(self, obj: object):
        """This context manager is used as a means of circular-reference identification.
        Naturally, such cyclic saving should never happen however if there is a bug, at least this method
        allows us to catch it early and see the source.
        """
        obj_id = id(obj)
        if obj_id in self._saving_set:
            raise RuntimeError(
                "The object is already being saved, this cannot be called twice and suggests "
                "a circular reference is being made"
            )
        self._saving_set.add(obj_id)
        try:
            yield
        finally:
            self._saving_set.remove(obj_id)

    @staticmethod
    def _create_record(entry_dict: dict) -> records.DataRecord:
        return records.DataRecord(**entry_dict)


class SnapshotLoader(Loader):
    """Responsible for loading snapshots.  This object should not be reused and only
    one external call to `load` should be made.  This is because it keeps an internal
    cache."""

    def __init__(self, historian):
        super().__init__(historian)
        self._snapshots = {}  # type: Dict[records.SnapshotId, object]

    def load(self, snapshot_id: records.SnapshotId) -> object:
        """Load an object from its snapshot id"""
        if not isinstance(snapshot_id, records.SnapshotId):
            raise TypeError(snapshot_id)

        try:
            snapshot = self._snapshots[snapshot_id]
        except KeyError:
            record = self.get_archive().load(snapshot_id)
            if record.is_deleted_record():
                snapshot = None
            else:
                snapshot = self.load_from_record(record)

            # Cache it
            self._snapshots[snapshot_id] = snapshot

        return snapshot

    def load_from_record(self, record: records.DataRecord) -> Any:
        with self._historian.in_transaction() as trans:  # type: transactions.Transaction
            updates = {}
            obj = self.decode(record.state, record.get_state_schema(), updates=updates)
            trans.insert_snapshot(obj, record.snapshot_id)
            if updates:
                logger.warning(
                    "Object snapshot '%s' is at an older version that your current codebase.  It "
                    "can be migrated by using `mince migrate` from the command line.",
                    record.snapshot_id,
                )

            return obj


class Migrator(Saver, SnapshotLoader):
    """A migrating depositor used to make migrations to database records"""

    def get_snapshot_id(self, obj) -> records.SnapshotId:
        try:
            return self.get_historian().get_snapshot_id(obj)
        except exceptions.NotFound:
            pass

        # Ok, try the current transaction
        trans = self.get_historian().current_transaction()
        if trans is not None:
            for sid, snapshot in trans.snapshots.items():
                if obj is snapshot:
                    return sid

        # Ok, it's a brand new object that's never been saved, so save it
        self._historian.save_one(obj)
        return self.get_historian().get_snapshot_id(obj)

    def migrate_records(
        self, to_migrate: Iterable[records.DataRecord]
    ) -> Sequence[records.DataRecord]:
        """Migrate multiple records.  This call will return an iterable of those that were migrated"""
        migrated = []
        with self._historian.in_transaction() as trans:  # type: transactions.Transaction
            for record in to_migrate:
                updates = {}
                obj = self.decode(
                    record.state, record.get_state_schema(), updates=updates
                )
                if updates:
                    self._migrate_record(record, obj, trans)
                    migrated.append(record)

        return migrated

    def _migrate_record(self, record, new_obj, trans):
        """Given the current record and the corresponding instance this will save an updated state
        to the dictionary by re-saving the object.  The current transaction must be supplied."""
        new_schema = []
        new_state = self.encode(new_obj, new_schema)

        trans.stage(
            operations.Update(
                record.snapshot_id,
                {records.STATE: new_state, records.STATE_TYPES: new_schema},
            )
        )

        logger.info(
            "Snapshot %s has been migrated to the latest version", record.snapshot_id
        )
