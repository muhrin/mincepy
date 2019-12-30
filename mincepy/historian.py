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

__all__ = ('Historian', 'set_historian', 'get_historian')

CURRENT_HISTORIAN = None


class Historian:
    def __init__(self, archive: archive.Archive, equators=None):
        self._records = utils.WeakObjectIdDict()  # Object to record
        self._ids = weakref.WeakValueDictionary()  # Archive id to object
        self._archive = archive
        equators = equators or defaults.get_default_equators()
        self._equator = types.Equator(equators)

    class UpdateAction(depositor.Referencer):
        """Create a series of data records to update the state of the historian"""

        def __init__(self, historian):
            self._historian = historian  # type: Historian
            self._up_to_date_ids = utils.WeakObjectIdDict()
            self._records = utils.WeakObjectIdDict()
            self._archivist = archive.Archivist(self._historian._archive, self)

        def get_record(self, obj):
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
            try:
                # Check if we have an up to date record
                return self.get_record(obj)
            except ValueError:
                pass  # Have to check it

            # Check if the record already exists in the historian
            create_new_record = False
            obj_hash = self._historian.hash(obj)
            try:
                record = self._historian.get_record(obj)
            except ValueError:
                create_new_record = True  # Brand new record
            else:
                # Now have to check if the historian's record is up to date
                saved_ids = copy.copy(self._up_to_date_ids)
                saved_records = copy.copy(self._records)

                loader = self._archivist.object_loader(record)
                self._up_to_date_ids[loader.obj] = record.obj_id
                self._records[loader.obj] = record
                loaded_obj = loader.load()

                # Check the object to see if it's up to date
                if obj_hash == record.obj_hash and self._historian.eq(obj, loaded_obj):
                    # No change, revert
                    self._up_to_date_ids = saved_ids
                    self._records = saved_records
                    self._up_to_date_ids[obj] = record.obj_id
                else:
                    # Record is not up to date
                    create_new_record = True

            if create_new_record:
                # First update the archive ID so if it's required during encoding it's available
                record_builder = self._archivist.record_builder(obj, obj_hash)
                self._up_to_date_ids[obj] = record_builder.archive_id
                record = record_builder.build()
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
                loader = self._archivist.object_loader(record)
                self._up_to_date_ids[loader.obj] = archive_id
                self._records[loader.obj] = record
                return loader.load()
            else:
                saved_ids = copy.copy(self._up_to_date_ids)
                saved_records = copy.copy(self._records)

                loader = self._archivist.object_loader(record)
                self._up_to_date_ids[loader.obj] = record.obj_id
                self._records[loader.obj] = record
                loaded_obj = loader.load()

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

        def ref(self, obj) -> bson.ObjectId:
            """Get a reference id to an object"""
            try:
                # Try in this update action
                return self.get_archive_id(obj)
            except ValueError:
                return self.save_object(obj).obj_id

        def deref(self, persistent_id):
            """Get the object from a reference"""
            try:
                self.get_obj(persistent_id)
            except ValueError:
                return self.load_object(persistent_id)

        def get_records(self):
            return self._records

    def _update_records(self, records: typing.Mapping[typing.Any, archive.DataRecord]):
        for obj, record in records.items():
            self._records[obj] = record
            self._ids[record.obj_id] = obj

    def save(self, obj):
        """Save the object in the history producing a unique id"""
        update = self.UpdateAction(self)
        update.save_object(obj)

        new_records = update.get_records()
        # Flush to the archive
        self._archive.save_many(new_records.values())
        # Update our in memory instances
        self._update_records(new_records)

        # We know the records are up to date now
        return self.get_record(obj).obj_id

    def load(self, persistent_id):
        """Load an object form its unique id"""
        update = self.UpdateAction(self)
        loaded = update.load_object(persistent_id)
        # Update ourselves
        self._update_records(update.get_records())
        return loaded

    def get_obj(self, archive_id):
        try:
            return self._ids[archive_id]
        except KeyError:
            raise ValueError("Unknown object id '{}'".format(archive_id))

    def get_record(self, obj) -> archive.DataRecord:
        try:
            return self._records[obj]
        except KeyError:
            raise ValueError("Unknown object '{}'".format(obj))

    def hash(self, obj):
        return self._equator.hash(obj)

    def eq(self, one, other):
        return self._equator.eq(one, other)

    def add_equator(self, type_equator: types.TypeEquator):
        self._equator.add_equator(type_equator)

    def remove_equator(self, type_equator: types.TypeEquator):
        self._equator.remove_equator(type_equator)


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
