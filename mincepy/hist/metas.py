from typing import Any, Optional, Mapping, Dict

from .. import exceptions

__all__ = ('Meta',)


class Meta:
    """A class for grouping metadata related methods"""

    # Meta is a 'friend' of Historian and so can access privates pylint: disable=protected-access

    def __init__(self, historian, archive):
        self._hist = historian  # type: 'mincepy.Historian'
        self._archive = archive
        self._sticky = {}

    @property
    def sticky(self) -> dict:
        return self._sticky

    def get(self, obj_or_identifier) -> Optional[dict]:
        """Get the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        """
        results = self.get_many((obj_or_identifier,))
        assert len(results) == 1
        meta = tuple(results.values())[0]
        return meta

    def get_many(self, obj_or_identifiers) -> Dict[Any, dict]:
        obj_ids = set(map(self._hist._ensure_obj_id, obj_or_identifiers))
        trans = self._hist.current_transaction()
        if trans:
            # First, get what we can from the archive
            found = {}
            for obj_id in obj_ids:
                try:
                    found[obj_id] = trans.get_meta(obj_id)
                except exceptions.NotFound:
                    pass

            # Now get anything else from the archive
            obj_ids -= found.keys()
            if obj_ids:
                from_archive = self._archive.meta_get_many(obj_ids)
                # Now put into the transaction so it doesn't look it up again.
                for obj_id in obj_ids:
                    trans.set_meta(obj_id, from_archive[obj_id])
                found.update(from_archive)

            return found

        # No transaction
        return self._archive.meta_get_many(obj_ids)

    def set(self, obj_or_identifier, meta: Optional[Mapping]):
        """Set the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        obj_id = self._hist._ensure_obj_id(obj_or_identifier)
        trans = self._hist.current_transaction()
        if trans:
            return trans.set_meta(obj_id, meta)

        return self._archive.meta_set(obj_id, meta)

    def set_many(self, metas: Mapping[Any, Optional[dict]]):
        mapped = {self._hist._ensure_obj_id(ident): meta for ident, meta in metas.items()}
        trans = self._hist.current_transaction()
        if trans:
            for entry in mapped.items():
                trans.set_meta(*entry)
        else:
            self._archive.meta_set_many(mapped)

    def update(self, obj_or_identifier, meta: Mapping):
        """Update the metadata for an object

        :param obj_or_identifier: either the object instance, an object ID or a snapshot reference
        :param meta: the metadata dictionary
        """
        obj_id = self._hist._ensure_obj_id(obj_or_identifier)
        trans = self._hist.current_transaction()
        if trans:
            # Update the metadata in the transaction
            try:
                current = trans.get_meta(obj_id)
            except exceptions.NotFound:
                current = self._archive.meta_get(obj_id)  # Try the archive
                if current is None:
                    current = {}  # Ok, no meta

            current.update(meta)
            trans.set_meta(obj_id, current)
        else:
            self._archive.meta_update(obj_id, meta)

    def update_many(self, metas: Mapping[Any, Optional[dict]]):
        mapped = {self._hist._ensure_obj_id(ident): meta for ident, meta in metas.items()}
        trans = self._hist.current_transaction()
        if trans:
            for entry in mapped.items():
                self.update(*entry)
        else:
            self._archive.meta_update_many(mapped)

    def find(self, filter, obj_id=None):  # pylint: disable=redefined-builtin
        """Find metadata matching the given criteria.  Ever returned metadata dictionary will
        contain an 'obj_id' key which identifies the object it belongs to"""
        return self._archive.meta_find(filter=filter, obj_id=obj_id)

    def create_index(self, keys, unique=False, where_exist=False):
        """Create an index on the metadata.  Takes either a single key or list of (key, direction)
         pairs

         :param keys: the key or keys to create the index on
         :param unique: if True, create a uniqueness constraint on this index
         :param where_exist: if True, only apply this index on documents that contain the key(s)
         """
        self._archive.meta_create_index(keys, unique=unique, where_exist=where_exist)
