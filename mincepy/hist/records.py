from typing import Iterator, Callable, Any

from mincepy import archives
from mincepy import records

__all__ = ('Records',)


class Records:
    """A class that provides access to an archive's records"""

    def __init__(self, archive: archives.Archive, prepare_obj_id: Callable,
                 prepare_type_id: Callable, record_factory: Callable[[records.DataRecord], Any]):
        self._archive = archive
        self._prepare_obj_id = prepare_obj_id
        self._prepare_type_id = prepare_type_id
        self._record_factory = record_factory

    def find(self,
             obj_type=None,
             obj_id=None,
             version: int = -1,
             state=None,
             meta: dict = None,
             extras: dict = None,
             sort=None,
             limit=0,
             skip=0) -> Iterator[Any]:
        """Find records

        :param obj_type: the object type to look for
        :param obj_id: an object or multiple object ids to look for
        :param version: the version of the object to retrieve, -1 means latest
        :param state: the criteria on the state of the object to apply
        :type state: must be subclass of historian.primitive
        :param meta: the search criteria to apply on the metadata of the object
        :param extras: the search criteria to apply on the data record extras
        :param sort: the sort criteria
        :param limit: the maximum number of results to return, 0 means unlimited
        :param skip: the page to get results from
        """
        # pylint: disable=too-many-arguments
        type_id = self._prepare_type_id(obj_type)
        obj_id = self._prepare_obj_id(obj_id)

        results = self._archive.find(obj_id=obj_id,
                                     type_id=type_id,
                                     state=state,
                                     version=version,
                                     meta=meta,
                                     extras=extras,
                                     sort=sort,
                                     limit=limit,
                                     skip=skip)

        for result in results:
            yield self._record_factory(result)

    # pylint: disable=too-many-arguments
    def distinct(self,
                 key: str,
                 obj_type=None,
                 obj_id=None,
                 version: int = -1,
                 state=None,
                 extras: dict = None):
        """Get distinct values of the given record key

        :param key: the key to find distinct values for, see DataRecord for possible keys
        :param obj_type: restrict the search to this type of object
        :param obj_id: restrict the search to this object id
        :param version: convenience for setting the version to search for.  Can also be specified in
            the filter in which case this will be ignored.
        :param state: restrict the search to records marching this state
        :param extras: restrict the search to records matching these extras
        """
        # Build up the filter
        record_filter = {}
        if obj_type is not None:
            record_filter[records.TYPE_ID] = self._prepare_type_id(obj_type)

        if obj_id is not None:
            # Convert object ids to the expected type before passing to archive
            record_filter[records.OBJ_ID] = self._prepare_obj_id(obj_id)

        if version is not None:
            record_filter[records.VERSION] = version

        if state is not None:
            record_filter[records.STATE] = state

        if extras is not None:
            record_filter[records.EXTRAS] = extras

        yield from self._archive.distinct(key, record_filter)
