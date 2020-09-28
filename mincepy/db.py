import functools
from typing import TypeVar, Generic, Iterable, Callable, Any, Iterator, Optional

from mincepy import archives
from mincepy import expr
from mincepy import records

T = TypeVar('T')  # The type stored by the collection pylint: disable=invalid-name


class NotOneError(Exception):
    pass


class ResultSet(Generic[T]):
    __slots__ = ('_archive_collection', '_filter', '_limit', '_sort', '_skip', '_kwargs',
                 '_entry_factory')

    def __init__(  # pylint: disable=too-many-arguments
            self,
            archive_collection: archives.Collection,
            filter: expr.FilterSpec = None,  # pylint: disable=redefined-builtin
            limit: int = None,
            sort: dict = None,
            skip: int = 0,
            kwargs: dict = None,
            entry_factory: Callable[[Any], T] = None):
        self._archive_collection = archive_collection
        self._filter = filter or {}
        self._limit = limit
        self._sort = sort
        self._skip = skip
        self._kwargs = kwargs or {}
        self._entry_factory = entry_factory or (lambda x: x)

    def __iter__(self) -> Iterable[T]:
        for entry in self._archive_collection.find(expr.query_filter(self._filter),
                                                   limit=self._limit,
                                                   sort=self._sort,
                                                   skip=self._skip,
                                                   **self._kwargs):
            yield self._entry_factory(entry)

    def distinct(self, key) -> Iterator:
        yield from self._archive_collection.distinct(key, filter=expr.query_filter(self._filter))

    def any(self) -> Optional[T]:
        """
        Return a single object from the result set.

        :return: an arbitrary object or `None` if there aren't any
        """
        results = tuple(
            self._archive_collection.find(expr.query_filter(self._filter),
                                          limit=1,
                                          skip=self._skip,
                                          **self._kwargs))
        return self._entry_factory(results[0])

    def one(self) -> Optional[T]:
        """Return one item from a result set containing at most one item.

        @raises NotOneError: Raised if the result set contains more than one
            item.
        @return: The object or `None` if there aren't any
        """
        # limit could be 1 due to slicing, for instance.
        limit = self._limit
        if limit is not None and limit > 2:
            limit = 2
        results = tuple(
            self._archive_collection.find(expr.query_filter(self._filter),
                                          limit=limit,
                                          sort=self._sort,
                                          skip=self._skip,
                                          **self._kwargs))
        if not results:
            return None

        if len(results) > 1:
            raise NotOneError("one() used with more than one result available")

        return self._entry_factory(results[0])


class EntriesCollection(Generic[T]):

    def __init__(self, archive_collection: archives.Collection, entry_factory: Callable[[dict],
                                                                                        object],
                 type_id_factory: Callable[[Any], Any], obj_id_factory: Callable[[Any], Any]):
        self._archive_collection = archive_collection
        self._entry_factory = entry_factory
        self._type_id_factory = type_id_factory
        self._obj_id_factory = obj_id_factory

    def find(
        self,  # pylint: disable=redefined-builtin
        *filter: expr.FilterSpec,
        obj_type=None,
        obj_id=None,
        version: int = -1,
        state=None,
        meta: dict = None,
        extras: dict = None,
        sort=None,
        limit=None,
        skip=0,
    ) -> ResultSet[T]:
        """Query the collection returning a result set"""
        filter_expr = self._prepare_filter_expr(*filter,
                                                obj_type=obj_type,
                                                obj_id=obj_id,
                                                version=version,
                                                state=state,
                                                extras=extras)
        return ResultSet(self._archive_collection,
                         filter_expr,
                         sort=sort,
                         limit=limit,
                         skip=skip,
                         kwargs=dict(meta=meta),
                         entry_factory=self._entry_factory)

    def distinct(
            self,
            key: str,
            *filter,  # pylint: disable=redefined-builtin
            obj_type=None,
            obj_id=None,
            version: int = -1,
            state=None,
            extras: dict = None) -> Iterator:
        """Get the distinct values for the given key, optionally restricting to a subset of results
        """
        yield from self.find(*filter,
                             obj_type=obj_type,
                             obj_id=obj_id,
                             version=version,
                             state=state,
                             extras=extras).distinct(key)

    def _prepare_filter_expr(
            self,
            *filter,  # pylint: disable=redefined-builtin
            obj_type=None,
            obj_id=None,
            version: int = -1,
            state=None,
            extras: dict = None) -> expr.Expr:
        """Prepare a query filter expression from the passed filter criteria"""
        query_filter = list(map(expr.get_expr, filter))

        if obj_type is not None:
            query_filter.append(records.DataRecord.type_id == self._type_id_factory(obj_type))
        if obj_id is not None:
            query_filter.append(records.DataRecord.obj_id == self._obj_id_factory(obj_id))
        if version is not None and version != -1:
            query_filter.append(records.DataRecord.version == version)
        if state is not None:
            if isinstance(state, dict):
                for key, value in state.items():
                    query_filter.append(getattr(records.DataRecord.state, key) == value)
            else:
                query_filter.append(records.DataRecord.state == state)

        if extras is not None:
            if not isinstance(extras, dict):
                raise TypeError("extras must be a dict, got '{}'".format(extras))

            for key, value in extras.items():
                query_filter.append(getattr(records.DataRecord.state, key) == value)

        if not query_filter:
            return expr.Empty()

        return expr.And(*query_filter)


class ObjectCollection(EntriesCollection):

    def __init__(
        self,
        archive_collection: archives.Collection,
        record_factory: Callable[[dict], records.DataRecord],
        obj_loader: Callable[[records.DataRecord], object],
        type_id_factory: Callable[[Any], Any],
        obj_id_factory: Callable[[Any], Any],
    ):
        super().__init__(
            archive_collection,
            self._create_object,
            type_id_factory=type_id_factory,
            obj_id_factory=obj_id_factory,
        )
        self._record_factory = record_factory
        self._obj_loader = obj_loader
        self._records = EntriesCollection(archive_collection,
                                          record_factory,
                                          type_id_factory=type_id_factory,
                                          obj_id_factory=obj_id_factory)

    @property
    def records(self) -> EntriesCollection:
        """Access the records directly"""
        return self._records

    def _create_object(self, archive_record: dict):
        # Translate to a data record and then create the object
        return self._obj_loader(self._record_factory(archive_record))


def flatten_filter(entry_name: str, query) -> list:
    """Expand nested search criteria, e.g. state={'color': 'red'} -> {'state.colour': 'red'}"""
    if isinstance(query, dict):
        transformed = _transform_query_keys(query, entry_name)
        flattened = [{key: value} for key, value in transformed.items()]
    else:
        flattened = [{entry_name: query}]

    return flattened


@functools.singledispatch
def _transform_query_keys(entry, prefix: str = ''):  # pylint: disable=unused-argument
    """Transform a query entry into the correct syntax given a global prefix and the entry itself"""
    return entry


@_transform_query_keys.register(list)
def _(entry: list, prefix: str = ''):
    return [_transform_query_keys(value, prefix) for value in entry]


@_transform_query_keys.register(dict)
def _(entry: dict, prefix: str = ''):
    transformed = {}
    for key, value in entry.items():
        if key.startswith('$'):
            if key in ('$and', '$not', '$nor', '$or'):
                transformed[key] = _transform_query_keys(value, prefix)
            else:
                update = {prefix: {key: value}} if prefix else {key: value}
                transformed.update(update)
        else:
            to_join = [prefix, key] if prefix else [key]
            # Don't pass the prefix on, we've consumed it here
            transformed['.'.join(to_join)] = _transform_query_keys(value)

    return transformed
