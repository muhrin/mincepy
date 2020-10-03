# -*- coding: utf-8 -*-
"""
This module collects all the frontend database entities such as collections and results that a
user interacts with (through the historian)
"""
import functools
from typing import TypeVar, Generic, Iterable, Callable, Any, Iterator, Optional

from . import archives
from . import exceptions
from . import expr
from . import records

T = TypeVar('T')  # The type stored by the collection pylint: disable=invalid-name


class ResultSet(Generic[T]):
    """The representation of the results of a query.

    The actual execution of queries is delayed until the user actually requests data through one
    of the methods (including iteration) of this class.

    In general the user should not instantiate this directly but will be returned instances from
    various mincepy methods.
    """

    __slots__ = ('_archive_collection', '_query', '_kwargs', '_entry_factory')

    def __init__(self,
                 archive_collection: archives.Collection,
                 query: expr.Query,
                 kwargs: dict = None,
                 entry_factory: Callable[[Any], T] = None):
        self._archive_collection = archive_collection
        self._query = query
        self._kwargs = kwargs or {}
        self._entry_factory = entry_factory or (lambda x: x)

    def __iter__(self) -> Iterable[T]:
        for entry in self._archive_collection.find(**self._query.__dict__, **self._kwargs):
            yield self._entry_factory(entry)

    def distinct(self, key) -> Iterator:
        yield from self._archive_collection.distinct(key, filter=self._query.get_filter())

    def any(self) -> Optional[T]:
        """
        Return a single object from the result set.

        :return: an arbitrary object or `None` if there aren't any
        """
        query = self._query.copy()
        query.limit = 1
        query.sort = None

        results = tuple(self._archive_collection.find(**query.__dict__, **self._kwargs))
        return self._entry_factory(results[0])

    def one(self) -> Optional[T]:
        """Return one item from a result set containing at most one item.

        :raises NotOneError: Raised if the result set contains more than one item.
        :return: The object or `None` if there aren't any
        """
        # limit could be 1 due to slicing, for instance.
        query = self._query.copy()
        if query.limit is not None and query.limit > 2:
            query.limit = 2
        results = tuple(self._archive_collection.find(**query.__dict__, **self._kwargs))
        if not results:
            return None

        if len(results) > 1:
            raise exceptions.NotOneError('one() used with more than one result available')

        return self._entry_factory(results[0])

    def count(self) -> int:
        return self._archive_collection.count(self._query.get_filter(), **self._kwargs)


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
            skip=0) -> ResultSet[T]:
        """Query the collection returning a result set"""
        query = self._prepare_query(*filter,
                                    obj_type=obj_type,
                                    obj_id=obj_id,
                                    version=version,
                                    state=state,
                                    extras=extras)
        query.sort = sort
        query.limit = limit
        query.skip = skip
        return ResultSet(self._archive_collection,
                         query,
                         kwargs=dict(meta=meta),
                         entry_factory=self._entry_factory)

    def distinct(self,
                 key: str,
                 *expression,
                 obj_type=None,
                 obj_id=None,
                 version: int = -1,
                 state=None,
                 extras: dict = None) -> Iterator:
        """Get the distinct values for the given key, optionally restricting to a subset of results
        """
        yield from self.find(*expression,
                             obj_type=obj_type,
                             obj_id=obj_id,
                             version=version,
                             state=state,
                             extras=extras).distinct(key)

    def get(self, entry_id) -> records.DataRecord:
        return self._entry_factory(self._archive_collection.get(entry_id))

    def _prepare_query(self,
                       *expression,
                       obj_type=None,
                       obj_id=None,
                       version: int = -1,
                       state=None,
                       extras: dict = None) -> expr.Query:
        """Prepare a query filter expression from the passed filter criteria"""
        query = expr.Query(*expression)

        if obj_type is not None:
            query.append(self._get_type_expr(obj_type))

        if obj_id is not None:
            query.append(self._get_obj_id_expr(obj_id))

        if version is not None and version != -1:
            query.append(records.DataRecord.version == version)

        if state is not None:
            if isinstance(state, dict):
                result = flatten_filter('state', state)
                query.extend(result)
            else:
                query.append(records.DataRecord.state == state)

        if extras is not None:
            if not isinstance(extras, dict):
                raise TypeError("extras must be a dict, got '{}'".format(extras))

            result = flatten_filter('extras', extras)
            query.extend(result)

        return query

    def _get_obj_id_expr(self, obj_id) -> expr.Expr:
        if isinstance(obj_id, expr.Expr):
            oper = obj_id
        else:
            if isinstance(obj_id, list):
                oper = expr.In(list(map(self._obj_id_factory, obj_id)))
            else:
                oper = expr.Eq(self._obj_id_factory(obj_id))

        return expr.Comparison(records.DataRecord.obj_id, oper)

    def _get_type_expr(self, obj_type) -> expr.Expr:
        if isinstance(obj_type, expr.Expr):
            oper = obj_type
        else:
            if isinstance(obj_type, list):
                oper = expr.In(list(map(self._type_id_factory, obj_type)))
            else:
                oper = expr.Eq(self._type_id_factory(obj_type))

        return expr.Comparison(records.DataRecord.type_id, oper)


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

    def get(self, entry_id) -> object:
        return self._create_object(self._archive_collection.get(entry_id))

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
