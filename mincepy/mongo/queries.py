# -*- coding: utf-8 -*-
import functools
from typing import Mapping

from .aggregation import and_, eq_
from . import db


def pipeline_latest_version(data_collection: str) -> list:
    """Returns a pipeline that will take the incoming data record documents and for each one find
    the latest version."""
    oid_var = f"${db.OBJ_ID}"
    ver_var = f"${db.VERSION}"

    pipeline = []
    pipeline.extend(
        [
            # Group by object id the maximum version
            {"$group": {"_id": oid_var, "max_ver": {"$max": ver_var}}},
            # Then do a lookup against the same collection to get the records
            {
                "$lookup": {
                    "from": data_collection,
                    "let": {"obj_id": "$_id", "max_ver": "$max_ver"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": and_(
                                    # Match object id and version
                                    eq_(oid_var, "$$obj_id"),
                                    eq_(ver_var, "$$max_ver"),
                                ),
                            }
                        }
                    ],
                    "as": "latest",
                }
            },
            # Now unwind and promote the 'latest' field
            {"$unwind": {"path": "$latest"}},
            {"$replaceRoot": {"newRoot": "$latest"}},
        ]
    )

    return pipeline


class QueryBuilder:
    """Simple MongoDB query builder.  Creates a compound query of one or more more terms"""

    def __init__(self, *terms):
        self._terms = []
        for term in terms:
            self.and_(term)

    def and_(self, *term: dict):
        for entry in term:
            if not isinstance(entry, dict):
                raise TypeError(entry)

        self._terms.extend(term)

    def build(self):
        if not self._terms:
            return {}
        if len(self._terms) == 1:
            return self._terms[0]

        return {"$and": self._terms.copy()}


def flatten_filter(entry_name: str, query) -> list:
    """Expand nested search criteria, e.g. state={'color': 'red'} -> {'state.colour': 'red'}"""
    if isinstance(query, dict):
        transformed = _transform_query_keys(query, entry_name)
        flattened = [{key: value} for key, value in transformed.items()]
    else:
        flattened = [{entry_name: query}]

    return flattened


def expand_filter(entry_name: str, query: Mapping) -> dict:
    if not query:
        return {}
    return {f"{entry_name}.{key}": value for key, value in query.items()}


@functools.singledispatch
def _transform_query_keys(entry, prefix: str = ""):  # pylint: disable=unused-argument
    """Transform a query entry into the correct syntax given a global prefix and the entry itself"""
    return entry


@_transform_query_keys.register(list)
def _(entry: list, prefix: str = ""):
    return [_transform_query_keys(value, prefix) for value in entry]


@_transform_query_keys.register(dict)
def _(entry: dict, prefix: str = ""):
    transformed = {}
    for key, value in entry.items():
        if key.startswith("$"):
            if key in ("$and", "$not", "$nor", "$or"):
                transformed[key] = _transform_query_keys(value, prefix)
            else:
                update = {prefix: {key: value}} if prefix else {key: value}
                transformed.update(update)
        else:
            to_join = [prefix, key] if prefix else [key]
            # Don't pass the prefix on, we've consumed it here
            transformed[".".join(to_join)] = _transform_query_keys(value)

    return transformed
