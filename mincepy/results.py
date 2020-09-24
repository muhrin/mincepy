from typing import Iterable, Optional

from . import exceptions


class ResultSet:
    """Representation of the results from a query
    """

    def __init__(self, archive, query_spec, sort_by=None):
        self._archive = archive
        self._query_spec = query_spec
        self._sort_by = sort_by

        self._results_cache = []
        self._results_iter = None  # type: Optional[Iterable]

    def sort(self, sort_by: dict) -> 'ResultSet':
        return ResultSet(self._archive, self._query_spec, sort_by=sort_by)

    def first(self):
        if not self._sort_by:
            raise exceptions.UnorderedError("Can't use first() on unordered result set")

        self._fetch_up_to(0)
        return self._results_cache[0]

    def last(self):
        if not self._sort_by:
            raise exceptions.UnorderedError("Can't use last() on unordered result set")

        self._fetch_up_to(-1)
        return self._results_cache[-1]

    def _fetch_up_to(self, idx: int):
        if idx == -1:
            for entry in self._results_iter:
                self._results_cache.append(entry)
        else:
            if idx >= len(self._results_cache):
                to_fetch = idx - len(self._results_cache)
                for _ in range(to_fetch):
                    try:
                        self._results_cache.append(next(self._results_iter))
                    except StopIteration:
                        raise IndexError("invalid index: {}".format(idx)) from None
