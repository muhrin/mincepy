# -*- coding: utf-8 -*-
from typing import List, Iterable, Tuple

from . import records as recordsm

__all__ = 'MergeResult', 'DeleteResult'


class MergeResult:
    """Information about the results from a merge operation.
    `all` contains all of the IDs that were considered in the merge which means not only those that were passed but also
    all those that they reference.
    `merged` contains the ids of all the records that were actually merged (the rest were already present)
     """
    __slots__ = 'all', 'merged'

    def __init__(self, all_snapshots=None, merged_snapshots=None):
        self.all = []  # type: List[recordsm.SnapshotId]
        self.merged = []  # type: List[recordsm.SnapshotId]
        if all_snapshots:
            self.all.extend(all_snapshots)
        if merged_snapshots:
            self.merged.extend(merged_snapshots)

    def update(self, result: 'MergeResult'):
        self.all.extend(result.all)
        self.merged.extend(result.merged)


class DeleteResult:
    """Information about results from a delete operation."""

    __slots__ = '_deleted', '_not_found', '_files_transferred'

    def __init__(self, deleted: list, not_found: Iterable = None):
        self._deleted = tuple(deleted)
        self._not_found = tuple() if not_found is None else tuple(not_found)

    @property
    def deleted(self) -> Tuple:
        return self._deleted

    @property
    def not_found(self) -> Tuple:
        return self._not_found
