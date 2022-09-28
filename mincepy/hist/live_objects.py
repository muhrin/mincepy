# -*- coding: utf-8 -*-
from typing import Callable, Optional

from mincepy import archives
from mincepy import frontend
import mincepy.records as recordsm

__all__ = ("LiveObjectsCollection",)


class LiveObjectsCollection(frontend.ObjectCollection):
    def __init__(self, historian, archive_collection: archives.Collection):
        super().__init__(
            historian,
            archive_collection,
            record_factory=lambda record_dict: LoadableRecord(
                record_dict,
                historian.load_snapshot_from_record,
                # pylint: disable=protected-access
                historian._load_object_from_record,
            ),
            obj_loader=historian._load_object_from_record,
        )


class LoadableRecord(recordsm.DataRecord):
    __slots__ = ()
    _obj_loader: Optional[Callable[[recordsm.DataRecord], object]] = None
    _snapshot_loader: Optional[Callable[[recordsm.DataRecord], object]] = None

    def __new__(
        cls,
        record_dict: dict,
        snapshot_loader: Callable[[recordsm.DataRecord], object],
        obj_loader: Callable[[recordsm.DataRecord], object],
    ):
        loadable = super().__new__(cls, **record_dict)
        loadable._obj_loader = obj_loader
        loadable._snapshot_loader = snapshot_loader
        return loadable

    def load_snapshot(self) -> object:
        return self._snapshot_loader(self)  # pylint: disable=not-callable

    def load(self) -> object:
        return self._obj_loader(self)  # pylint: disable=not-callable
