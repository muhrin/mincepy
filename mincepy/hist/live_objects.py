from typing import TYPE_CHECKING, Callable, Optional

from mincepy import frontend
from mincepy import records as records_

if TYPE_CHECKING:
    import mincepy

__all__ = ("LiveObjectsCollection",)


class LiveObjectsCollection(frontend.ObjectCollection):
    def __init__(
        self, historian: "mincepy.Historian", archive_collection: "mincepy.archives.Collection"
    ):
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


class LoadableRecord(records_.DataRecord):
    __slots__ = ()
    _obj_loader: Optional[Callable[["mincepy.DataRecord"], object]] = None
    _snapshot_loader: Optional[Callable[["mincepy.DataRecord"], object]] = None

    def __new__(
        cls,
        record_dict: dict,
        snapshot_loader: Callable[["mincepy.DataRecord"], object],
        obj_loader: Callable[["mincepy.DataRecord"], object],
    ):
        loadable = super().__new__(cls, **record_dict)
        loadable._obj_loader = obj_loader
        loadable._snapshot_loader = snapshot_loader
        return loadable

    def load_snapshot(self) -> object:
        return self._snapshot_loader(self)  # pylint: disable=not-callable

    def load(self) -> object:
        return self._obj_loader(self)  # pylint: disable=not-callable
