from typing import Union, Generic, TypeVar, List

from mincepy import archives
from mincepy import records

__all__ = ('References',)

IdT = TypeVar('IdT')  # The archive ID type


class References(Generic[IdT]):

    def __init__(self, archive: archives.Archive):
        self._archive = archive

    SnapshotId = records.SnapshotId[IdT]

    def references(self, identifier: 'Union[IdT, SnapshotId]') -> 'List[Union[IdT, SnapshotId]]':
        """Get the ids of the objects referred to by the passed object"""
        if isinstance(identifier, records.SnapshotId):
            graph = next(self._archive.get_snapshot_ref_graph(identifier, max_depth=1))
        elif isinstance(identifier, self._archive.get_id_type()):
            graph = next(self._archive.get_obj_ref_graph(identifier, max_depth=1))
        else:
            raise TypeError(identifier)

        return [edge.target for edge in graph]

    def referenced_by(self, identifier: 'Union[IdT, SnapshotId]') -> 'List[Union[IdT, SnapshotId]]':
        """Get the ids of the objects that refer to the passed object"""
        if isinstance(identifier, records.SnapshotId):
            graph = next(
                self._archive.get_snapshot_ref_graph(identifier,
                                                     direction=archives.BACKWARDS,
                                                     max_depth=1))
        elif isinstance(identifier, self._archive.get_id_type()):
            graph = next(
                self._archive.get_obj_ref_graph(identifier,
                                                direction=archives.BACKWARDS,
                                                max_depth=1))
        else:
            raise TypeError(identifier)

        return [edge.source for edge in graph]
