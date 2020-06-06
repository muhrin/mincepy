from typing import Union, Generic, TypeVar, Iterator, Set, overload  # pylint: disable=unused-import

import networkx
from networkx.algorithms import dag

from mincepy import archives
from mincepy import records
from mincepy import operations
from mincepy import transactions  # pylint: disable=unused-import

__all__ = ('References',)

IdT = TypeVar('IdT')  # The archive ID type


class References(Generic[IdT]):

    def __init__(self, historian):
        self._historian = historian
        self._archive = historian.archive  # type: archives.Archive

    SnapshotId = records.SnapshotId[IdT]

    @overload
    def references(self, identifier: IdT) -> Set[IdT]:  # pylint: disable=no-self-use
        ...

    @overload
    def references(self, identifier: 'SnapshotId') -> 'Set[SnapshotId]':  # pylint: disable=no-self-use
        ...

    def references(self, identifier):
        """Get the ids of the objects referred to by the passed object"""
        if isinstance(identifier, records.SnapshotId):
            graph = next(self.get_snapshot_ref_graph(identifier, max_dist=1))
        elif isinstance(identifier, self._archive.get_id_type()):
            graph = next(self.get_obj_ref_graph(identifier, max_dist=1))
        else:
            raise TypeError(identifier)

        return set(edge[1] for edge in graph.edges)

    @overload
    def referenced_by(self, identifier: IdT) -> 'Set[IdT]':  # pylint: disable=no-self-use
        ...

    @overload
    def referenced_by(self, identifier: 'SnapshotId') -> 'Set[SnapshotId]':  # pylint: disable=no-self-use
        ...

    def referenced_by(self, identifier):
        """Get the ids of the objects that refer to the passed object"""
        if isinstance(identifier, records.SnapshotId):
            graph = next(
                self.get_snapshot_ref_graph(identifier, direction=archives.INCOMING, max_dist=1))
        elif isinstance(identifier, self._archive.get_id_type()):
            graph = next(self.get_obj_ref_graph(identifier, direction=archives.INCOMING,
                                                max_dist=1))
        else:
            raise TypeError(identifier)

        return set(edge[0] for edge in graph.edges)

    def get_snapshot_ref_graph(self,
                               *snapshot_ids: SnapshotId,
                               direction=archives.OUTGOING,
                               max_dist: int = None) -> Iterator[networkx.DiGraph]:

        yield from self._archive.get_snapshot_ref_graph(*snapshot_ids,
                                                        direction=direction,
                                                        max_dist=max_dist)

    def get_obj_ref_graph(self,
                          *obj_ids: IdT,
                          direction=archives.OUTGOING,
                          max_dist: int = None) -> Iterator[networkx.DiGraph]:

        for source, graph in zip(
                obj_ids,
                self._archive.get_obj_ref_graph(*obj_ids, direction=direction, max_dist=max_dist)):
            trans = self._historian.current_transaction()  # type: transactions.Transaction
            if trans is not None:
                for op in trans.staged:  # pylint: disable=invalid-name
                    if isinstance(op, operations.Insert):
                        # Modify the graph to reflect the insertion
                        obj_id = op.obj_id
                        if obj_id in graph.nodes:
                            # Remove all out outgoing edges from this node
                            out_edges = tuple(graph.out_edges(obj_id))
                            graph.remove_edges_from(out_edges)
                        # And add in the current ones
                        for refs in op.record.get_references():
                            graph.add_edge(obj_id, refs[1].obj_id)

                # Now, get the subgraph we're interested in
                if direction == archives.OUTGOING:
                    reachable = dag.descendants(graph, source)
                else:
                    reachable = dag.ancestors(graph, source)
                # Remove all non-reachable nodes except ourselves
                graph.remove_nodes_from(set(graph.nodes) - {source} - reachable)

            yield graph
