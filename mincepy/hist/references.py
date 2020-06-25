from typing import Generic, TypeVar, Set, overload

import networkx
from networkx.algorithms import dag

from mincepy import archives
from mincepy import records
from mincepy import operations
from mincepy import transactions

__all__ = ('References',)

IdT = TypeVar('IdT')  # The archive ID type


class References(Generic[IdT]):
    """
    A class that can provide reference graph information about objects stored in the archive.

    .. note::
        It is deliberately not possible to pass an object directly to methods in this class as what
        is returned is reference information from the archive and _not_ reference information about
        the in-memory python object.
    """

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
        """Get the ids of the objects referred to by the passed identifier."""
        if isinstance(identifier, records.SnapshotId):
            graph = self.get_snapshot_ref_graph(identifier, max_dist=1)
        elif isinstance(identifier, self._archive.get_id_type()):
            graph = self.get_obj_ref_graph(identifier, max_dist=1)
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
            graph = self.get_snapshot_ref_graph(identifier, direction=archives.INCOMING, max_dist=1)
        elif isinstance(identifier, self._archive.get_id_type()):
            graph = self.get_obj_ref_graph(identifier, direction=archives.INCOMING, max_dist=1)
        else:
            raise TypeError(identifier)

        return set(edge[0] for edge in graph.edges)

    def get_snapshot_ref_graph(self,
                               *snapshot_ids: SnapshotId,
                               direction=archives.OUTGOING,
                               max_dist: int = None) -> networkx.DiGraph:

        return self._archive.get_snapshot_ref_graph(*snapshot_ids,
                                                    direction=direction,
                                                    max_dist=max_dist)

    def get_obj_ref_graph(self,
                          *obj_ids: IdT,
                          direction=archives.OUTGOING,
                          max_dist: int = None) -> networkx.DiGraph:
        obj_ids = set(obj_ids)
        graph = self._archive.get_obj_ref_graph(*obj_ids, direction=direction, max_dist=max_dist)

        # If there is a transaction then we should fix up the graph to contain information from that
        # too
        trans = self._historian.current_transaction()  # type: transactions.Transaction
        if trans is not None:
            _update_from_transaction(graph, trans)

            # Now cull all the nodes not reachable from the nodes of interest

            # Now, get the subgraph we're interested in
            reachable = set()
            for obj_id in obj_ids:
                if direction == archives.OUTGOING:
                    reachable.update(dag.descendants(graph, obj_id))
                else:
                    reachable.update(dag.ancestors(graph, obj_id))

            # Remove all non-reachable nodes except obj_ids as these can stay even if they have no
            # edges
            graph.remove_nodes_from(set(graph.nodes) - obj_ids - reachable)

        return graph


def _update_from_transaction(graph: networkx.DiGraph, transaction: transactions.Transaction):
    """Given a transaction update the reference graph to reflect the insertion of any new records"""
    for op in transaction.staged:  # pylint: disable=invalid-name
        if isinstance(op, operations.Insert):
            # Modify the graph to reflect the insertion
            obj_id = op.obj_id
            if obj_id in graph.nodes:
                # Incoming edges (things referencing this object) can stay, as they haven't
                # changed but outgoing edges may have
                out_edges = tuple(graph.out_edges(obj_id))
                graph.remove_edges_from(out_edges)

            # And add in the current ones
            for refs in op.record.get_references():
                graph.add_edge(obj_id, refs[1].obj_id)
