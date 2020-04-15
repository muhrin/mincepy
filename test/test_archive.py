import mincepy
from mincepy.testing import Car, Garage, Cycle


def test_get_ref_graph_simple(historian: mincepy.Historian):
    car = Car()
    garage = Garage(mincepy.ObjRef(car))

    garage.save()
    garage_sref = historian.get_snapshot_ref(garage)

    ref_graphs = historian.archive.get_reference_graph((garage_sref,))

    assert len(ref_graphs) == 1
    garage_graph = ref_graphs[0]
    assert len(garage_graph) == 1
    assert garage_graph[0] == (garage_sref, historian.get_snapshot_ref(car))


def test_get_ref_self_cycle(historian: mincepy.Historian):
    node = Cycle()
    node.ref = node  # Cycle complete
    historian.save(node)

    node_ref = historian.get_snapshot_ref(node)
    ref_graphs = historian.archive.get_reference_graph((node_ref,))
    assert len(ref_graphs) == 1

    node_graph = ref_graphs[0]
    assert len(node_graph) == 1
    assert node_graph[0] == (node_ref, node_ref)


def test_get_ref_graph_cycle(historian: mincepy.Historian):
    node1 = Cycle()
    node2 = Cycle(node1)
    node3 = Cycle(node2)
    node1.ref = node3  # Cycle complete

    historian.save(node1, node2, node3)

    node1ref = historian.get_snapshot_ref(node1)
    node2ref = historian.get_snapshot_ref(node2)
    node3ref = historian.get_snapshot_ref(node3)

    ref_graphs = historian.archive.get_reference_graph((node1ref,))

    assert len(ref_graphs) == 1

    node1_graph = ref_graphs[0]
    assert len(node1_graph) == 3

    # Created the edges to check
    n13 = (node1ref, node3ref)
    n21 = (node2ref, node1ref)
    n32 = (node3ref, node2ref)

    assert n13 in node1_graph
    assert n21 in node1_graph
    assert n32 in node1_graph
