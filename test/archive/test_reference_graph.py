import mincepy
from mincepy.testing import Car, Garage, Cycle


def test_get_snapshot_graph_simple(historian: mincepy.Historian):
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    garage.save()
    garage_sid = historian.get_snapshot_id(garage)

    ref_graphs = historian.archive.get_snapshot_ref_graph((garage_sid,))

    assert len(ref_graphs) == 1
    garage_graph = ref_graphs[0]
    assert len(garage_graph) == 1
    assert garage_graph[0] == (garage_sid, historian.get_snapshot_id(car))


def test_get_snapshot_self_cycle(historian: mincepy.Historian):
    node = Cycle()
    node.ref = node  # Cycle complete
    historian.save_one(node)

    node_ref = historian.get_snapshot_id(node)
    ref_graphs = historian.archive.get_snapshot_ref_graph((node_ref,))
    assert len(ref_graphs) == 1

    node_graph = ref_graphs[0]
    assert len(node_graph) == 1
    assert node_graph[0] == (node_ref, node_ref)


def test_get_snapshot_graph_cycle(historian: mincepy.Historian):
    node1 = Cycle()
    node2 = Cycle(node1)
    node3 = Cycle(node2)
    node1.ref = node3  # Cycle complete

    historian.save(node1, node2, node3)

    node1ref = historian.get_snapshot_id(node1)
    node2ref = historian.get_snapshot_id(node2)
    node3ref = historian.get_snapshot_id(node3)

    ref_graphs = historian.archive.get_snapshot_ref_graph((node1ref,))

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


def test_get_snapshot_graph_twice(historian: mincepy.Historian):
    """Check for a bug that arise when asking for references twice"""
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    garage.save()
    garage_sref = historian.get_snapshot_id(garage)

    def make_checks(graph):
        assert len(graph) == 1
        garage_graph = graph[0]
        assert len(garage_graph) == 1
        assert garage_graph[0] == (garage_sref, historian.get_snapshot_id(car))

    ref_graphs = historian.archive.get_snapshot_ref_graph((garage_sref,))
    make_checks(ref_graphs)

    # Check again
    ref_graphs = historian.archive.get_snapshot_ref_graph((garage_sref,))
    make_checks(ref_graphs)


def test_get_object_graph(historian: mincepy.Historian):
    """Try getting the reference graph for live objects"""
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    gid = garage.save()

    graph = historian.archive.get_obj_ref_graph((gid,))
    assert len(graph) == 1
    garage_graph = graph[0]
    assert len(garage_graph) == 1
    assert garage_graph[0] == (gid, car.obj_id)


def test_get_object_graph_current(historian: mincepy.Historian):
    """Test that when changing a reference the reference graph is returned correctly"""
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    gid = garage.save()

    garage_graph = historian.archive.get_obj_ref_graph((gid,))[0]
    assert len(garage_graph) == 1
    assert garage_graph[0] == (gid, car.obj_id)

    # Now, modify the garage
    car2 = Car()
    garage.car = mincepy.ObjRef(car2)
    garage.save()

    # Check that the reference graph is correct
    garage_graph = historian.archive.get_obj_ref_graph((gid,))[0]
    assert len(garage_graph) == 1
    assert garage_graph[0] == (gid, car2.obj_id)

    # Finally, set the reference to None
    garage.car = mincepy.ObjRef()
    garage.save()

    # Check that the reference graph is correct
    garage_graph = historian.archive.get_obj_ref_graph((gid,))[0]
    assert len(garage_graph) == 0
