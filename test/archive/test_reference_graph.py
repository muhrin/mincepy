import mincepy
from mincepy.testing import Car, Garage, Cycle


def test_get_snapshot_graph_simple(historian: mincepy.Historian):
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    garage.save()
    garage_sid = historian.get_snapshot_id(garage)

    garage_graph = historian.archive.get_snapshot_ref_graph(garage_sid)
    assert len(garage_graph.edges) == 1
    assert (garage_sid, historian.get_snapshot_id(car)) in garage_graph.edges


def test_get_snapshot_self_cycle(historian: mincepy.Historian):
    node = Cycle()
    node.ref = node  # Cycle complete
    historian.save_one(node)

    node_ref = historian.get_snapshot_id(node)
    node_graph = historian.archive.get_snapshot_ref_graph(node_ref)
    assert len(node_graph.edges) == 1
    assert (node_ref, node_ref) in node_graph.edges


def test_get_snapshot_graph_cycle(historian: mincepy.Historian):
    node1 = Cycle()
    node2 = Cycle(node1)
    node3 = Cycle(node2)
    node1.ref = node3  # Cycle complete

    historian.save(node1, node2, node3)

    node1ref = historian.get_snapshot_id(node1)
    node2ref = historian.get_snapshot_id(node2)
    node3ref = historian.get_snapshot_id(node3)

    graph = historian.archive.get_snapshot_ref_graph(node1ref)
    assert len(graph.edges) == 3

    # Created the edges to check
    n13 = (node1ref, node3ref)
    n21 = (node2ref, node1ref)
    n32 = (node3ref, node2ref)

    assert n13 in graph.edges
    assert n21 in graph.edges
    assert n32 in graph.edges


def test_get_snapshot_graph_twice(historian: mincepy.Historian):
    """Check for a bug that arise when asking for references twice"""
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    garage.save()
    garage_sid = historian.get_snapshot_id(garage)

    def make_checks(graph):
        assert len(graph.edges) == 1
        assert (garage_sid, historian.get_snapshot_id(car)) in graph.edges

    ref_graphs = historian.archive.get_snapshot_ref_graph(garage_sid)
    make_checks(ref_graphs)

    # Check again
    ref_graphs = historian.archive.get_snapshot_ref_graph(garage_sid)
    make_checks(ref_graphs)


def test_get_object_graph(historian: mincepy.Historian):
    """Try getting the reference graph for live objects"""
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    gid = garage.save()

    garage_graph = historian.archive.get_obj_ref_graph(gid)
    assert len(garage_graph.edges) == 1
    assert (gid, car.obj_id) in garage_graph.edges


def test_get_obj_graph_current(historian: mincepy.Historian):
    """Test that when changing a reference the reference graph is returned correctly"""
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    gid = garage.save()

    garage_graph = historian.archive.get_obj_ref_graph(gid)
    assert len(garage_graph.edges) == 1
    assert (gid, car.obj_id) in garage_graph.edges

    # Now, modify the garage
    car2 = Car()
    garage.car = mincepy.ObjRef(car2)
    garage.save()

    # Check that the reference graph is correct
    garage_graph = historian.archive.get_obj_ref_graph(gid)
    assert len(garage_graph.edges) == 1
    assert (gid, car2.obj_id) in garage_graph.edges

    # Finally, set the reference to None
    garage.car = mincepy.ObjRef()
    garage.save()

    # Check that the reference graph is correct
    garage_graph = historian.archive.get_obj_ref_graph(gid)
    assert len(garage_graph.edges) == 0
    assert len(garage_graph.nodes) == 1


def test_get_obj_referencing_simple(historian: mincepy.Historian):
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    gid = garage.save()

    car_graph = historian.archive.get_obj_ref_graph(car.obj_id, direction=mincepy.INCOMING)
    assert len(car_graph.edges) == 1
    assert len(car_graph.nodes) == 2
    assert (gid, car.obj_id) in car_graph.edges

    # Now, create a new garage
    garage2 = Garage(mincepy.ObjRef(car))
    g2id = garage2.save()

    # Check that the reference graph is correct
    car_graph = historian.archive.get_obj_ref_graph(car.obj_id, direction=mincepy.INCOMING)
    assert len(car_graph.nodes) == 3
    assert len(car_graph.edges) == 2
    assert (gid, car.obj_id) in car_graph.edges
    assert (g2id, car.obj_id) in car_graph.edges

    # Finally, set the references to None
    garage.car = mincepy.ObjRef()
    garage.save()
    garage2.car = mincepy.ObjRef()
    garage2.save()

    # Check that the reference graph is correct
    car_graph = historian.archive.get_obj_ref_graph(gid)
    assert len(car_graph.edges) == 0
    assert len(car_graph.nodes) == 1


def test_obj_ref_max_depth(historian: mincepy.Historian):
    """Test object references max depth"""
    # Set up the chain: three -> two -> one -> zero
    zero = mincepy.ObjRef()
    one = mincepy.ObjRef(zero)
    two = mincepy.ObjRef(one)
    three = mincepy.ObjRef(two)
    zero_id, one_id, two_id, three_id = historian.save(zero, one, two, three)

    # No max depth
    graph = historian.archive.get_obj_ref_graph(two_id)
    assert len(graph.nodes) == 3
    assert len(graph.edges) == 2
    assert (one_id, zero_id) in graph.edges
    assert (two_id, one_id) in graph.edges

    graph = historian.archive.get_obj_ref_graph(three_id, max_dist=3)
    assert len(graph.nodes) == 4
    assert len(graph.edges) == 3
    assert (one_id, zero_id) in graph.edges
    assert (two_id, one_id) in graph.edges
    assert (three_id, two_id) in graph.edges

    graph = historian.archive.get_obj_ref_graph(two_id, max_dist=1)
    assert len(graph.edges) == 1
    assert (two_id, one_id) in graph.edges

    graph = historian.archive.get_obj_ref_graph(two_id, max_dist=0)
    assert len(graph.nodes) == 1
    assert len(graph.edges) == 0


def test_obj_referencing_max_depth(historian: mincepy.Historian):
    """Test object referencing max depth"""
    # Set up the chain: three -> two -> one -> zero
    zero = mincepy.ObjRef()
    one = mincepy.ObjRef(zero)
    two = mincepy.ObjRef(one)
    three = mincepy.ObjRef(two)
    zero_id, one_id, two_id, three_id = historian.save(zero, one, two, three)

    # No max depth
    graph = historian.archive.get_obj_ref_graph(one_id, direction=mincepy.INCOMING)
    assert len(graph.edges) == 2
    assert len(graph.nodes) == 3
    assert (three_id, two_id) in graph.edges
    assert (two_id, one_id) in graph.edges

    graph = historian.archive.get_obj_ref_graph(zero_id, direction=mincepy.INCOMING, max_dist=2)
    assert len(graph.edges) == 2
    assert len(graph.nodes) == 3
    assert (one_id, zero_id) in graph.edges
    assert (two_id, one_id) in graph.edges

    graph = historian.archive.get_obj_ref_graph(one_id, direction=mincepy.INCOMING, max_dist=1)
    assert len(graph.edges) == 1
    assert len(graph.nodes) == 2
    assert (two_id, one_id) in graph.edges

    graph = historian.archive.get_obj_ref_graph(two_id, direction=mincepy.INCOMING, max_dist=0)
    assert len(graph.edges) == 0
    assert len(graph.nodes) == 1
