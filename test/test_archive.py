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
    historian.save_one(node)

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


def test_meta_set_update_many(historian: mincepy.Historian):
    car1 = Car()
    car2 = Car()
    car1id, car2id = historian.save(car1, car2)
    historian.archive.meta_set_many({car1id: {'reg': 'car1'}, car2id: {'reg': 'car2'}})

    results = historian.archive.meta_get_many((car1id, car2id))
    assert results == {car1id: {'reg': 'car1'}, car2id: {'reg': 'car2'}}

    historian.archive.meta_update_many({car1id: {'colour': 'red'}, car2id: {'reg': 'car2updated'}})

    metas = historian.archive.meta_get_many((car1id, car2id))
    assert metas == {car1id: {'reg': 'car1', 'colour': 'red'}, car2id: {'reg': 'car2updated'}}


def test_meta_find(historian: mincepy.Historian):
    car1 = Car()
    car2 = Car()

    car1id, _ = historian.save(car1, car2)
    historian.archive.meta_set(car1id, {'reg': 'car1'})

    results = dict(historian.archive.meta_find({}, (car1id,)))
    assert results == {car1id: {'reg': 'car1'}}


def test_meta_update_many(historian: mincepy.Historian):
    car1 = Car()
    car2 = Car()
    car1id, car2id = historian.save(car1, car2)
    historian.archive.meta_set_many({car1id: {'reg': 'car1'}, car2id: {'reg': 'car2'}})

    results = historian.archive.meta_get_many((car1id, car2id))
    assert results[car1id] == {'reg': 'car1'}
    assert results[car2id] == {'reg': 'car2'}


def test_count(historian: mincepy.Historian):
    car1 = Car('ferrari')
    car2 = Car('skoda')
    car1id, car2id = historian.save(car1, car2)
    historian.archive.meta_set_many({car1id: {'reg': 'car1'}, car2id: {'reg': 'car2'}})

    assert historian.archive.count() == 2
    assert historian.archive.count(state=dict(make='ferrari')) == 1
    assert historian.archive.count(meta=dict(reg='car1')) == 1
