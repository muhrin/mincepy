import mincepy
from mincepy.testing import Car, Garage, Cycle


def test_get_ref_graph_simple(historian: mincepy.Historian):
    car = Car()
    garage = Garage(mincepy.ObjRef(car))

    garage.save()
    garage_sid = historian.get_snapshot_id(garage)

    ref_graphs = historian.archive.get_reference_graph((garage_sid,))

    assert len(ref_graphs) == 1
    garage_graph = ref_graphs[0]
    assert len(garage_graph) == 1
    assert garage_graph[0] == (garage_sid, historian.get_snapshot_id(car))


def test_get_ref_self_cycle(historian: mincepy.Historian):
    node = Cycle()
    node.ref = node  # Cycle complete
    historian.save_one(node)

    node_sid = historian.get_snapshot_id(node)
    ref_graphs = historian.archive.get_reference_graph((node_sid,))
    assert len(ref_graphs) == 1

    node_graph = ref_graphs[0]
    assert len(node_graph) == 1
    assert node_graph[0] == (node_sid, node_sid)


def test_get_ref_graph_cycle(historian: mincepy.Historian):
    node1 = Cycle()
    node2 = Cycle(node1)
    node3 = Cycle(node2)
    node1.ref = node3  # Cycle complete

    historian.save(node1, node2, node3)

    node1sid = historian.get_snapshot_id(node1)
    node2sid = historian.get_snapshot_id(node2)
    node3sid = historian.get_snapshot_id(node3)

    ref_graphs = historian.archive.get_reference_graph((node1sid,))

    assert len(ref_graphs) == 1

    node1_graph = ref_graphs[0]
    assert len(node1_graph) == 3

    # Created the edges to check
    n13 = (node1sid, node3sid)
    n21 = (node2sid, node1sid)
    n32 = (node3sid, node2sid)

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


def test_find_from_id(historian: mincepy.Historian):
    car = Car('ferrari')
    car_id = car.save()

    results = tuple(historian.archive.find(obj_id=car_id))
    assert len(results) == 1
    assert results[0].obj_id == car_id

    # Now check that we can pass an iterable of ids
    car2 = Car('skoda')
    car2_id = car2.save()
    results = tuple(historian.archive.find(obj_id=[car_id, car2_id]))
    assert len(results) == 2
    ids = [record.obj_id for record in results]
    assert car_id in ids
    assert car2_id in ids
