import mincepy
from mincepy.testing import Person, Car, Garage


def test_references_simple(historian: mincepy.Historian):
    address_book = mincepy.RefList()
    for i in range(10):
        address_book.append(Person(name='test', age=i))
    address_book.save()

    refs = historian.references.references(address_book.obj_id)
    assert len(refs) == len(address_book)
    assert not set(person.obj_id for person in address_book) - set(refs)

    refs = historian.references.referenced_by(address_book[0].obj_id)
    assert len(refs) == 1
    assert address_book.obj_id in refs


def test_references_in_transaction(historian: mincepy.Historian):
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    gid = historian.save(garage)

    graph = next(historian.references.get_obj_ref_graph(gid))
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1
    assert (gid, car.obj_id) in graph.edges

    reffed_by = historian.references.referenced_by(car.obj_id)
    assert len(reffed_by) == 1
    assert gid in reffed_by

    # Now, change the reference in a transaction
    with historian.transaction():
        garage.car = None
        garage.save()
        # Still in a transaction, check the references are correct, i.e. None
        graph = next(historian.references.get_obj_ref_graph(gid))

        assert len(graph.nodes) == 1
        assert gid in graph.nodes
        assert len(graph.edges) == 0

    assert len(historian.references.referenced_by(car.obj_id)) == 0
    assert len(historian.references.references(gid)) == 0
