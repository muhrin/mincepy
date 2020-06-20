import pytest

import mincepy
from mincepy.testing import Person, Car, Garage

# pylint: disable=invalid-name


def test_references_wrong_type(historian: mincepy.Historian):
    with pytest.raises(TypeError):
        historian.references.references(1234)


def test_referenced_by_wrong_type(historian: mincepy.Historian):
    with pytest.raises(TypeError):
        historian.references.referenced_by(1234)


def test_references_simple(historian: mincepy.Historian):
    address_book = mincepy.RefList()
    for i in range(3):
        address_book.append(Person(name='test', age=i))
    address_book.save()

    refs = historian.references.references(address_book.obj_id)
    assert len(refs) == len(address_book)
    assert not set(person.obj_id for person in address_book) - set(refs)

    refs = historian.references.referenced_by(address_book[0].obj_id)
    assert len(refs) == 1
    assert address_book.obj_id in refs


def test_remove_references_in_transaction(historian: mincepy.Historian):
    car = Car()
    garage = Garage(mincepy.ObjRef(car))
    gid = historian.save(garage)

    graph = historian.references.get_obj_ref_graph(gid)
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
        graph = historian.references.get_obj_ref_graph(gid)

        assert len(graph.nodes) == 1
        assert gid in graph.nodes
        assert len(graph.edges) == 0

        reffed_by = historian.references.referenced_by(car.obj_id)
        assert len(reffed_by) == 0

    assert len(historian.references.referenced_by(car.obj_id)) == 0
    assert len(historian.references.references(gid)) == 0


def test_add_references_in_transaction(historian: mincepy.Historian):
    car = Car()
    garage = mincepy.RefList()
    garage.append(car)
    historian.save(garage)

    refs = historian.references.references(garage.obj_id)
    assert len(refs) == 1
    assert car.obj_id in refs

    # Now add a car
    with historian.transaction():
        car2 = Car()
        garage.append(car2)
        garage.save()

        # Still in a transaction, check the references are correct
        refs = historian.references.references(garage.obj_id)
        assert len(refs) == 2
        assert car.obj_id in refs
        assert car2.obj_id in refs

        reffed_by = historian.references.referenced_by(car2.obj_id)
        assert len(reffed_by) == 1
        assert garage.obj_id in reffed_by

    refs = historian.references.references(garage.obj_id)
    assert len(refs) == 2
    assert car.obj_id in refs
    assert car2.obj_id in refs

    reffed_by = historian.references.referenced_by(car2.obj_id)
    assert len(reffed_by) == 1
    assert garage.obj_id in reffed_by


def test_snapshot_references(historian: mincepy.Historian):
    address_book = mincepy.RefList()
    for i in range(3):
        address_book.append(Person(name='test', age=i))
    address_book.save()
    sid = historian.get_snapshot_id(address_book)

    refs = historian.references.references(sid)
    assert len(refs) == len(address_book)
    assert not set(historian.get_snapshot_id(person) for person in address_book) - set(refs)


def test_snapshot_referenced_by(historian: mincepy.Historian):
    address_book = mincepy.RefList()
    for i in range(3):
        address_book.append(Person(name='test', age=i))
    address_book.save()
    sid = historian.get_snapshot_id(address_book)

    refs = historian.references.referenced_by(historian.get_snapshot_id(address_book[0]))
    assert len(refs) == 1
    assert sid in refs
