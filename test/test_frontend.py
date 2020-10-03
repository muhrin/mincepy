# -*- coding: utf-8 -*-
from mincepy import frontend
from mincepy import testing

# pylint: disable=invalid-name


def test_collection(historian):

    def identity(x):
        return x

    coll = frontend.EntriesCollection(historian.archive.objects,
                                      entry_factory=identity,
                                      obj_id_factory=identity,
                                      type_id_factory=identity)
    p1 = testing.Person('martin', 35)
    p1.save()
    p2 = testing.Person('john', 5)
    p2.save()

    p1.age = 36
    p1.save()

    # Find by obj ID
    records = coll.find(obj_id=p1.obj_id).one()
    assert isinstance(records, dict)
    assert records['obj_id'] == p1.obj_id

    # Find using multiple obj IDs
    records = list(coll.find(obj_id=[p1.obj_id, p2.obj_id]))
    assert len(records) == 2
    assert {records[0]['obj_id'], records[1]['obj_id']} == {p1.obj_id, p2.obj_id}

    c1 = testing.Car()
    c1.save()

    records = list(coll.find(obj_type=[testing.Car.TYPE_ID, testing.Person.TYPE_ID]))
    assert len(records) == 3
    assert {records[0]['type_id'], records[1]['type_id'], records[2]['type_id']} == \
           {testing.Car.TYPE_ID, testing.Person.TYPE_ID}
