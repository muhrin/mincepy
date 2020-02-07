from argparse import Namespace

import mincepy
from mincepy.testing import Car, Cycle


def test_obj_ref_simple(historian: mincepy.Historian):
    a = Cycle()
    a.ref = mincepy.ObjRef(a)
    aid = historian.save(a)
    del a

    loaded = historian.load(aid)
    assert loaded.ref() is loaded


def test_obj_ref_snapshot(historian: mincepy.Historian):
    """Check that a historic snapshot still works with references"""
    ns = Namespace()
    car = Car('honda', 'white')
    ns.car = mincepy.ObjRef(car)
    ns_id_honda = historian.save(ns, return_sref=True)

    car.make = 'fiat'
    ns_id_fiat = historian.save(ns, return_sref=True)
    del ns

    assert ns_id_fiat.version == ns_id_honda.version + 1

    loaded = historian.load(ns_id_honda)
    assert loaded.car().make == 'honda'

    loaded2 = historian.load(ns_id_fiat)
    assert loaded2.car().make == 'fiat'
    assert loaded2.car() is not car

    # Load the 'live' namespace
    loaded3 = historian.load(ns_id_honda.obj_id)
    assert loaded3.car() is car


def test_obj_ref_complex(historian: mincepy.Historian):
    honda = Car('honda')
    nested1 = Namespace()
    nested2 = Namespace()
    parent = Namespace()

    # Both the nested refer to the same car
    nested1.car = mincepy.ObjRef(honda)
    nested2.car = mincepy.ObjRef(honda)

    # Now put them in their containers
    parent.ns1 = mincepy.ObjRef(nested1)
    parent.ns2 = mincepy.ObjRef(nested2)

    parent_id = historian.save(parent)
    del parent

    loaded = historian.load(parent_id)
    assert loaded.ns1() is nested1
    assert loaded.ns2() is nested2
    assert loaded.ns1().car() is loaded.ns2().car()

    fiat = Car('fiat')
    loaded.ns2().car = mincepy.ObjRef(fiat)
    parent_snapshot_id = historian.save(loaded, return_sref=True)
    del loaded

    loaded2 = historian.load_snapshot(mincepy.Ref(parent_id, 0))
    assert loaded2.ns1().car().make == 'honda'
    assert loaded2.ns2().car().make == 'honda'
    del loaded2

    loaded3 = historian.load_snapshot(parent_snapshot_id)
    assert loaded3.ns1().car().make == 'honda'
    assert loaded3.ns2().car().make == 'fiat'
