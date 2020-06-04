from argparse import Namespace

import mincepy
import mincepy.records
from mincepy.testing import Car, Cycle

# pylint: disable=invalid-name


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
    historian.save(ns)
    honda_ns_sid = historian.get_snapshot_id(ns)

    car.make = 'fiat'
    historian.save(ns)
    fiat_ns_sid = historian.get_snapshot_id(ns)
    del ns

    assert fiat_ns_sid.version == honda_ns_sid.version + 1

    loaded = historian.load(honda_ns_sid)
    assert loaded.car().make == 'honda'

    loaded2 = historian.load(fiat_ns_sid)
    assert loaded2.car().make == 'fiat'
    assert loaded2.car() is not car

    # Load the 'live' namespace
    loaded3 = historian.load(honda_ns_sid.obj_id)
    assert loaded3.car() is car


def test_obj_sid_complex(historian: mincepy.Historian):
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
    historian.save(loaded)
    parent_sid = historian.get_snapshot_id(loaded)
    del loaded

    loaded2 = historian.load_snapshot(mincepy.records.SnapshotId(parent_id, 0))
    assert loaded2.ns1().car().make == 'honda'
    assert loaded2.ns2().car().make == 'honda'
    del loaded2

    loaded3 = historian.load_snapshot(parent_sid)
    assert loaded3.ns1().car().make == 'honda'
    assert loaded3.ns2().car().make == 'fiat'


def test_null_ref(historian: mincepy.Historian):
    null = mincepy.ObjRef()
    null2 = mincepy.ObjRef()

    assert null == null2

    nid1, _nid2 = historian.save(null, null2)
    del null
    loaded = historian.load(nid1)
    assert loaded == null2


def test_ref_load_save_load(historian: mincepy.Historian):
    """This is here to catch a bug that manifested when a reference was saved, loaded and then
    re-saved without being dereferenced in-between.  This would result in the second saved state
    being that of a null reference.  This can only be tested if the reference is stored by value
    as otherwise the historian will not re-save a reference that has not been mutated."""
    ref_list = mincepy.List((mincepy.ObjRef(Car()),))
    assert isinstance(ref_list[0](), Car)

    list_id = ref_list.save()
    del ref_list

    loaded = historian.load(list_id)
    # Re-save
    loaded.save()
    del loaded

    # Re-load
    reloaded = historian.load(list_id)
    # Should still be our car but because of a bug this was a None reference
    assert isinstance(reloaded[0](), Car)
