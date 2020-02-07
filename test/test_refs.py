from argparse import Namespace

import pytest

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
    car = Car()
    nested1 = Namespace()
    nested2 = Namespace()
    parent = Namespace()

    # Both the nested refer to the same car
    nested1.car = car
    nested2.car = car

    # Now put them in their containers
    parent.ns1 = nested1
    parent.ns2 = nested2

    parent_id = historian.save(parent)
