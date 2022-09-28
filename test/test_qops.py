# -*- coding: utf-8 -*-
from mincepy import qops


def test_and():
    # If there's only one condition then there is no need to 'and' things together
    assert qops.and_("single") == "single"


def test_or():
    assert qops.or_("single") == "single"


def test_eq():
    assert qops.eq_("value") == {"$eq": "value"}


def test_gt():
    assert qops.gt_("value") == {"$gt": "value"}


def test_gte():
    assert qops.gte_("value") == {"$gte": "value"}


def test_lte():
    assert qops.lte_("value") == {"$lte": "value"}


def test_ne():
    assert qops.ne_("value") == {"$ne": "value"}


def test_nin():
    assert qops.nin_("value") == {"$ne": "value"}
    assert qops.nin_("value", "value2") == {"$nin": ["value", "value2"]}
