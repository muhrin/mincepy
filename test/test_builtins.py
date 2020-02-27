import mincepy
from mincepy.testing import *


def test_ref_list(historian: mincepy.Historian):
    """Test references list"""
    list1 = mincepy.builtins.RefList()
    list2 = mincepy.builtins.RefList()
    car = Car()
    list1.append(car)
    list2.append(car)

    # Reference condition satisfied
    assert list1[0] is list2[0]

    # Now delete everything, reload, and make sure the condition is still satisfied
    list1_id, list2_id = historian.save(list1, list2)
    del list1, list2, car

    list1_loaded = historian.load(list1_id)
    list2_loaded = historian.load(list2_id)
    assert len(list1_loaded) == 1
    assert len(list2_loaded) == 1
    assert isinstance(list1_loaded[0], Car)
    assert list1_loaded[0] is list2_loaded[0]


def test_ref_dict(historian: mincepy.Historian):
    """Test that the ref dict correctly stores entries as references"""
    dict1 = mincepy.builtins.RefDict()
    dict2 = mincepy.builtins.RefDict()
    car = Car()
    dict1['car'] = car
    dict2['car'] = car

    # Reference condition satisfied
    assert dict1['car'] is dict2['car']

    # Now delete everything, reload, and make sure the condition is still satisfied
    dict1_id, dict2_id = historian.save(dict1, dict2)
    del dict1, dict2, car

    dict1_loaded = historian.load(dict1_id)
    dict2_loaded = historian.load(dict2_id)
    assert len(dict1_loaded) == 1
    assert len(dict2_loaded) == 1
    assert isinstance(dict1_loaded['car'], Car)
    assert dict1_loaded['car'] is dict2_loaded['car']


def test_live_list(historian: mincepy.Historian):
    """Basic tests on live list. It's difficult to test the 'syncing' aspects
    of this container without another process"""
    live_list = mincepy.builtins.LiveList()

    live_list.append(0)
    wrapper_id = live_list.save()
    del live_list

    live_list = historian.load(wrapper_id)
    assert len(live_list) == 1

    # Test getitem
    live_list.append(1)
    live_list.append(2)
    assert live_list[2] == 2

    # Test delete
    del live_list[2]
    assert len(live_list) == 2


def test_live_dict(historian: mincepy.Historian):
    """Test live dictionary, difficult to test the sync capability"""
    dict1 = mincepy.builtins.LiveDict()
    dict2 = mincepy.builtins.LiveDict()
    car = Car('mini', 'green')
    dict1['car'] = car
    dict2['car'] = car

    # Reference condition satisfied
    assert dict1['car'] is dict2['car']

    # Now delete everything, reload, and make sure the condition is still satisfied
    dict1_id, dict2_id = historian.save(dict1, dict2)
    del dict1, dict2, car

    dict1_loaded = historian.load(dict1_id)
    dict2_loaded = historian.load(dict2_id)
    assert len(dict1_loaded) == 1
    assert len(dict2_loaded) == 1
    assert isinstance(dict1_loaded['car'], Car)
    assert dict1_loaded['car'] == dict2_loaded['car']
