# -*- coding: utf-8 -*-
import argparse
import collections
import pathlib
from typing import MutableMapping
import uuid

import pytest

import mincepy
from mincepy import testing
from mincepy import builtins


# region lists

# pylint: disable=redefined-outer-name


def test_ref_list(historian: mincepy.Historian):
    """Test references list"""
    list1 = mincepy.RefList()
    list2 = mincepy.RefList()
    car = testing.Car()
    list1.append(car)
    list2.append(car)

    # Test iteration
    for entry in list1:
        assert entry is car

    # Reference condition satisfied
    assert list1[0] is list2[0]

    # Now delete everything, reload, and make sure the condition is still satisfied
    list1_id, list2_id = historian.save(list1, list2)
    assert (
        car.is_saved()
    ), "The container should automatically save all it's entries saved"
    del list1, list2, car

    list1_loaded = historian.load(list1_id)
    list2_loaded = historian.load(list2_id)
    assert len(list1_loaded) == 1
    assert len(list2_loaded) == 1
    assert isinstance(list1_loaded[0], testing.Car)
    assert list1_loaded[0] is list2_loaded[0]


@pytest.mark.parametrize(
    "list_type", (builtins.List, builtins.LiveList, builtins.LiveRefList)
)
def test_list_primitives(list_type, historian: mincepy.Historian):
    """Test that we can store primitives in a ref list also"""
    reflist = list_type()
    reflist.append(5)
    reflist.append("hello")
    reflist.append(10.8)

    reflist_id = reflist.save()
    del reflist

    loaded = historian.load(reflist_id)
    assert loaded[0] == 5
    assert loaded[1] == "hello"
    assert loaded[2] == 10.8


@pytest.mark.parametrize("list_type", (builtins.LiveList, builtins.LiveRefList))
def test_live_lists(list_type, historian: mincepy.Historian):
    """Basic tests on live list. It's difficult to test the 'syncing' aspects
    of this container without another process"""
    live_list = list_type()

    car = testing.Car()

    live_list.append(car)
    wrapper_id = live_list.save()
    del live_list

    live_list = historian.load(wrapper_id)
    assert len(live_list) == 1

    # Test getitem
    live_list.append(testing.Car("honda"))
    live_list.append(testing.Car("fiat"))
    assert live_list[2].make == "fiat"

    # Test delete
    del live_list[2]
    assert len(live_list) == 2


def test_life_list_delete(historian: mincepy.Historian):
    llist = builtins.LiveRefList()
    car = testing.Car()
    llist.append(car)
    assert len(llist) == 1
    del llist[0]
    assert len(llist) == 0

    # Ok, now try when happens when we actually save
    llist.append(car)
    historian.save(llist)
    assert historian.is_saved(car)
    del llist[0]
    # The car should still be saved as the container is not considered to own it
    assert historian.is_saved(car)


def test_ref_list_none(historian: mincepy.Historian):
    """Test that we can store None in a ref list."""
    lrlist = mincepy.RefList()
    lrlist.append(None)
    assert lrlist[-1] is None
    lrdict_id = lrlist.save()
    del lrlist

    loaded = historian.load(lrdict_id)  # type: mincepy.RefList
    assert loaded[-1] is None


# endregion

# region dict


def test_ref_dict(historian: mincepy.Historian):
    """Test that the ref dict correctly stores entries as references"""
    dict1 = mincepy.builtins.RefDict()
    dict2 = mincepy.builtins.RefDict()
    car = testing.Car()
    dict1["car"] = car
    dict2["car"] = car

    # Test iteration
    for key, value in dict1.items():
        assert key == "car"
        assert value is car

    # Reference condition satisfied
    assert dict1["car"] is dict2["car"]

    # Now delete everything, reload, and make sure the condition is still satisfied
    dict1_id, dict2_id = historian.save(dict1, dict2)
    del dict1, dict2, car

    dict1_loaded = historian.load(dict1_id)
    dict2_loaded = historian.load(dict2_id)
    assert len(dict1_loaded) == 1
    assert len(dict2_loaded) == 1
    assert isinstance(dict1_loaded["car"], testing.Car)
    assert dict1_loaded["car"] is dict2_loaded["car"]


@pytest.mark.parametrize(
    "dict_type", (builtins.RefDict, builtins.LiveDict, builtins.LiveRefDict)
)
def test_primitives(dict_type, historian: mincepy.Historian):
    """Test that we can store primitives in a ref list also"""
    refdict = dict_type()
    refdict["0"] = 5
    refdict["1"] = "hello"
    refdict["2"] = 10.8

    reflist_id = refdict.save()
    del refdict

    loaded = historian.load(reflist_id)
    assert loaded["0"] == 5
    assert loaded["1"] == "hello"
    assert loaded["2"] == 10.8


@pytest.mark.parametrize(
    "dict_type", (builtins.RefDict, builtins.LiveDict, builtins.LiveRefDict)
)
def test_ref_dicts_iterate(dict_type: MutableMapping):
    to_store = {"car": testing.Car(), "msg": "hello", "number": 5}

    ref_dict = dict_type(to_store)
    for key, val in ref_dict.items():
        assert val is to_store[key]


@pytest.mark.parametrize("dict_type", (builtins.LiveDict, builtins.LiveRefDict))
def test_live_dicts(dict_type: MutableMapping, historian: mincepy.Historian):
    """Test live dictionary, difficult to test the sync capability"""
    dict1 = dict_type()
    dict2 = dict_type()
    car = testing.Car("mini", "green")
    dict1["car"] = car
    dict2["car"] = car

    # Reference condition satisfied
    assert dict1["car"] is dict2["car"]

    # Now delete everything, reload, and make sure the condition is still satisfied
    dict1_id, dict2_id = historian.save(dict1, dict2)
    del dict1, dict2, car

    dict1_loaded = historian.load(dict1_id)
    dict2_loaded = historian.load(dict2_id)
    assert len(dict1_loaded) == 1
    assert len(dict2_loaded) == 1
    assert isinstance(dict1_loaded["car"], testing.Car)
    assert dict1_loaded["car"] == dict2_loaded["car"]


def test_live_ref_dict(historian: mincepy.Historian):
    dict1 = builtins.LiveRefDict()
    dict2 = builtins.LiveRefDict()
    car = testing.Car("mini", "green")
    dict1["car"] = car
    dict2["car"] = car

    # Reference condition satisfied
    assert dict1["car"] is dict2["car"]

    # Now delete everything, reload, and make sure the condition is still satisfied
    dict1_id, dict2_id = historian.save(dict1, dict2)
    del dict1, dict2, car

    dict1_loaded = historian.load(dict1_id)
    dict2_loaded = historian.load(dict2_id)
    assert len(dict1_loaded) == 1
    assert len(dict2_loaded) == 1
    assert isinstance(dict1_loaded["car"], testing.Car)
    # Check they are the same object
    assert dict1_loaded["car"] is dict2_loaded["car"]


def test_ref_dict_none(historian: mincepy.Historian):
    """Test that we can store None in a ref dict."""
    lrdict = mincepy.LiveRefDict()
    lrdict["test"] = None
    assert lrdict["test"] is None
    lrdict_id = lrdict.save()
    del lrdict

    loaded = historian.load(lrdict_id)  # type: mincepy.RefDict
    assert loaded["test"] is None


# endregion


def test_live_ref_create(historian: mincepy.Historian):
    car = testing.Car("mini", "green")
    lrdict = builtins.LiveRefDict({"car": car})
    lrdict["car"] = car

    # Now delete everything, reload, and make sure the condition is still satisfied
    dict1_id = historian.save(lrdict)
    car_id = lrdict["car"].obj_id
    assert car_id is not None
    del lrdict, car

    rldict_loaded = historian.load(dict1_id)
    assert len(rldict_loaded) == 1
    assert isinstance(rldict_loaded["car"], testing.Car)
    # Check they are the same object
    assert rldict_loaded["car"].obj_id == car_id


def test_live_ref_lists(historian: mincepy.Historian):
    """Basic tests on live list. It's difficult to test the 'syncing' aspects
    of this container without another process"""
    live_list = builtins.LiveRefList()

    car = testing.Car()

    live_list.append(car)
    wrapper_id = live_list.save()
    car_id = car.obj_id
    assert car_id is not None
    del live_list

    live_list = historian.load(wrapper_id)
    assert len(live_list) == 1

    # Test getitem
    live_list.append(testing.Car("honda"))
    live_list.append(testing.Car("fiat"))
    assert live_list[2].make == "fiat"

    assert car.obj_id == car_id

    # Test delete
    del live_list[2]
    assert len(live_list) == 2


def test_file_from_disk(tmp_path, historian: mincepy.Historian):
    disk_file_path = tmp_path / "test.txt"
    disk_file_path.write_text("TEST")
    file = historian.create_file("test.txt")
    file.from_disk(disk_file_path)
    contents = file.read_text()
    assert contents == "TEST"


def test_file_to_disk(tmp_path, historian: mincepy.Historian):
    disk_file_path = tmp_path / "test.txt"

    file = historian.create_file("test.txt")
    file.write_text("TEST")

    file.to_disk(tmp_path)

    assert disk_file_path.read_text() == "TEST"

    # Now test writing to a custom file name
    custom_filename = tmp_path / "test_file.txt"
    file.to_disk(custom_filename)
    assert custom_filename.read_text() == "TEST"


def test_list_state(historian: mincepy.Historian):
    my_list = builtins.List([3, 1, 4])
    my_list.save()
    # Check that the state is indeed stored directly as a primitive list
    results = tuple(historian.find(state=[3, 1, 4]))
    assert len(results) == 1
    assert results[0] is my_list


def test_dict_state(historian: mincepy.Historian):
    my_dict = builtins.Dict({"hungry": "hippo"})
    my_dict.save()
    # Check that the state is indeed stored directly as a primitive list
    results = tuple(historian.find(state={"hungry": "hippo"}))
    assert len(results) == 1
    assert results[0] is my_dict


def test_str_state(historian: mincepy.Historian):
    my_str = builtins.Str("hungry hippo!")
    my_str.save()
    # Check that the state is indeed stored directly as a primitive list
    results = tuple(historian.find(state="hungry hippo!"))
    assert len(results) == 1
    assert results[0] is my_str


def test_ordered_dict(historian: mincepy.Historian):
    """Test that OrderedDictHelper works correctly"""
    entries = [("a", 5), ("b", "10"), ("c", testing.Car())]
    loaded = testing.do_round_trip(historian, collections.OrderedDict, entries)
    assert list(loaded.items()) == entries


def test_set(historian: mincepy.Historian):
    """Test that SetHelper works correctly"""
    entries = {5, "elephant", testing.Car()}
    loaded = testing.do_round_trip(historian, set, entries)
    assert loaded == entries


def test_path_helper(historian: mincepy.Historian):
    historian.register_type(mincepy.common_helpers.PathHelper())

    class File(mincepy.ConvenientSavable):
        TYPE_ID = uuid.UUID("8d645bb8-4657-455b-8b61-8613bc8a0acf")
        path = mincepy.field()

        def __init__(self, path):
            super().__init__()
            self.path = path

    path = pathlib.Path("some_path")
    loaded = testing.do_round_trip(historian, File, path)
    assert loaded.path == pathlib.Path("some_path")


def test_tuple_helper(historian: mincepy.Historian):
    historian.register_type(mincepy.common_helpers.TupleHelper())

    container = mincepy.builtins.List()
    container.append((testing.Car("ferrari"),))

    container_id = historian.save(container)
    del container
    loaded = historian.load(container_id)
    assert loaded[0][0] == testing.Car("ferrari")


def test_namespace_helper(historian: mincepy.Historian):
    historian.register_type(mincepy.common_helpers.NamespaceHelper())

    car = testing.Car("fiat", "500")
    dinner = argparse.Namespace(
        food="pizza",
        drink="wine",
        cost=10.94,
        car=car,
        host=testing.Person("Martin", 34),
    )
    dinner_id = historian.save(dinner)
    del dinner

    loaded = historian.load(dinner_id)
    assert loaded.car == car
    assert loaded.host == testing.Person("Martin", 34)
    assert loaded.food == "pizza"
    assert loaded.cost == 10.94

    loaded.guest = testing.Person("Sonia", 30)
    historian.save(loaded)
    del loaded

    reloaded = historian.load(dinner_id)
    assert reloaded.guest == testing.Person("Sonia", 30)
