import pytest

import mincepy
from mincepy import _autosave, testing


def test_autosave_helper():
    helper = _autosave.autosavable(testing.Car)
    assert isinstance(helper, mincepy.TypeHelper)
    assert helper.TYPE is testing.Car


def test_autosave_historian(historian):
    with pytest.raises(ValueError):
        historian.type_registry.get_helper(testing.Sphere)

    new_obj = testing.Sphere(3.14)
    obj_id = historian.save(new_obj)
    helper = historian.type_registry.get_helper(testing.Sphere)
    assert isinstance(helper, mincepy.TypeHelper)
    type_id = helper.TYPE_ID

    del new_obj, helper

    historian.type_registry.unregister_type(type_id)
    with pytest.raises(ValueError):
        historian.type_registry.get_helper(testing.Sphere)

    loaded = historian.load(obj_id)
    assert isinstance(loaded, testing.Sphere)
    assert loaded.radius == 3.14
