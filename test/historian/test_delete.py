import pytest

import mincepy
from mincepy.testing import Car


def test_delete(historian: mincepy.Historian):
    """Test deleting and then attempting to load an object"""
    car = Car('lada')
    car_id = historian.save(car)
    historian.delete(car)
    with pytest.raises(mincepy.ObjectDeleted):
        historian.load(car_id)

    records = historian.history(car_id, as_objects=False)
    assert len(records) == 2, "There should be two record, the initial and the delete"
    assert records[-1].is_deleted_record()


def test_delete_from_obj_id(historian: mincepy.Historian):
    """Test deleting an object using it's object id"""
    car = Car('skoda')
    car_id = car.save()
    del car

    historian.delete(car_id)

    with pytest.raises(mincepy.ObjectDeleted):
        historian.load(car_id)


def test_delete_in_transaction(historian: mincepy.Historian):
    saved_outside = Car('fiat')
    outside_id = saved_outside.save()

    with historian.transaction():
        saved_inside = Car('bmw')
        inside_id = saved_inside.save()
        historian.delete(inside_id)
        historian.delete(outside_id)

        with pytest.raises(mincepy.NotFound):
            historian.get_obj(obj_id=inside_id)
        with pytest.raises(mincepy.NotFound):
            historian.get_obj(obj_id=outside_id)

        with pytest.raises(mincepy.ObjectDeleted):
            historian.load(inside_id)
        with pytest.raises(mincepy.ObjectDeleted):
            historian.load(outside_id)

    with pytest.raises(mincepy.NotFound):
        historian.get_obj(obj_id=inside_id)
    with pytest.raises(mincepy.NotFound):
        historian.get_obj(obj_id=outside_id)

    with pytest.raises(mincepy.ObjectDeleted):
        historian.load(inside_id)
    with pytest.raises(mincepy.ObjectDeleted):
        historian.load(outside_id)


def test_delete_find(historian: mincepy.Historian):
    car = Car('trabant')
    car_id = car.save()

    historian.delete(car_id)
    with pytest.raises(mincepy.ObjectDeleted):
        # Have to evaluate find because it returns a generator that won't actually
        # do anything unless we iterate it
        tuple(historian.find(obj_id=car_id))

    # Now check the archive
    assert len(tuple(historian.archive.find(obj_id=car_id))) == 1
    assert len(tuple(historian.archive.find(obj_id=car_id, deleted=False))) == 0
