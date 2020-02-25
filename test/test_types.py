import mincepy

from mincepy.testing import Car


def test_savable_object(historian: mincepy.Historian):
    """Test basic properties and functions of a savable object"""
    car = Car('smart', 'black')
    car_id = car.save()
    car.set_meta({'reg': 'VD395'})
    del car

    loaded = historian.load(car_id)  # type: Car
    assert loaded.make == 'smart'
    assert loaded.colour == 'black'
    assert loaded.get_meta() == {'reg': 'VD395'}

    loaded.update_meta({'driver': 'martin'})
    assert loaded.get_meta() == {'reg': 'VD395', 'driver': 'martin'}

    assert loaded.obj_id == car_id

    loaded.make = 'honda'
    loaded.save()
    del loaded

    reloaded = historian.load(car_id)
    assert reloaded.make == 'honda'
