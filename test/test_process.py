import mincepy

from .common import Car


def test_basic_save_process(historian: mincepy.Historian):
    car = Car('nissan', 'white')

    proc = mincepy.Process('test_basic_save')
    pid = historian.save(proc)
    with proc.running():
        car_id = historian.save(car)
    assert historian.created_in(car) == pid

    second_car = Car('ford')
    historian.save(second_car)
    assert historian.created_in(second_car) == None

    # Now check we can get the creator id from the object id
    del car
    assert historian.created_in(car_id) == pid
