import mincepy

from mincepy.testing import Car


def test_basic_save_process(historian: mincepy.Historian):
    car = Car('nissan', 'white')

    proc = mincepy.Process('test_basic_save')
    pid = historian.save(proc)
    with proc.running():
        car_id = historian.save(car)
    assert historian.created_in(car) == pid

    second_car = Car('ford')
    historian.save(second_car)
    assert historian.created_in(second_car) is None

    # Now check we can get the creator id from the object id
    del car
    assert historian.created_in(car_id) == pid


def test_save_after_creation(historian: mincepy.Historian):
    """
    Test saving an object that was created inside a process context but then saved
    outside it.  The creator should still be correctly set
    """
    proc = mincepy.Process('test_delayed_save')
    with proc.running():
        # Create the car
        car = Car('nissan', 'white')

    # Save it
    historian.save(car)
    created_in = historian.created_in(car)
    assert created_in is not None
    assert created_in == historian.get_current_record(proc).obj_id
