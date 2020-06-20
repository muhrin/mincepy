import pytest

import mincepy
from mincepy import testing

# pylint: disable=invalid-name


def test_basic_save_process(historian: mincepy.Historian):
    proc = mincepy.Process('test_basic_save')
    pid = historian.save(proc)
    with proc.running():
        car = testing.Car('nissan', 'white')
        car_id = historian.save(car)
    assert historian.created_by(car) == pid

    second_car = testing.Car('ford')
    historian.save(second_car)
    assert historian.created_by(second_car) is None

    # Now check we can get the creator id from the object id
    del car
    assert historian.created_by(car_id) == pid


def test_save_after_creation(historian: mincepy.Historian):
    """
    Test saving an object that was created inside a process context but then saved
    outside it.  The creator should still be correctly set
    """
    proc = mincepy.Process('test_delayed_save')
    proc.save()
    with proc.running():
        # Create the car
        car = testing.Car('nissan', 'white')

    # Save it
    historian.save(car)
    created_in = historian.created_by(car)
    assert created_in is not None
    assert created_in == historian.get_current_record(proc).obj_id


def test_process_nested_running(historian: mincepy.Historian):
    proc = mincepy.Process('test_nested_exception')
    with proc.running():
        with proc.running():
            pass
        assert proc.is_running
    historian.save(proc)

    # Now check that nested exceptions are correctly handled
    with pytest.raises(TypeError):
        with proc.running():
            with pytest.raises(RuntimeError):
                with proc.running():
                    raise RuntimeError('Failed yo')
            assert proc.is_running
            raise TypeError("New error")
        assert proc.is_running
    proc_id = historian.save(proc)
    del proc

    loaded = historian.load(proc_id)
    assert not loaded.is_running


def test_saving_while_running(historian: mincepy.Historian):
    proc = mincepy.Process('test_nested_exception')
    with proc.running():
        historian.save(proc)


def test_saving_creator_that_owns_child(historian: mincepy.Historian):

    class TestProc(mincepy.Process):
        ATTRS = ('child',)

        def __init__(self):
            super().__init__('test_proc')
            self.child = None

    test_proc = TestProc()
    with test_proc.running():
        test_proc.child = mincepy.ObjRef(testing.Car())
        historian.save(test_proc)


def test_process_track(historian: mincepy.Historian):

    class TestProc(mincepy.Process):

        @mincepy.track
        def execute(self):  # pylint: disable=no-self-use
            return mincepy.builtins.RefList([testing.Car()])

    proc = TestProc('test process')
    proc.save()
    car_list = proc.execute()
    historian.save(car_list)
    proc_id = historian.get_obj_id(proc)

    assert proc_id is not None
    assert historian.get_current_record(car_list).created_by == proc_id
    assert historian.get_current_record(car_list[0]).created_by == proc_id
