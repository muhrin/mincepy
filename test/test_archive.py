import mincepy
from mincepy.testing import Car


def test_meta_set_update_many(historian: mincepy.Historian):
    car1 = Car()
    car2 = Car()
    car1id, car2id = historian.save(car1, car2)
    historian.archive.meta_set_many({car1id: {'reg': 'car1'}, car2id: {'reg': 'car2'}})

    results = historian.archive.meta_get_many((car1id, car2id))
    assert results == {car1id: {'reg': 'car1'}, car2id: {'reg': 'car2'}}

    historian.archive.meta_update_many({car1id: {'colour': 'red'}, car2id: {'reg': 'car2updated'}})

    metas = historian.archive.meta_get_many((car1id, car2id))
    assert metas == {car1id: {'reg': 'car1', 'colour': 'red'}, car2id: {'reg': 'car2updated'}}


def test_meta_find(historian: mincepy.Historian):
    car1 = Car()
    car2 = Car()

    car1id, _ = historian.save(car1, car2)
    historian.archive.meta_set(car1id, {'reg': 'car1'})

    results = dict(historian.archive.meta_find({}, (car1id,)))
    assert results == {car1id: {'reg': 'car1'}}


def test_meta_update_many(historian: mincepy.Historian):
    car1 = Car()
    car2 = Car()
    car1id, car2id = historian.save(car1, car2)
    historian.archive.meta_set_many({car1id: {'reg': 'car1'}, car2id: {'reg': 'car2'}})

    results = historian.archive.meta_get_many((car1id, car2id))
    assert results[car1id] == {'reg': 'car1'}
    assert results[car2id] == {'reg': 'car2'}


def test_count(historian: mincepy.Historian):
    car1 = Car('ferrari')
    car2 = Car('skoda')
    car1id, car2id = historian.save(car1, car2)
    historian.archive.meta_set_many({car1id: {'reg': 'car1'}, car2id: {'reg': 'car2'}})

    assert historian.archive.count() == 2
    assert historian.archive.count(state=dict(make='ferrari')) == 1
    assert historian.archive.count(meta=dict(reg='car1')) == 1


def test_find_from_id(historian: mincepy.Historian):
    car = Car('ferrari')
    car_id = car.save()

    results = tuple(historian.archive.find(obj_id=car_id))
    assert len(results) == 1
    assert results[0].obj_id == car_id

    # Now check that we can pass an iterable of ids
    car2 = Car('skoda')
    car2_id = car2.save()
    results = tuple(historian.archive.find(obj_id=[car_id, car2_id]))
    assert len(results) == 2
    ids = [record.obj_id for record in results]
    assert car_id in ids
    assert car2_id in ids


def test_find_using_iterator(mongodb_archive: mincepy.Archive):
    """Test that passing an iterable to find types that support it, works."""
    record_details = dict(state=None, state_types=None, snapshot_hash=None)

    record1 = mincepy.DataRecord.new_builder(obj_id=123, type_id=1, **record_details).build()
    record2 = mincepy.DataRecord.new_builder(obj_id=456, type_id=2, **record_details).build()

    mongodb_archive.save_many([record1, record2])

    results = tuple(mongodb_archive.find(obj_id=[123].__iter__()))
    assert len(results) == 1

    results = tuple(mongodb_archive.find(type_id=[1].__iter__()))
    assert len(results) == 1
