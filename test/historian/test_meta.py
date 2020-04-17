import pytest

import mincepy
from mincepy.testing import Car

# pylint: disable=invalid-name


def test_meta_unique_index(historian: mincepy.Historian):
    historian.meta.create_index('reg', True)
    car = Car()
    car.save(meta={'reg': 'VD495'})

    historian.archive.meta_find(dict(reg='VD495'))

    with pytest.raises(mincepy.DuplicateKeyError):
        Car().save(meta={'reg': 'VD495'})


def test_meta_index_unique_joint_index_where_exist(historian: mincepy.Historian):
    """Test a joint index that allows missing entries"""
    historian.meta.create_index([('reg', mincepy.ASCENDING), ('colour', mincepy.ASCENDING)],
                                unique=True,
                                where_exist=True)

    Car().save(meta=dict(reg='VD395', colour='red'))

    Car().save(meta=dict(colour='red'))
    Car().save(meta=dict(colour='red'))  # This should be ok

    Car().save(meta=dict(reg='VD395'))
    Car().save(meta=dict(reg='VD395'))  # This should be ok

    with pytest.raises(mincepy.DuplicateKeyError):
        Car().save(meta=dict(reg='VD395', colour='red'))


def test_meta_on_delete(historian: mincepy.Historian):
    """Test that metadata gets deleted when the object does"""
    car = Car()
    car_id = car.save(meta={'reg': '1234'})

    assert historian.meta.get(car_id) == {'reg': '1234'}
    historian.delete(car_id)
    assert historian.meta.get(car_id) is None
