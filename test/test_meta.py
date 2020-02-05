""""Tests of metadata storage"""
import pytest

import mincepy
from mincepy.testing import Car


def test_metadata_simple(historian: mincepy.Historian):
    car = Car()
    ferrari_id = historian.save(car, with_meta={'reg': 'VD395'})
    # Check that we get back what we just set
    assert historian.get_meta(ferrari_id) == {'reg': 'VD395'}

    car.make = 'fiat'
    red_fiat_id = historian.save(car)
    # Check that the metadata is shared
    assert historian.get_meta(red_fiat_id) == {'reg': 'VD395'}

    historian.set_meta(ferrari_id, {'reg': 'N317'})
    # Check that this saves the metadata on the object level i.e. both are changed
    assert historian.get_meta(ferrari_id) == {'reg': 'N317'}
    assert historian.get_meta(red_fiat_id) == {'reg': 'N317'}


def test_metadata_using_object_instance(historian: mincepy.Historian):
    car = Car()
    historian.save(car, with_meta={'reg': 'VD395'})
    # Check that we get back what we just set
    assert historian.get_meta(car) == {'reg': 'VD395'}

    car.make = 'fiat'
    historian.save(car)
    # Check that the metadata is shared
    assert historian.get_meta(car) == {'reg': 'VD395'}

    historian.set_meta(car, {'reg': 'N317'})
    assert historian.get_meta(car) == {'reg': 'N317'}


def test_metadata_multiple(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    zonda = Car('zonda', 'yellow')

    historian.save(honda, zonda, with_meta=({'reg': 'H123'}, {'reg': 'Z456'}))

    assert historian.get_meta(honda) == {'reg': 'H123'}
    assert historian.get_meta(zonda) == {'reg': 'Z456'}


def test_metadata_wrong_type(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    with pytest.raises(TypeError):
        historian.save(honda, with_meta=['a', 'b'])


def test_metadata_update(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    historian.save(honda, with_meta={'reg': 'H123', 'vin': 1234, 'owner': 'Mike'})
    historian.update_meta(honda, {'vin': 5678, 'owner': 'Mart'})
    assert historian.get_meta(honda) == {'reg': 'H123', 'vin': 5678, 'owner': 'Mart'}


def test_metadata_update_inexistant(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    historian.save(honda)
    # If the data doesn't exist in the metadata already we expect an update to simply insert
    historian.update_meta(honda, {'reg': 'H123', 'vin': 1234})
    assert historian.get_meta(honda) == {'reg': 'H123', 'vin': 1234}


def test_metadata_find(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    honda2 = Car('honda', 'white')
    historian.save(honda, with_meta={'reg': 'H123', 'vin': 1234})
    historian.save(honda2)

    results = list(historian.find(Car, meta={'reg': 'H123'}))
    assert len(results) == 1
