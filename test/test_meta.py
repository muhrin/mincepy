""""Tests of metadata storage"""
import pytest

import mincepy
from mincepy.testing import Car

# pylint: disable=invalid-name


def test_metadata_simple(historian: mincepy.Historian):
    car = Car()
    ferrari_id = historian.save(car, with_meta={'reg': 'VD395'})
    # Check that we get back what we just set
    assert historian.meta.get(ferrari_id) == {'reg': 'VD395'}

    car.make = 'fiat'
    red_fiat_id = historian.save(car)
    # Check that the metadata is shared
    assert historian.meta.get(red_fiat_id) == {'reg': 'VD395'}

    historian.meta.set(ferrari_id, {'reg': 'N317'})
    # Check that this saves the metadata on the object level i.e. both are changed
    assert historian.meta.get(ferrari_id) == {'reg': 'N317'}
    assert historian.meta.get(red_fiat_id) == {'reg': 'N317'}


def test_metadata_using_object_instance(historian: mincepy.Historian):
    car = Car()
    historian.save(car, with_meta={'reg': 'VD395'})
    # Check that we get back what we just set
    assert historian.meta.get(car) == {'reg': 'VD395'}

    car.make = 'fiat'
    historian.save(car)
    # Check that the metadata is shared
    assert historian.meta.get(car) == {'reg': 'VD395'}

    historian.meta.set(car, {'reg': 'N317'})
    assert historian.meta.get(car) == {'reg': 'N317'}


def test_metadata_multiple(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    zonda = Car('zonda', 'yellow')

    historian.save(honda, zonda, with_meta=({'reg': 'H123'}, {'reg': 'Z456'}))

    assert historian.meta.get(honda) == {'reg': 'H123'}
    assert historian.meta.get(zonda) == {'reg': 'Z456'}


def test_metadata_wrong_type(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    with pytest.raises(TypeError):
        historian.save(honda, with_meta=['a', 'b'])


def test_metadata_update(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    historian.save(honda, with_meta={'reg': 'H123', 'vin': 1234, 'owner': 'Mike'})
    historian.meta.update(honda, {'vin': 5678, 'owner': 'Mart'})
    assert historian.meta.get(honda) == {'reg': 'H123', 'vin': 5678, 'owner': 'Mart'}


def test_metadata_update_inexistant(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    historian.save(honda)
    # If the data doesn't exist in the metadata already we expect an update to simply insert
    historian.meta.update(honda, {'reg': 'H123', 'vin': 1234})
    assert historian.meta.get(honda) == {'reg': 'H123', 'vin': 1234}


def test_metadata_find_objects(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    honda2 = Car('honda', 'white')
    historian.save(honda, with_meta={'reg': 'H123', 'vin': 1234})
    historian.save(honda2)

    results = list(historian.find(Car, meta={'reg': 'H123'}))
    assert len(results) == 1


def test_stick_meta(historian: mincepy.Historian):
    car1 = Car()
    historian.meta.sticky['owner'] = 'martin'
    car2 = Car()
    car3 = Car()

    car1_id = car1.save()
    car2_id = car2.save(with_meta={'for_sale': True})
    car3_id = car3.save(with_meta={'owner': 'james'})
    del car1, car2, car3

    assert historian.meta.get(car1_id) == {'owner': 'martin'}
    assert historian.meta.get(car2_id) == {'owner': 'martin', 'for_sale': True}
    assert historian.meta.get(car3_id) == {'owner': 'james'}


def test_meta_sticky_children(historian: mincepy.Historian):
    """Catch bug where metadata was not being set on being references by other objects"""
    garage = mincepy.RefList()
    garage.append(Car())
    garage.append(Car())
    historian.meta.sticky['owner'] = 'martin'

    garage_id = garage.save()
    car0_id = garage[0].save(with_meta={'for_sale': True})
    car1_id = garage[1].save(with_meta={'owner': 'james'})
    del garage

    assert historian.meta.get(garage_id) == {'owner': 'martin'}
    assert historian.meta.get(car0_id) == {'owner': 'martin', 'for_sale': True}
    assert historian.meta.get(car1_id) == {'owner': 'james'}


def test_meta_transaction(historian: mincepy.Historian):
    """Check that metadata respects transaction boundaries"""
    car1 = Car()

    with historian.transaction():
        car1.save()
        with historian.transaction() as trans:
            historian.set_meta(car1.obj_id, {'spurious': True})
            assert historian.get_meta(car1.obj_id) == {'spurious': True}
            trans.rollback()
        assert not historian.get_meta(car1)
    assert not historian.get_meta(car1)


def test_metadata_find_object_regex(historian: mincepy.Historian):
    car1 = Car('honda', 'white')
    car2 = Car('honda', 'white')
    car3 = Car('honda', 'white')

    car1.save(with_meta={'reg': 'VD395'})
    car2.save(with_meta={'reg': 'VD574'})
    car3.save(with_meta={'reg': 'BE368'})

    # Find all cars with a reg starting in VD
    results = tuple(historian.find(Car, meta={'reg': {'$regex': '^VD'}}))
    assert len(results) == 2
    assert results[0] in [car1, car2]
    assert results[1] in [car1, car2]


def test_metadata_find(historian: mincepy.Historian):
    """Test querying for metadata directly"""
    car1 = Car('honda', 'white')
    car2 = Car('honda', 'white')
    car3 = Car('honda', 'white')

    car1.save(with_meta={'reg': 'VD395'})
    car2.save(with_meta={'reg': 'VD574'})
    car3.save(with_meta={'reg': 'BE368'})

    results = tuple(historian.meta.efind(reg={'$regex': '^VD'}))
    assert len(results) == 2
    ids = (meta['obj_id'] for meta in results)
    assert car1.obj_id in ids
    assert car2.obj_id in ids
