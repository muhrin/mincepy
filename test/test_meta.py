""""Tests of metadata storage"""
import uuid

import pytest

import mincepy
from mincepy.testing import Car

# pylint: disable=invalid-name


def test_metadata_simple(historian: mincepy.Historian):
    car = Car()
    ferrari_id = historian.save((car, {'reg': 'VD395'}))
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
    historian.save((car, {'reg': 'VD395'}))
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

    historian.save((honda, {'reg': 'H123'}), (zonda, {'reg': 'Z456'}))

    assert historian.meta.get(honda) == {'reg': 'H123'}
    assert historian.meta.get(zonda) == {'reg': 'Z456'}


def test_metadata_wrong_type(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    with pytest.raises(TypeError):
        historian.save_one(honda, meta=['a', 'b'])


def test_metadata_update(historian: mincepy.Historian):
    honda = Car('honda', 'white')
    historian.save_one(honda, meta=dict(reg='H123', vin=1234, owner='Mike'))
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
    historian.save_one(honda, meta={'reg': 'H123', 'vin': 1234})
    historian.save(honda2)

    results = list(historian.find(Car, meta={'reg': 'H123'}))
    assert len(results) == 1


def test_stick_meta(historian: mincepy.Historian):
    car1 = Car()
    historian.meta.sticky['owner'] = 'martin'
    car2 = Car()
    car3 = Car()

    car1_id = car1.save()
    car2_id = car2.save(meta={'for_sale': True})
    car3_id = car3.save(meta={'owner': 'james'})
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
    car0_id = garage[0].save(meta={'for_sale': True})
    car1_id = garage[1].save(meta={'owner': 'james'})
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
            historian.meta.set(car1.obj_id, {'spurious': True})
            assert historian.meta.get(car1.obj_id) == {'spurious': True}
            trans.rollback()
        assert not historian.meta.get(car1)
    assert not historian.meta.get(car1)


def test_metadata_find_object_regex(historian: mincepy.Historian):
    car1 = Car('honda', 'white')
    car2 = Car('honda', 'white')
    car3 = Car('honda', 'white')

    car1.save(meta={'reg': 'VD395'})
    car2.save(meta={'reg': 'VD574'})
    car3.save(meta={'reg': 'BE368'})

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

    car1.save(meta={'reg': 'VD395'})
    car2.save(meta={'reg': 'VD574'})
    car3.save(meta={'reg': 'BE368'})

    results = dict(historian.meta.find(filter=dict(reg={'$regex': '^VD'})))
    assert len(results) == 2
    ids = results.keys()
    assert car1.obj_id in ids
    assert car2.obj_id in ids


def test_meta_delete(historian: mincepy.Historian):
    """Test that when an object is delete so is its metadata"""
    car = Car()
    car_id = car.save(meta={'reg': '1234'})
    results = dict(historian.meta.get(car))
    assert results == {'reg': '1234'}

    historian.delete(car)

    assert historian.meta.get(car_id) is None

    results = tuple(historian.meta.find({}, obj_id=car_id))
    assert len(results) == 0


def test_set_meta_in_save(historian: mincepy.Historian):

    class Info(mincepy.BaseSavableObject):
        TYPE_ID = uuid.UUID('6744689d-5f88-482e-bb42-2bec5f139cc2')

        def save_instance_state(self, saver: mincepy.Saver) -> dict:
            state = super().save_instance_state(saver)
            saver.get_historian().meta.set(self, dict(msg='good news'))
            return state

    info = Info()
    historian.save(info)

    meta = historian.meta.get(info)
    assert meta == {'msg': 'good news'}


def test_set_meta_in_save_simple(historian: mincepy.Historian):

    class Info(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('6744689d-5f88-482e-bb42-2bec5f139cc2')

        def save_instance_state(self, saver: mincepy.Saver) -> dict:
            state = super().save_instance_state(saver)
            self.set_meta(dict(msg='good news'))
            return state

    info = Info()
    info.save()

    meta = historian.meta.get(info)
    assert meta == {'msg': 'good news'}


def test_set_meta_in_save_fail(historian: mincepy.Historian):
    """Make sure that metadata isn't saved if saving of the object fails"""

    class Info(mincepy.SimpleSavable):
        TYPE_ID = uuid.UUID('6744689d-5f88-482e-bb42-2bec5f139cc2')

        def save_instance_state(self, saver: mincepy.Saver) -> dict:
            super().save_instance_state(saver)
            self.set_meta(dict(msg='good news'))
            raise RuntimeError("I'm crashin' yo")

    info = Info()
    with pytest.raises(RuntimeError):
        info.save()

    with pytest.raises(mincepy.NotFound):
        historian.meta.get(info)
