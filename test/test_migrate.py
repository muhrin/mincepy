""""Tests of migration"""
import mincepy
import mincepy.testing
from .common import CarV1, CarV2, StoreByValue, StoreByRef


def test_find_migratable(historian: mincepy.Historian):
    car = CarV1('white', 'lada')
    car_id = car.save()
    by_val = StoreByValue(car)
    by_val_id = by_val.save()

    # Register a new version of the car
    historian.register_type(CarV2)

    # Now both car and by_val should need migration (because by_val stores a car)
    migratable = tuple(historian.migrations.find_migratable_records())
    assert len(migratable) == 2
    ids = [record.obj_id for record in migratable]
    assert car_id in ids
    assert by_val_id in ids

    # Now register a new version of StoreByVal
    historian.register_type(StoreByRef)

    # There should still be the same to migratables as before
    migratable = tuple(historian.migrations.find_migratable_records())
    assert len(migratable) == 2
    ids = [record.obj_id for record in migratable]
    assert car_id in ids
    assert by_val_id in ids
