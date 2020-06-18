""""Tests of migration"""
import gc

import mincepy
from mincepy import testing
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


def test_migrate_with_saved(historian: mincepy.Historian):
    """Test migrating an object that has saved references"""

    class V3(mincepy.SimpleSavable):
        ATTRS = (mincepy.AsRef('ref'), 'description')
        TYPE_ID = StoreByRef.TYPE_ID

        class Migration(mincepy.ObjectMigration):
            VERSION = 2
            PREVIOUS = StoreByRef.ToRefMigration

            @classmethod
            def upgrade(cls, saved_state, loader: 'mincepy.Loader'):
                saved_state['description'] = None
                return saved_state

        LATEST_MIGRATION = Migration

        def __init__(self, ref):
            super().__init__()
            self.ref = ref
            self.description = None

    obj = StoreByRef(testing.Car())
    obj_id = obj.save()
    del obj
    gc.collect()

    historian.register_type(V3)
    migrated = historian.migrations.migrate_all()
    assert len(migrated) == 1
    assert migrated[0].obj_id == obj_id

    obj = historian.load(obj_id)

    assert isinstance(obj, V3)
    assert hasattr(obj, 'description')
    assert obj.description is None
