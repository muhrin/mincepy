from click.testing import CliRunner

import mincepy
import mincepy.cli.main
from ..common import CarV1, CarV2, StoreByValue, StoreByRef


def test_simple_migrate(historian: mincepy.Historian, archive_uri):
    car = CarV1('white', 'lada')
    car.save()
    by_val = StoreByValue(car)
    by_val_id = by_val.save()

    # Register a new version of the car
    historian.register_type(CarV2)

    # Now both car and by_val should need migration (because by_val stores a car)
    migratable = tuple(historian.migrations.find_migratable_records())
    assert len(migratable) == 2

    # Now migrate
    runner = CliRunner()
    result = runner.invoke(mincepy.cli.main.migrate, ["--yes", archive_uri],
                           obj={'helpers': [CarV2, StoreByValue]})
    assert result.exit_code == 0

    migratable = tuple(historian.migrations.find_migratable_records())
    assert len(migratable) == 0

    # Now register a new version of StoreByVal
    historian.register_type(StoreByRef)

    # There should still be the same to migratables as before
    migratable = tuple(historian.migrations.find_migratable_records())
    assert len(migratable) == 1
    ids = [record.obj_id for record in migratable]
    assert by_val_id in ids

    # Now migrate
    runner = CliRunner()
    result = runner.invoke(mincepy.cli.main.migrate, ["--yes", archive_uri],
                           obj={'helpers': [CarV2, StoreByRef]})
    assert result.exit_code == 0

    migratable = tuple(historian.migrations.find_migratable_records())
    assert len(migratable) == 0
