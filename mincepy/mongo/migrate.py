import abc
from typing import List, Type

import pymongo.database

from . import settings

VERSION = 'version'
MIGRATIONS = 'migrations'


class MigrationError(RuntimeError):
    """An error that occurred during migration"""


class Migration(metaclass=abc.ABCMeta):
    """Migration class to be used when making changes to the database.  The implementer should
    provide a version number that higher than the previous as well as a reference to the previous
    migration.

    Migrations should avoid using constants from the codebase as these could change.  Ideally each
    migration should be self-contained and not refer to any other parts of the code.
    """

    # pylint: disable=invalid-name
    NAME = None
    VERSION = None
    PREVIOUS = None

    def __init__(self):
        if self.NAME is None:
            self.NAME = self.__class__.__name__

        if self.VERSION is None:
            raise RuntimeError("Migration version not set")

    @abc.abstractmethod
    def upgrade(self, database: pymongo.database.Database):
        """Apply the transformations to bring the database up to this version"""
        current = settings.get_settings(database) or {}
        # Update the version
        current[VERSION] = self.VERSION
        # and migrations
        current.setdefault(MIGRATIONS, []).append(self.NAME)
        settings.set_settings(database, current)


class MigrationManager:

    def __init__(self, latest: Type[Migration]):
        self.latest = latest

    def migration_required(self, database: pymongo.database.Database) -> bool:
        return len(self._get_required_migrations(database)) > 0

    def migrate(self, database: pymongo.database.Database) -> int:
        """Migrate the database and returns the number of migrations applied"""
        migrations = self._get_required_migrations(database)

        if migrations:
            for migration in reversed(migrations):
                migration().upgrade(database)

            # Recheck the settings to make sure the migration has been applied
            current = settings.get_settings(database)
            if current[MIGRATIONS][-1] != migrations[0].NAME:
                raise RuntimeError(
                    "After applying migrations, latest migration doesn't seem to be applied")

        return len(migrations)

    def get_migration_sequence(self) -> List[Type[Migration]]:
        """Get the sequence of migrations in REVERSE order i.e. the 0th entry is the latest"""
        # Establish the sequence of migrations
        migrations = [self.latest]
        while migrations[-1].PREVIOUS is not None:
            migrations.append(migrations[-1].PREVIOUS)

        return migrations

    def _get_required_migrations(self,
                                 database: pymongo.database.Database) -> List[Type[Migration]]:
        current = settings.get_settings(database) or {}
        migrations = self.get_migration_sequence()
        # Find out where we're at
        applied = current.get(MIGRATIONS, [])

        if applied:
            # Find the last one that was applied
            while migrations and applied[-1] != migrations.pop().NAME:
                pass

        # Anything left in the migrations list needs to be applied
        return migrations


def ensure_up_to_date(database: pymongo.database.Database, latest: Type[Migration]):
    """Apply any necessary migrations to the database"""
    current = settings.get_settings(database) or {}
    current_version = current.get(VERSION, None)
    if current_version and current_version > latest.VERSION:
        raise MigrationError(
            "The current database version ({}) is higher than the code version ({}) you may need "
            "to update your version of the code".format(current_version, latest.VERSION))

    migrator = MigrationManager(latest)
    return migrator.migrate(database)
