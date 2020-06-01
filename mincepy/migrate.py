from typing import Iterator, Sequence, Iterable

import mincepy  # pylint: disable=unused-import
from . import depositors
from . import helpers
from . import qops
from . import records

__all__ = ('Migrations',)


def _state_types_migration_condition(helper: helpers.TypeHelper) -> dict:
    return qops.elem_match_(
        **{
            '1':
                helper.TYPE_ID,  # Type id has to match, and,
            **qops.or_(
                # version has to be less than the current, or,
                {'2': qops.lt_(helper.get_version())},
                # there is no version number
                {'2': None})
        })


class Migrations:
    """The historian migrations namespace"""

    def __init__(self, historian: 'mincepy.Historian'):
        self._historian = historian

    def find_migratable_records(self) -> Iterator[records.DataRecord]:
        """Find archive records that can be migrated"""
        type_registry = self._historian.type_registry
        # Find all the types in the registry that have migrations
        have_migrations = [
            helper for helper in type_registry.type_helpers.values()
            if helper.get_version() is not None
        ]

        if not have_migrations:
            return []

        # Now, let's look for those records that would need migrating
        archive = self._historian.archive
        query = qops.or_(*list(map(_state_types_migration_condition, have_migrations)))
        return archive.find(state_types=query)

    def migrate_all(self) -> Sequence[records.DataRecord]:
        """Migrate all records that can be updated"""
        to_migrate = self.find_migratable_records()
        return self.migrate_records(to_migrate)

    def migrate_records(self,
                        to_migrate: Iterable[records.DataRecord]) -> Sequence[records.DataRecord]:
        """Migrate the given records (if possible).  Returns all the records that were actually
        migrated."""
        migrator = depositors.Migrator(self._historian)
        return migrator.migrate_records(to_migrate)
