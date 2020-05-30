from typing import Iterator

from . import historians
from . import helpers
from . import qops
from . import records


def find_migratable_objects(historian: historians.Historian) -> Iterator[records.DataRecord]:
    type_registry = historian.type_registry
    # Find all the types in the registry that have migrations
    have_migrations = [
        helper for helper in type_registry.type_helpers.values() if helper.get_version() is not None
    ]

    if not have_migrations:
        return []

    # Now, let's look for those records that would need migrating
    archive = historian.archive
    query = qops.or_(*list(map(_state_types_migration_condition, have_migrations)))
    return archive.find(state_types=query)


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
