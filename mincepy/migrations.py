from typing import Any, Optional

import mincepy  # pylint: disable=unused-import

__all__ = ('ObjectMigration',)


class ObjectMigrationMeta(type):

    def __call__(cls, *args, **kwargs):
        if cls.VERSION is None:
            raise RuntimeError("Migration version not set")

        if cls.PREVIOUS is not None and cls.VERSION <= cls.PREVIOUS.VERSION:
            raise RuntimeError(
                "A migration must have a version number higher than the previous migration.  Our"
                "version is {} while the migration is {}".format(cls.VERSION, cls.PREVIOUS.VERSION))

        if cls.NAME is None:
            cls.NAME = cls.__class__.__name__

        return super().__call__(cls, *args, **kwargs)


class ObjectMigration(metaclass=ObjectMigrationMeta):
    NAME = None  # type: Optional[str]
    VERSION = None  # type: Optional[int]
    PREVIOUS = None  # type: Optional[ObjectMigration]

    @classmethod
    def upgrade(cls, saved_state, loader: 'mincepy.Loader') -> Any:
        """
        This method should take the saved state, which will have been created with the previous
        version, and return a new saved state that is compatible with this version.

        :raises mincepy.MigrationError: if a problem is encountered during migration
        """
        raise NotImplementedError
