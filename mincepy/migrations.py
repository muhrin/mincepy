# -*- coding: utf-8 -*-
from typing import Any, Optional

import pytray.pretty

from . import depositors  # pylint: disable=unused-import

__all__ = "ObjectMigration", "ObjectMigrationMeta"


class ObjectMigrationMeta(type):
    PARENT = None

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        if cls.PARENT is None or cls == cls.PARENT:
            # This one is the parent class for others and doesn't need to pass the checks
            return

        if cls.VERSION is None:
            raise RuntimeError("Migration version not set")

        if cls.PREVIOUS is not None and cls.VERSION <= cls.PREVIOUS.VERSION:
            raise RuntimeError(
                f"A migration must have a version number higher than the previous migration.  "
                f"{pytray.pretty.type_string(cls.PREVIOUS)}.VERSION is {cls.PREVIOUS.VERSION} while "
                f"{pytray.pretty.type_string(cls)}.VERSION is {cls.VERSION}"
            )

        if cls.NAME is None:
            cls.NAME = cls.__class__.__name__


class ObjectMigration(metaclass=ObjectMigrationMeta):
    NAME = None  # type: Optional[str]
    VERSION = None  # type: Optional[int]
    PREVIOUS = None  # type: Optional[ObjectMigration]

    @classmethod
    def upgrade(cls, saved_state, loader: depositors.Loader) -> Any:
        """
        This method should take the saved state, which will have been created with the previous
        version, and return a new saved state that is compatible with this version.

        :raises mincepy.MigrationError: if a problem is encountered during migration
        """
        raise NotImplementedError


ObjectMigrationMeta.PARENT = ObjectMigration
