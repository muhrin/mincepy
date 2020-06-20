from abc import ABCMeta, abstractmethod
import logging
from typing import Type, Optional, Sequence

import pytray.pretty

import mincepy  # pylint: disable=unused-import
from . import exceptions
from . import migrations
from . import tracking
from . import types

__all__ = 'TypeHelper', 'WrapperHelper', 'BaseHelper'

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def inject_creation_tracking(cls: Type):
    # Check to make sure we don't do this twice!
    if not hasattr(cls, '__orig_new'):
        cls.__orig_new = cls.__new__  # pylint: disable=protected-access

        def new(_cls, *_args, **_kwargs):
            inst = cls.__orig_new(_cls)  # pylint: disable=protected-access
            tracking.obj_created(inst)
            return inst

        cls.__new__ = new


def remove_creation_tracking(cls: Type):
    try:
        cls.__new__ = cls.__orig_new  # pylint: disable=protected-access
    except AttributeError:
        pass


class TypeHelper(metaclass=ABCMeta):
    """This interface provides the basic methods necessary to enable a type to be compatible with
    the historian."""
    TYPE = None  # The type this helper corresponds to
    TYPE_ID = None  # The unique id for this type of object
    IMMUTABLE = False  # If set to true then the object is decoded straight away
    INJECT_CREATION_TRACKING = False
    # The latest migration, if there is one
    LATEST_MIGRATION = None  # type: migrations.ObjectMigration

    def __init__(self):
        assert self.TYPE is not None, "Must set the TYPE to a type of or a tuple of types"
        if self.INJECT_CREATION_TRACKING:
            inject_creation_tracking(self.TYPE)

    def new(self, encoded_saved_state):  # pylint: disable=unused-argument
        """Create a new blank object of this type"""
        cls = self.TYPE
        return cls.__new__(cls)

    @abstractmethod
    def yield_hashables(self, obj, hasher):
        """Produce a hash representing the value"""

    @abstractmethod
    def eq(self, one, other) -> bool:  # pylint: disable=invalid-name
        """Determine if two objects are equal"""

    @abstractmethod
    def save_instance_state(self, obj, saver):
        """Save the instance state of an object, should return a saved instance"""

    @abstractmethod
    def load_instance_state(self, obj, saved_state, loader: 'mincepy.Loader'):
        """Take the given blank object and load the instance state into it"""

    def get_version(self) -> Optional[int]:
        """Gets the version of the latest migration, returns None if there is not migration"""
        if self.LATEST_MIGRATION is None:
            return None

        version = self.LATEST_MIGRATION.VERSION
        if version is None:
            raise RuntimeError("Object '{}' has a migration ({}) which has no version "
                               "number".format(self.TYPE, self.LATEST_MIGRATION))

        return version

    def ensure_up_to_date(self, saved_state, version: Optional[int], loader: 'mincepy.Loader'):
        """Apply any migrations that are necessary to this saved state.  If no migrations are
        necessary then None is returned"""
        latest_version = None if self.LATEST_MIGRATION is None else self.LATEST_MIGRATION.VERSION
        if latest_version == version:
            return None

        if latest_version is None or (version is not None and latest_version < version):
            raise exceptions.VersionError(
                "This codebase's version of '{}' is older ({}) than the saved version ({}).  Check "
                "for updates.".format(pytray.pretty.type_string(self.TYPE), latest_version,
                                      version))

        to_apply = self._get_migrations(version)
        if not to_apply:
            return None

        total = len(to_apply)
        logger.info("Migrating saved state of '%s' from version %s to %i (%i migrations to apply)",
                    pytray.pretty.type_string(self.TYPE), version, self.get_version(), total)
        for i, migration in enumerate(to_apply):
            saved_state = migration.upgrade(saved_state, loader)
            logger.info("Migration '%s' applied (%i/%i)", pytray.pretty.type_string(migration),
                        i + 1, total)

        logger.info("Migration of '%s' completed successfully",
                    pytray.pretty.type_string(self.TYPE))

        return saved_state

    def _get_migrations(self, version: Optional[int]) -> Sequence[migrations.ObjectMigration]:
        """Get the sequence of migrations that needs to be applied to a given version"""
        if self.LATEST_MIGRATION is None:
            return []  # No migrations we can apply

        to_apply = []
        current = self.LATEST_MIGRATION
        while version is None or version < current.VERSION:
            to_apply.append(current)
            current = current.PREVIOUS
            if current is None:
                break

        to_apply.reverse()
        return to_apply


class BaseHelper(TypeHelper, metaclass=ABCMeta):
    """A base helper that defaults to yielding hashables directly on the object
    and testing for equality using == given two objects.  This behaviour is fairly
    standard and therefor more type helpers will want to subclass from this class."""

    # pylint: disable=abstract-method

    def yield_hashables(self, obj, hasher):
        yield from hasher.yield_hashables(obj)

    def eq(self, one, other) -> bool:
        return one == other


class WrapperHelper(TypeHelper):
    """Wraps up an object type to perform the necessary Historian actions"""

    # pylint: disable=invalid-name

    def __init__(self, obj_type: Type[types.SavableObject]):
        self.TYPE = obj_type
        self.TYPE_ID = obj_type.TYPE_ID
        self.LATEST_MIGRATION = obj_type.LATEST_MIGRATION
        super(WrapperHelper, self).__init__()

    def yield_hashables(self, obj, hasher):
        yield from self.TYPE.yield_hashables(obj, hasher)

    def eq(self, one, other) -> bool:
        return self.TYPE.__eq__(one, other)

    def save_instance_state(self, obj: types.Savable, saver):
        return self.TYPE.save_instance_state(obj, saver)

    def load_instance_state(self, obj, saved_state: types.Savable, loader):
        self.TYPE.load_instance_state(obj, saved_state, loader)
