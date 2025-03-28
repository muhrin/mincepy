from abc import ABCMeta
import logging
from typing import TYPE_CHECKING, Optional, Sequence, Union

import pytray.pretty

from . import depositors, exceptions, expr, fields, migrations, saving, tracking

if TYPE_CHECKING:
    import mincepy

__all__ = "TypeHelper", "WrapperHelper", "BaseHelper"

logger = logging.getLogger(__name__)


def inject_creation_tracking(cls: type):
    # Check to make sure we don't do this twice!
    if not hasattr(cls, "__orig_new"):
        cls.__orig_new = cls.__new__  # pylint: disable=protected-access

        def new(_cls, *_args, **_kwargs):
            inst = cls.__orig_new(_cls)  # pylint: disable=protected-access
            tracking.obj_created(inst)
            return inst

        cls.__new__ = new


def remove_creation_tracking(cls: type):
    try:
        cls.__new__ = cls.__orig_new  # pylint: disable=protected-access
    except AttributeError:
        pass


class TypeHelper(fields.WithFields):
    """This interface provides the basic methods necessary to enable a type to be compatible with
    the historian."""

    #: The type this helper corresponds to
    TYPE: Union[type, tuple[type]] = None
    TYPE_ID = None  # The unique id for this type of object
    IMMUTABLE = False  # If set to true then the object is decoded straight away
    INJECT_CREATION_TRACKING = False
    # The latest migration, if there is one
    LATEST_MIGRATION: "mincepy.ObjectMigration" = None

    @classmethod
    def init_field(cls, obj_field: fields.Field, attr_name: str):
        super().init_field(obj_field, attr_name)
        obj_field.set_query_context(expr.Comparison("type_id", expr.Eq(cls.TYPE_ID)))
        obj_field.path_prefix = "state"

    def __init__(self):  # pylint: disable=super-init-not-called
        if isinstance(self.TYPE, tuple):
            for entry in self.TYPE:  # pylint: disable=not-an-iterable
                if not isinstance(entry, type):
                    raise RuntimeError(
                        f"All entries of the TYPE must be `types`, got '{self.TYPE}'"
                    )
        elif not isinstance(self.TYPE, type):
            raise RuntimeError(
                f"Must set the TYPE to a type or a tuple of types, got '{self.TYPE}'"
            )

        if self.INJECT_CREATION_TRACKING:
            inject_creation_tracking(self.TYPE)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({repr(self.TYPE)})"

    def new(self, encoded_saved_state):  # pylint: disable=unused-argument
        """Create a new blank object of this type"""
        if isinstance(self.TYPE, tuple):
            obj_type = self.TYPE[0]  # pylint: disable=unsubscriptable-object
        else:
            obj_type = self.TYPE

        return obj_type.__new__(obj_type)

    def yield_hashables(self, obj: object, hasher):
        """Yield values from this object that should be included in its hash"""
        yield from hasher.yield_hashables(saving.save_instance_state(obj, type(self)))

    def eq(self, one, other) -> bool:  # pylint: disable=invalid-name
        """Determine if two objects are equal"""
        if not isinstance(  # pylint: disable=isinstance-second-argument-not-valid-type
            one, self.TYPE
        ) or not isinstance(  # pylint: disable=isinstance-second-argument-not-valid-type
            other, self.TYPE
        ):
            return False

        return saving.save_instance_state(one, type(self)) == saving.save_instance_state(
            other, type(self)
        )

    def save_instance_state(self, obj, saver, /):  # pylint: disable=unused-argument
        """Save the instance state of an object, should return a saved instance"""
        return saving.save_instance_state(obj, type(self))

    def load_instance_state(
        # pylint: disable=unused-argument
        self,
        obj,
        saved_state,
        loader: depositors.Loader,
        /,
    ):
        """Take the given blank object and load the instance state into it"""
        saving.load_instance_state(obj, saved_state, type(self))

    def get_version(self) -> Optional[int]:
        """Gets the version of the latest migration, returns `None` if there is not a migration"""
        if self.LATEST_MIGRATION is None:
            return None

        version = self.LATEST_MIGRATION.VERSION
        if version is None:
            raise RuntimeError(
                f"Object '{self.TYPE}' has a migration ({self.LATEST_MIGRATION}) which has no "
                f"version number"
            )

        return version

    def ensure_up_to_date(self, saved_state, version: Optional[int], loader: depositors.Loader):
        """Apply any migrations that are necessary to this saved state.  If no migrations are
        necessary then None is returned"""
        latest_version = None if self.LATEST_MIGRATION is None else self.LATEST_MIGRATION.VERSION
        if latest_version == version:
            return None

        if latest_version is None or (version is not None and latest_version < version):
            raise exceptions.VersionError(
                f"This codebase's version of '{pytray.pretty.type_string(self.TYPE)}' is older "
                f"({latest_version}) than the saved version ({version}).  Check for updates."
            )

        to_apply = self._get_migrations(version)
        if not to_apply:
            return None

        total = len(to_apply)
        logger.info(
            "Migrating saved state of '%s' from version %s to %i (%i migrations to apply)",
            pytray.pretty.type_string(self.TYPE),
            version,
            self.get_version(),
            total,
        )
        for i, migration in enumerate(to_apply):
            saved_state = migration.upgrade(saved_state, loader)
            logger.info(
                "Migration '%s' applied (%i/%i)",
                pytray.pretty.type_string(migration),
                i + 1,
                total,
            )

        logger.info(
            "Migration of '%s' completed successfully",
            pytray.pretty.type_string(self.TYPE),
        )

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
    standard and therefore most type helpers will want to subclass from this class."""

    def yield_hashables(self, obj, hasher):
        yield from hasher.yield_hashables(obj)

    def eq(self, one, other) -> bool:
        return one == other


class WrapperHelper(TypeHelper):
    """Wraps up an object type to perform the necessary Historian actions"""

    # pylint: disable=invalid-name

    def __init__(self, obj_type: type["mincepy.SavableObject"]):
        self.TYPE = obj_type
        self.TYPE_ID = obj_type.TYPE_ID
        self.LATEST_MIGRATION = obj_type.LATEST_MIGRATION
        super().__init__()

    def __repr__(self) -> str:
        return f"WrapperHelper({repr(self.TYPE)})"

    def yield_hashables(self, obj, hasher):
        yield from self.TYPE.yield_hashables(obj, hasher)

    def eq(self, one, other) -> bool:
        return self.TYPE.__eq__(one, other)  # pylint: disable=unnecessary-dunder-call

    def save_instance_state(self, obj: "mincepy.Savable", saver, /):
        return self.TYPE.save_instance_state(obj, saver)

    def load_instance_state(self, obj, saved_state: "mincepy.Savable", loader, /):
        self.TYPE.load_instance_state(obj, saved_state, loader)
