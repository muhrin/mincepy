from abc import ABCMeta, abstractmethod
import datetime
from hashlib import blake2b
from typing import TYPE_CHECKING, List, Optional, Sequence, Type
import uuid

from . import depositors, expr, fields, saving, tracking

if TYPE_CHECKING:
    import mincepy

__all__ = "Savable", "Comparable", "Object", "SavableObject", "PRIMITIVE_TYPES"

# The primitives that all archive types must support
PRIMITIVE_TYPES = (
    bool,
    int,
    float,
    str,
    dict,
    list,
    type(None),
    bytes,
    uuid.UUID,
    datetime.datetime,
)


def is_primitive(obj):
    return obj.__class__ in PRIMITIVE_TYPES


class Savable(fields.WithFields, expr.FilterLike):
    """Interface for an object that can save and load its instance state"""

    TYPE_ID = None
    LATEST_MIGRATION: Optional["mincepy.ObjectMigration"] = None

    def __init__(self, *args, **kwargs):
        assert self.TYPE_ID is not None, "Must set the TYPE_ID for an object to be savable"
        super().__init__(*args, **kwargs)

    @classmethod
    def __expr__(cls):
        """This method gives savables the ability to be used as an expression"""
        return expr.Comparison("type_id", expr.Eq(cls.TYPE_ID))

    @classmethod
    def __query_expr__(cls) -> dict:  # pylint: disable=arguments-differ
        """This method gives savables the ability to be used in query filter expressions"""
        return cls.__expr__().__query_expr__()

    def save_instance_state(self, saver: depositors.Saver):  # pylint: disable=unused-argument
        """Save the instance state of an object, should return a saved instance"""
        return saving.save_instance_state(self)

    def load_instance_state(
        self, saved_state, loader: depositors.Loader
    ):  # pylint: disable=unused-argument
        """Take the given object and load the instance state into it"""
        saving.load_instance_state(self, saved_state)


class Comparable(metaclass=ABCMeta):
    """Interface for an object that can be compared and hashed"""

    @abstractmethod
    def __eq__(self, other) -> bool:
        """Determine if two objects are equal"""

    @abstractmethod
    def yield_hashables(self, hasher):
        """Produce a hash representing the value"""


class Object(Comparable, metaclass=ABCMeta):
    """A simple object that is comparable"""


class SavableObject(Object, Savable, metaclass=ABCMeta):
    """A class that is both savable and comparable"""

    _historian: "mincepy.Historian" = None

    @classmethod
    def init_field(cls, obj_field: fields.Field, attr_name: str):
        super().init_field(obj_field, attr_name)
        obj_field.set_query_context(expr.Comparison("type_id", expr.Eq(cls.TYPE_ID)))
        obj_field.path_prefix = "state"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        tracking.obj_created(self)

    def __eq__(self, other) -> bool:
        """Determine if two objects are equal"""
        if not isinstance(other, type(self)):
            return False

        return saving.save_instance_state(self) == saving.save_instance_state(other)

    def yield_hashables(self, hasher):
        """Produce a hash representing the object"""
        yield from hasher.yield_hashables(saving.save_instance_state(self))


class Equator:
    def __init__(self, equators: Sequence["mincepy.TypeHelper"] = tuple()):
        self._equators: List["mincepy.TypeHelper"] = []

        def do_hash(*args):
            hasher = blake2b(digest_size=32)
            for arg in args:
                hasher.update(arg)

            return hasher.hexdigest()

        self._hasher = do_hash

        # Initialise all the equators
        for equator in equators:
            self.add_equator(equator)

    def add_equator(self, equator: "mincepy.TypeHelper"):
        self._equators.append(equator)

    def remove_equator(self, equator: "mincepy.TypeHelper"):
        self._equators.reverse()
        try:
            self._equators.remove(equator)
        except ValueError as exc:
            raise ValueError(f"Unknown equator '{equator}'") from exc
        finally:
            self._equators.reverse()

    def get_equator(self, obj):
        # Iterate in reversed order i.e. the latest added should be used preferentially
        for equator in reversed(self._equators):
            try:
                if isinstance(obj, equator.TYPE):
                    return equator
            except TypeError as exc:
                raise RuntimeError(
                    f"There is a problem with equator '{type(equator).__name__}'"
                ) from exc
        raise TypeError(f"Don't know how to compare '{type(obj)}' types, no type equator set")

    def yield_hashables(self, obj):
        try:
            equator = self.get_equator(obj)
        except TypeError:
            # Try the objects' method
            try:
                yield from obj.yield_hashables(self)
            except AttributeError:
                raise TypeError(
                    f"No helper registered and no `yield_hashables()` method on "
                    f"'{type(obj).__name__}'"
                ) from None
        else:
            yield from equator.yield_hashables(obj, self)

    def hash(self, obj):
        return self._hasher(*self.yield_hashables(obj))

    def eq(self, obj1, obj2) -> bool:  # pylint: disable=invalid-name
        if not type(obj1) == type(obj2):  # pylint: disable=unidiomatic-typecheck # noqa: E721
            return False

        try:
            equator = self.get_equator(obj1)
        except TypeError:
            # Fallback to python eq
            return obj1 == obj2

        return equator.eq(obj1, obj2)

    def float_to_str(self, value, sig=14):
        """
        Convert float to text string for computing hash.
        Preserve up to N significant number given by sig.

        :param value: the float value to convert
        :param sig: choose how many digits after the comma should be output
        """
        fmt = f"{{:.{sig}g}}"
        return fmt.format(value)


def is_savable_type(obj_type: Type) -> bool:
    return issubclass(obj_type, SavableObject) and obj_type.TYPE_ID is not None


def savable_mro(obj_type: Type[SavableObject]) -> List[Type[SavableObject]]:
    """Given a SavableObject type this will give the mro of the savable types in the hierarchy"""
    mro = obj_type.mro()
    return list(filter(is_savable_type, mro))
