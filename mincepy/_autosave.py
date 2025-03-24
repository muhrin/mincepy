from typing import TYPE_CHECKING, Union

from pytray import obj_load
from typing_extensions import override

from . import helpers

if TYPE_CHECKING:
    import mincepy

TYPE_ID_PREFIX = "autosave"

State = Union[dict, tuple]


def _create_helper(obj_type: type, obj_path: str) -> "mincepy.TypeHelper":
    """Create a type helper that uses the object path as the type id"""

    class AutoSavable(helpers.TypeHelper):
        TYPE = obj_type
        TYPE_ID = f"{TYPE_ID_PREFIX}:{obj_path}"

        @override
        def save_instance_state(self, obj, /, *_) -> State:
            return _get_state(obj)

        @override
        def load_instance_state(self, obj, state: State, /, *_) -> None:
            _set_state(obj, state)

    return AutoSavable()


def autosavable(obj_type_or_id: Union[type, str]) -> "mincepy.TypeHelper":
    if isinstance(obj_type_or_id, type):
        obj_type = obj_type_or_id
        obj_path = obj_load.full_name(obj_type)
        # Make sure that it's importable
        assert obj_load.load_obj(obj_path) is obj_type
    elif isinstance(obj_type_or_id, str) and obj_type_or_id.startswith(TYPE_ID_PREFIX):
        obj_path = obj_type_or_id[len(TYPE_ID_PREFIX) + 1 :]
        obj_type = obj_load.load_obj(obj_path)
    else:
        raise TypeError(f"Unknown object type or id: {obj_type_or_id}")

    return _create_helper(obj_type, obj_path)


def _get_state(obj) -> State:
    """
    Get the writable attributes of an object.

    This will try to use vars() but this fails for object with __slots__ in which case it will fall
    back to that
    """
    try:
        obj.__getstate__()
    except AttributeError:
        pass

    try:
        return obj.__dict__
    except AttributeError:
        if "__weakref__" not in obj.__slots__:
            raise ValueError(
                f"Object `{obj}` is not compatible with the historian because it uses __slots__ "
                f"but does not have __weakref__.  Add it to make it compatible."
            ) from None
        return {name: getattr(obj, name) for name in obj.__slots__ if name not in ["__weakref__"]}


def _set_state(obj, state):
    try:
        obj.__setstate__(state)
    except AttributeError:
        pass

    if not isinstance(state, dict):
        raise ValueError(
            f"State must be dict or the object must support __setstate__, got: {type(obj).__name__}"
        )
    try:
        obj.__dict__.update(state)
    except AttributeError:
        # Could be a __slots__ object
        for name, value in state.items():
            setattr(obj, name, value)
