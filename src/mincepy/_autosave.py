from typing import TYPE_CHECKING, Union

from pytray import obj_load
from typing_extensions import override

from . import helpers

if TYPE_CHECKING:
    import mincepy

TYPE_ID_PREFIX = "autosave"


def _create_helper(obj_type: type, obj_path: str) -> "mincepy.TypeHelper":
    """Create a type helper that uses the object path as the type id"""

    class AutoSavable(
        helpers.TypeHelper, obj_type=obj_type, type_id=f"{TYPE_ID_PREFIX}:{obj_path}"
    ):
        @override
        def save_instance_state(self, obj, saver: "mincepy.Saver", /) -> dict:
            saved_state = _get_state(obj)
            # Check if a superclass wants to save the state
            super_state = {}
            for super_type in obj_type.mro()[1:]:
                try:
                    helper: "mincepy.TypeHelper" = (
                        saver.historian.type_registry.get_helper_from_obj_type(super_type)
                    )
                    super_state = helper.save_instance_state(obj, saver)
                    break
                except ValueError:
                    pass
            saved_state.update(super_state)
            return saved_state

        @override
        def load_instance_state(self, obj, state: dict, loader: "mincepy.Loader", /) -> None:
            _set_state(obj, state)
            # Now see if there is a superclass that wants to load the instance state
            for super_type in obj_type.mro()[1:]:
                try:
                    helper: "mincepy.TypeHelper" = (
                        loader.historian.type_registry.get_helper_from_obj_type(super_type)
                    )
                    helper.load_instance_state(obj, state, loader)
                    break
                except ValueError:
                    pass

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


def _get_state(obj: object) -> dict:
    """
    Get the writable attributes of an object.

    This will try to use vars() but this fails for object with __slots__ in which case it will fall
    back to that
    """
    state = {}
    if hasattr(obj, "__slots__"):
        if not hasattr(obj, "__dict__") and "__weakref__" not in obj.__slots__:
            raise ValueError(
                f"Object `{obj}` is not compatible with the historian because it uses __slots__ "
                f"but does not have __weakref__.  Add it to make it compatible."
            )
        state.update(
            {name: getattr(obj, name) for name in obj.__slots__ if name not in ["__weakref__"]}
        )
    if hasattr(obj, "__dict__"):
        state.update(obj.__dict__)

    return state


def _set_state(obj: object, state: dict):
    for name, value in state.items():
        setattr(obj, name, value)
