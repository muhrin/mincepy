from typing import Optional

from . import builtins
from . import common_helpers
from . import historian

__all__ = 'get_historian', 'set_historian'

CURRENT_HISTORIAN = None


def get_historian() -> historian.Historian:
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    return CURRENT_HISTORIAN


def set_historian(new_historian: Optional[historian.Historian], register_common_types=True):
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    CURRENT_HISTORIAN = new_historian
    if new_historian is not None and register_common_types:
        new_historian.register_types(common_helpers.HISTORIAN_TYPES)
        new_historian.register_types(builtins.HISTORIAN_TYPES)
