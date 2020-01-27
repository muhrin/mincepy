from typing import Optional

from . import historian

__all__ = 'get_historian', 'set_historian'

CURRENT_HISTORIAN = None


def get_historian() -> historian.Historian:
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    return CURRENT_HISTORIAN


def set_historian(new_historian: Optional[historian.Historian]):
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    CURRENT_HISTORIAN = new_historian
