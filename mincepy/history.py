from typing import Optional

from . import historian
from . import inmemory

__all__ = 'create_default_historian', 'get_historian', 'set_historian'

CURRENT_HISTORIAN = None


def create_default_historian() -> historian.Historian:
    return historian.Historian(inmemory.InMemory())


def get_historian() -> historian.Historian:
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    if CURRENT_HISTORIAN is None:
        CURRENT_HISTORIAN = create_default_historian()
    return CURRENT_HISTORIAN


def set_historian(new_historian: Optional[historian.Historian]):
    global CURRENT_HISTORIAN  # pylint: disable=global-statement
    CURRENT_HISTORIAN = new_historian
