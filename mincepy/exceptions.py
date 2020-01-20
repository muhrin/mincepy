__all__ = ('NotFound', 'ModificationError', 'ObjectDeleted')


class NotFound(Exception):
    """Raised when something can not be found in the history"""


class ModificationError(Exception):
    """Raised when a modification of the history encountered a problem"""


class ObjectDeleted(Exception):
    """Raise when the user tries to interact with a deleted object"""
