__all__ = 'NotFound', 'ModificationError', 'ObjectDeleted', 'DuplicateKeyError', 'MigrationError'


class NotFound(Exception):
    """Raised when something can not be found in the history"""


class ModificationError(Exception):
    """Raised when a modification of the history encountered a problem"""


class ObjectDeleted(NotFound):
    """Raise when the user tries to interact with a deleted object"""


class DuplicateKeyError(Exception):
    """Indicates that a uniqueness constraint was violated"""


class MigrationError(Exception):
    """Indicates that an error occurred during migration"""
