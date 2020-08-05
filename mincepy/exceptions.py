__all__ = ('NotFound', 'ModificationError', 'ObjectDeleted', 'DuplicateKeyError', 'MigrationError',
           'VersionError', 'IntegrityError', 'ReferenceError', 'ConnectionError')


class ConnectionError(Exception):  # pylint: disable=redefined-builtin
    """Raise when there is an error in connecting to the backend"""


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


class VersionError(Exception):
    """Indicates a version mismatch between the code and the database"""


class IntegrityError(Exception):
    """Indicates an error that occurred because of an operation that would conflict with a database
    constraint"""


class ReferenceError(IntegrityError):  # pylint: disable=redefined-builtin

    def __init__(self, msg, references: set):
        super().__init__(msg)
        self.references = references
