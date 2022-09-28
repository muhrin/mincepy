# -*- coding: utf-8 -*-
__all__ = (
    "NotFound",
    "ModificationError",
    "ObjectDeleted",
    "DuplicateKeyError",
    "MigrationError",
    "VersionError",
    "IntegrityError",
    "ReferenceError",
    "ConnectionError",
    "MergeError",
)


class ConnectionError(Exception):  # pylint: disable=redefined-builtin
    """Raise when there is an error in connecting to the backend"""


class NotFound(Exception):
    """Raised when something can not be found in the history"""


class ModificationError(Exception):
    """Raised when a modification of the history encountered a problem"""


class ObjectDeleted(NotFound):
    """Raise when the user tries to interact with a deleted object"""


class DuplicateKeyError(ModificationError):
    """Indicates that a uniqueness constraint was violated"""


class MigrationError(Exception):
    """Indicates that an error occurred during migration"""


class VersionError(Exception):
    """Indicates a version mismatch between the code and the database"""


class IntegrityError(Exception):
    """Indicates an error that occurred because of an operation that would conflict with a database
    constraint"""


class MergeError(Exception):
    """Indicates that an error occurred when trying to merge"""


class ReferenceError(IntegrityError):  # pylint: disable=redefined-builtin
    """Raised when there is an operation that causes a problem with references for example if
    you try to delete an object that is referenced by another this exception will be raised.  The
    objects ids being referenced will be found in .references.
    """

    def __init__(self, msg, references: set):
        super().__init__(msg)
        self.references = references


class UnorderedError(Exception):
    """Raised when an operation is attempted that assumed an underlying ordering but the data is
    unordered"""


class NotOneError(Exception):
    """Raised when a singlar result is expected but there are in fact more"""
