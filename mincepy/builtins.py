from abc import ABCMeta, abstractmethod
import collections
from typing import BinaryIO, Optional
import uuid

from . import depositors
from . import types

__all__ = 'List', 'Str', 'Dict', 'BaseFile'


class _UserType(types.Archivable):
    """Mixin for helping user types to be compatible with the historian.
    These typically have a .data member that stores the actual data (list, dict, str, etc)"""
    ATTRS = ('data',)
    data = None  # placeholder


class List(collections.UserList, _UserType):
    TYPE_ID = uuid.UUID('2b033f70-168f-4412-99ea-d1f131e3a25a')

    def save_instance_state(self, _depositor: depositors.Depositor):
        return self.data

    def load_instance_state(self, state, _depositor: depositors.Depositor):
        self.__init__(state)


class Dict(collections.UserDict, _UserType):
    TYPE_ID = uuid.UUID('a7584078-95b6-4e00-bb8a-b077852ca510')

    def save_instance_state(self, _depositor: depositors.Depositor):
        return self.data

    def load_instance_state(self, state, _depositor: depositors.Depositor):
        self.__init__(state)


class Str(collections.UserString, _UserType):
    TYPE_ID = uuid.UUID('350f3634-4a6f-4d35-b229-71238ce9727d')

    def save_instance_state(self, _depositor: depositors.Depositor):
        return self.data

    def load_instance_state(self, state, _depositor: depositors.Depositor):
        self.data = state


class BaseFile(types.Archivable, metaclass=ABCMeta):
    ATTRS = ('_filename', '_encoding')
    READ_SIZE = 256  # The number of bytes to read at a time

    def __init__(self, filename: str = None, encoding=None):
        super().__init__()
        self._filename = filename
        self._encoding = encoding

    @property
    def filename(self) -> Optional[str]:
        return self._filename

    @property
    def encoding(self) -> Optional[str]:
        return self._encoding

    @abstractmethod
    def open(self, mode='r', **kwargs) -> BinaryIO:
        """Open returning a file like object that supports close() and read()"""

    def __eq__(self, other) -> bool:
        """Compare the contents of two files

        If both files do not exist they are considered equal.
        """
        if not isinstance(other, BaseFile) or self.filename != other.filename:
            return False

        try:
            with self.open() as my_file:
                try:
                    with other.open() as other_file:
                        while True:
                            my_line = my_file.readline(self.READ_SIZE)
                            other_line = other_file.readline(self.READ_SIZE)
                            if my_line != other_line:
                                return False
                            if my_line == '' and other_line == '':
                                return True
                except FileNotFoundError:
                    return False
        except FileNotFoundError:
            # Our file doesn't exist, make sure the other doesn't either
            try:
                with other.open():
                    return False
            except FileNotFoundError:
                return True

    def yield_hashables(self, hasher):
        """Has the contents of the file"""
        try:
            with self.open('rb') as opened:
                while True:
                    line = opened.read(self.READ_SIZE)
                    if line == b'':
                        return
                    yield line
        except FileNotFoundError:
            yield from hasher.yield_hashables(None)


HISTORIAN_TYPES = Str, List, Dict