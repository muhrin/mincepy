from abc import ABCMeta, abstractmethod
import collections
from typing import BinaryIO, Optional
import uuid

from . import depositors
from . import refs
from . import types

__all__ = 'List', 'Str', 'Dict', 'BaseFile'


class Archivable(types.SavableObject):
    """A helper class that makes a class compatible with the historian by flagging certain
    attributes which will be saved/loaded/hashed and compared in __eq__.  This should be an
    exhaustive list of all the attributes that define this class.  If more complex functionality
    is needed then the standard SavableComparable interface methods should be overwritten."""
    ATTRS = tuple()
    IGNORE_MISSING = True

    def __new__(cls, *_args, **_kwargs):
        new_instance = super(Archivable, cls).__new__(cls)
        attrs = []
        for entry in cls.__mro__:
            try:
                local = getattr(entry, 'ATTRS')
                attrs.extend(local)
            except AttributeError:
                pass
        setattr(new_instance, '__attrs', attrs)
        setattr(new_instance, '__to_deref', [])
        return new_instance

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False

        return all(getattr(self, name) == getattr(other, name) for name in self.__get_attrs())

    def yield_hashables(self, hasher):
        yield from hasher.yield_hashables([getattr(self, name) for name in self.__get_attrs()])

    def save_instance_state(self, _saver):
        return {name: getattr(self, name) for name in self.__get_attrs()}

    def load_instance_state(self, saved_state, _loader):
        for name in self.__get_attrs():
            try:
                setattr(self, name, saved_state[name])
            except KeyError:
                if not self.IGNORE_MISSING:
                    raise

    def __get_attrs(self):
        return getattr(self, '__attrs')


class _UserType(Archivable):
    """Mixin for helping user types to be compatible with the historian.
    These typically have a .data member that stores the actual data (list, dict, str, etc)"""
    ATTRS = ('data',)
    data = None  # placeholder


class List(collections.UserList, _UserType):
    TYPE_ID = uuid.UUID('2b033f70-168f-4412-99ea-d1f131e3a25a')

    def save_instance_state(self, _saver: depositors.Saver):
        return self.data

    def load_instance_state(self, state, _loader: depositors.Loader):
        self.__init__(state)


class Dict(collections.UserDict, _UserType):
    TYPE_ID = uuid.UUID('a7584078-95b6-4e00-bb8a-b077852ca510')

    def save_instance_state(self, _saver: depositors.Saver):
        return self.data

    def load_instance_state(self, state, _loader: depositors.Loader):
        self.__init__(state)


class Str(collections.UserString, _UserType):
    TYPE_ID = uuid.UUID('350f3634-4a6f-4d35-b229-71238ce9727d')

    def save_instance_state(self, _saver: depositors.Saver):
        return self.data

    def load_instance_state(self, state, _loader: depositors.Loader):
        self.data = state


class RefList(collections.abc.MutableSequence, Archivable):
    """A list that stores all entries as references in the database"""
    TYPE_ID = uuid.UUID('091efff5-136d-4ac2-bd59-28f50f151263')
    ATTRS = ('_data',)

    def __init__(self, init_list=None):
        super().__init__()
        self._data = []
        if init_list is not None:
            self._data = [refs.ObjRef(item) for item in init_list]

    def __str__(self):
        return str(self._data)

    def __getitem__(self, item):
        return self._data[item]()

    def __setitem__(self, key, value):
        self._data[key] = refs.ObjRef(value)

    def __delitem__(self, key):
        self._data.__delitem__(key)

    def __len__(self):
        return self._data.__len__()

    def insert(self, index, value):
        self._data.insert(index, refs.ObjRef(value))


class RefDict(collections.MutableMapping, Archivable):
    """A dictionary that stores all values as references in the database"""
    TYPE_ID = uuid.UUID('c95f4c4e-766b-4dda-a43c-5fca4fd7bdd0')
    ATTRS = ('_data',)

    def __init__(self, *args, **kwargs):
        super().__init__()
        initial = dict(*args, **kwargs)
        if initial:
            self._data = {key: refs.ObjRef(value) for key, value in initial.items()}
        else:
            self._data = {}

    def __str__(self):
        return str(self._data)

    def __getitem__(self, item):
        return self._data[item]()

    def __setitem__(self, key, value):
        self._data[key] = refs.ObjRef(value)

    def __delitem__(self, key):
        self._data.__delitem__(key)

    def __iter__(self):
        return self._data.__iter__()

    def __len__(self):
        return self._data.__len__()


class BaseFile(Archivable, metaclass=ABCMeta):
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


HISTORIAN_TYPES = Str, List, Dict, RefList, RefDict
