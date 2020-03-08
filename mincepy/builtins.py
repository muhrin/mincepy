"""Module for all the built in container and other types"""

from abc import ABCMeta, abstractmethod
import collections
from contextlib import contextmanager
from pathlib import Path
import shutil
from typing import BinaryIO, Optional
import uuid

from . import base_savable
from . import refs
from .utils import sync

__all__ = ('List', 'LiveList', 'LiveRefList', 'RefList', 'Str', 'Dict', 'RefDict', 'LiveDict',
           'LiveRefDict', 'BaseFile', 'File')


class _UserType(base_savable.BaseSavableObject, metaclass=ABCMeta):
    """Mixin for helping user types to be compatible with the historian.
    These typically have a .data member that stores the actual data (list, dict, str, etc)"""
    # pylint: disable=too-few-public-methods
    ATTRS = ('data',)
    data = None  # placeholder


class ObjProxy(_UserType):
    """A simple proxy for any object/data type which can also be a primitive"""
    TYPE_ID = uuid.UUID('d43c2db5-1e8c-428f-988f-8b198accde47')

    def __init__(self, data=None, historian=None):
        super(ObjProxy, self).__init__(historian)
        self.data = data

    def __call__(self):
        return self.data

    def assign(self, value):
        self.data = value


class Str(collections.UserString, _UserType):
    TYPE_ID = uuid.UUID('350f3634-4a6f-4d35-b229-71238ce9727d')

    def __init__(self, seq, historian=None):
        collections.UserString.__init__(self, seq)
        _UserType.__init__(self, historian)


class Reffer:
    """Mixin for types that want to be able to create references non-primitive types"""

    # pylint: disable=no-member, too-few-public-methods

    def _ref(self, obj):
        """Create a reference for a non-primitive, otherwise use the value"""
        if self._historian.is_primitive(obj):
            return obj

        return self._create_ref(obj)

    def _deref(self, obj):
        """Dereference a non-primitive, otherwise return the value"""
        if self._historian.is_primitive(obj):
            return obj

        return obj()

    def _create_ref(self, obj):
        """Create a reference for a given object"""
        return refs.ObjRef(obj, self._historian)


# region lists


class List(collections.UserList, _UserType):
    TYPE_ID = uuid.UUID('2b033f70-168f-4412-99ea-d1f131e3a25a')

    def __init__(self, initlist=None, historian=None):
        collections.UserList.__init__(self, initlist)
        _UserType.__init__(self, historian)


class RefList(collections.abc.MutableSequence, Reffer, _UserType):
    """A list that stores all entries as references in the database except primitives"""
    TYPE_ID = uuid.UUID('091efff5-136d-4ac2-bd59-28f50f151263')
    CONTAINER = list

    def __init__(self, init_list=None, historian=None):
        super().__init__(historian)
        self.data = []
        if init_list is not None:
            self.data = self.CONTAINER(self._ref(item) for item in init_list)

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return repr(self.data)

    def __getitem__(self, item):
        return self._deref(self.data[item])

    def __setitem__(self, key, value):
        self.data[key] = self._ref(value)

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __len__(self):
        return self.data.__len__()

    def insert(self, index, value):
        self.data.insert(index, self._ref(value))


class LiveList(collections.abc.MutableSequence, _UserType):
    """A list that is always in sync with the database"""
    TYPE_ID = uuid.UUID('c83e6206-cd29-4fda-bf76-11fce1681cd9')

    def __init__(self, init_list=None, historian=None):
        super(LiveList, self).__init__(historian)
        init_list = init_list or []
        self.data = RefList(init_list)

    @sync()
    def __getitem__(self, item):
        proxy = self.data[item]  # type: ObjProxy
        proxy.sync()
        return proxy()

    @sync(save=True)
    def __setitem__(self, key, value):
        proxy = self.data[key]  # type: ObjProxy
        proxy.assign(value)

    @sync(save=True)
    def __delitem__(self, key):
        proxy = self.data[key]  # type: ObjProxy
        del self.data[key]
        self._historian.delete(proxy)

    @sync()
    def __len__(self):
        return len(self.data)

    @sync(save=True)
    def insert(self, index, value):
        proxy = self._create_proxy(value)
        self.data.insert(index, proxy)

    @sync(save=True)
    def append(self, value):
        proxy = self._create_proxy(value)
        self.data.append(proxy)

    def _create_proxy(self, obj):
        return ObjProxy(obj, self._historian)


class LiveRefList(RefList):
    """A live list that uses references to store objects"""
    TYPE_ID = uuid.UUID('98454806-c587-4fcc-a514-65fdefb0180d')
    CONTAINER = LiveList


# endregion

# region Dicts


class Dict(collections.UserDict, _UserType):
    TYPE_ID = uuid.UUID('a7584078-95b6-4e00-bb8a-b077852ca510')

    def __init__(self, *args, historian=None, **kwarg):
        collections.UserDict.__init__(self, *args, **kwarg)
        _UserType.__init__(self, historian)


class RefDict(collections.MutableMapping, Reffer, _UserType):
    """A dictionary that stores all values as references in the database."""
    TYPE_ID = uuid.UUID('c95f4c4e-766b-4dda-a43c-5fca4fd7bdd0')
    CONTAINER = dict

    def __init__(self, *args, **kwargs):
        super().__init__(kwargs.pop('historian', None))
        initial = dict(*args, **kwargs)
        if initial:
            self.data = self.CONTAINER({key: self._ref(value) for key, value in initial.items()})
        else:
            self.data = self.CONTAINER()

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return repr(self.data)

    def __getitem__(self, item):
        return self._deref(self.data[item])

    def __setitem__(self, key, value):
        self.data[key] = self._ref(value)

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return self.data.__len__()


class LiveDict(collections.MutableMapping, _UserType):
    TYPE_ID = uuid.UUID('740cc832-721c-4f85-9628-706257eb55b9')

    def __init__(self, *args, **kwargs):
        historian = kwargs.pop('historian', None)
        super().__init__(historian=historian)

        initial = dict(*args, **kwargs)
        self.data = RefDict({key: self._create_proxy(value) for key, value in initial.items()})

    @sync()
    def __getitem__(self, item):
        proxy = self.data[item]  # type: ObjProxy
        proxy.sync()
        return proxy()

    @sync()
    def __setitem__(self, key, value):
        if key in self.data:
            # Can simply update the proxy
            proxy = self.data[key]  # type: ObjProxy
            proxy.assign(value)
            if proxy.is_saved():
                proxy.save()
        else:
            proxy = self._create_proxy(value)
            self.data[key] = proxy
            if self.is_saved():
                self.save()  # This will cause the proxy to be saved as well

    @sync(save=True)
    def __delitem__(self, key):
        proxy = self.data[key]
        del self.data[key]
        self._historian.delete(proxy)

    @sync()
    def __iter__(self):
        return self.data.__iter__()

    @sync()
    def __len__(self):
        return len(self.data)

    def _create_proxy(self, value):
        return ObjProxy(value, self._historian)


class LiveRefDict(RefDict):
    """A live dictionary that uses references to refer to contained objects"""
    # pylint: disable=too-few-public-methods
    TYPE_ID = uuid.UUID('16e7e814-8268-46e0-8d8e-6f34132366b9')
    CONTAINER = LiveDict


# endregion


class File(base_savable.BaseSavableObject, metaclass=ABCMeta):
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

    def from_disk(self, path):
        """Copy the contents of a disk file to this file"""
        with open(str(path), 'r', encoding=self.encoding) as disk_file:
            with self.open('w') as this:
                shutil.copyfileobj(disk_file, this)

    def to_disk(self, folder: [str, Path]):
        """Copy the contents of this file to a file on disk in the given folder"""
        file_path = Path(str(folder)) / self.filename
        with open(str(file_path), 'w', encoding=self._encoding) as disk_file:
            with self.open('r') as this:
                shutil.copyfileobj(this, disk_file)

    def write_text(self, text: str, encoding=None):
        encoding = encoding or self._encoding
        with self.open('w', encoding=encoding) as fileobj:
            fileobj.write(text)

    def read_text(self, encoding=None) -> str:
        """Read the contents of the file as text.
        This function is named as to mirror pathlib.Path"""
        encoding = encoding or self._encoding
        with self.open('r', encoding=encoding) as fileobj:
            return fileobj.read()

    def __str__(self):
        contents = [str(self._filename)]
        if self._encoding is not None:
            contents.append("({})".format(self._encoding))
        return " ".join(contents)

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
        """Hash the contents of the file"""
        try:
            with self.open('rb') as opened:
                while True:
                    line = opened.read(self.READ_SIZE)
                    if line == b'':
                        return
                    yield line
        except FileNotFoundError:
            yield from hasher.yield_hashables(None)


BaseFile = File

HISTORIAN_TYPES = (Str, List, RefList, LiveList, LiveRefList, Dict, RefDict, LiveDict, LiveRefDict,
                   ObjProxy)
