from abc import ABCMeta, abstractmethod
import collections
from typing import BinaryIO, Optional
import uuid

from . import base_savable
from . import refs

__all__ = 'List', 'LiveList', 'Str', 'Dict', 'LiveDict', 'BaseFile'


class _UserType(base_savable.BaseSavableObject):
    """Mixin for helping user types to be compatible with the historian.
    These typically have a .data member that stores the actual data (list, dict, str, etc)"""
    ATTRS = ('data',)
    data = None  # placeholder


class ObjProxy(_UserType):
    """A simple proxy for any object/data type which can also be a primitive"""
    TYPE_ID = uuid.UUID('d43c2db5-1e8c-428f-988f-8b198accde47')

    def __init__(self, data=None, historian=None):
        super(ObjProxy, self).__init__(historian)
        self.data = data


class List(collections.UserList, _UserType):
    TYPE_ID = uuid.UUID('2b033f70-168f-4412-99ea-d1f131e3a25a')

    def __init__(self, initlist=None, historian=None):
        collections.UserList.__init__(self, initlist)
        _UserType.__init__(self, historian)


class Dict(collections.UserDict, _UserType):
    TYPE_ID = uuid.UUID('a7584078-95b6-4e00-bb8a-b077852ca510')

    def __init__(self, *args, historian=None, **kwarg):
        collections.UserDict.__init__(self, *args, **kwarg)
        _UserType.__init__(self, historian)


class Str(collections.UserString, _UserType):
    TYPE_ID = uuid.UUID('350f3634-4a6f-4d35-b229-71238ce9727d')

    def __init__(self, seq, historian=None):
        collections.UserString.__init__(self, seq)
        _UserType.__init__(self, historian)


class RefList(collections.abc.MutableSequence, _UserType):
    """A list that stores all entries as references in the database"""
    TYPE_ID = uuid.UUID('091efff5-136d-4ac2-bd59-28f50f151263')

    def __init__(self, init_list=None, historian=None):
        super().__init__(historian)
        self.data = []
        if init_list is not None:
            self.data = [refs.ObjRef(item) for item in init_list]

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return repr(self.data)

    def __getitem__(self, item):
        return self.data[item]()

    def __setitem__(self, key, value):
        self.data[key] = refs.ObjRef(value)

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __len__(self):
        return self.data.__len__()

    def insert(self, index, value):
        self.data.insert(index, refs.ObjRef(value))


class LiveList(RefList):
    """A list that is always in sync with the database"""

    TYPE_ID = uuid.UUID('c83e6206-cd29-4fda-bf76-11fce1681cd9')

    def __init__(self, init_list=None, historian=None):
        init_list = init_list or []
        self._ref_list = RefList([ObjProxy(value, historian) for value in init_list])
        super().__init__(historian)

    def __getitem__(self, item):
        self.sync()
        proxy = super(LiveList, self).__getitem__(item)  # type: ObjProxy
        proxy.sync()
        return proxy.data

    def __setitem__(self, key, value):
        self.sync()
        proxy = super(LiveList, self).__getitem__(key)  # type: ObjProxy
        proxy.data = value
        proxy.save()

    def __delitem__(self, key):
        self.sync()
        proxy = super(LiveList, self).__getitem__(key)  # type: ObjProxy
        super(LiveList, self).__delitem__(key)
        self.save()
        self._historian.delete(proxy)

    def __len__(self):
        self.sync()
        return super(LiveList, self).__len__()

    def insert(self, index, value):
        proxy = ObjProxy(value, self._historian)
        self.sync()
        super(LiveList, self).insert(index, proxy)
        self.save()


class RefDict(collections.MutableMapping, _UserType):
    """A dictionary that stores all values as references in the database"""
    TYPE_ID = uuid.UUID('c95f4c4e-766b-4dda-a43c-5fca4fd7bdd0')

    def __init__(self, *args, **kwargs):
        super().__init__(kwargs.pop('historian', None))
        initial = dict(*args, **kwargs)
        if initial:
            self.data = {key: refs.ObjRef(value) for key, value in initial.items()}
        else:
            self.data = {}

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return repr(self.data)

    def __getitem__(self, item):
        return self.data[item]()

    def __setitem__(self, key, value):
        self.data[key] = refs.ObjRef(value)

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return self.data.__len__()


class LiveDict(RefDict):
    TYPE_ID = uuid.UUID('740cc832-721c-4f85-9628-706257eb55b9')

    def __init__(self, *args, **kwargs):
        historian = kwargs.pop('historian', None)
        initial = dict(*args, **kwargs)
        if initial:
            initial = {key: ObjProxy(value) for key, value in initial.items()}
        super().__init__(initial, historian=historian)

    def __getitem__(self, item):
        self.sync()
        proxy = super().__getitem__(item)  # type: ObjProxy
        proxy.sync()
        return proxy.data

    def __setitem__(self, key, value):
        self.sync()
        if key in self:
            proxy = super().__getitem__(key)  # type: ObjProxy
            proxy.data = value
            proxy.save()
        else:
            proxy = ObjProxy(value)
            super().__setitem__(key, proxy)
            self.save()
            proxy.save()

    def __delitem__(self, key):
        self.sync()
        proxy = super(LiveDict, self).__getitem__(key)  # type: ObjProxy
        super(LiveDict, self).__delitem__(key)
        self.save()
        self._historian.delete(proxy)

    def __iter__(self):
        self.sync()
        return super(LiveDict, self).__iter__()

    def __len__(self):
        self.sync()
        return super(LiveDict, self).__len__()


class BaseFile(base_savable.BaseSavableObject, metaclass=ABCMeta):
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


HISTORIAN_TYPES = Str, List, Dict, RefList, RefDict, LiveDict, LiveList, ObjProxy
