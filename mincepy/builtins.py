"""Module for all the built in container and other types"""

from abc import ABCMeta, abstractmethod
import collections
from pathlib import Path
import shutil
from typing import BinaryIO, Optional
import uuid

from . import base_savable
from . import helpers
from . import records
from . import refs
from .utils import sync
from . import types

__all__ = ('List', 'LiveList', 'LiveRefList', 'RefList', 'Str', 'Dict', 'RefDict', 'LiveDict',
           'LiveRefDict', 'BaseFile', 'File')


class _UserType(base_savable.SimpleSavable, metaclass=ABCMeta):
    """Mixin for helping user types to be compatible with the historian.
    These typically have a .data member that stores the actual data (list, dict, str, etc)"""
    ATTRS = ('data',)
    data = None  # placeholder
    # This is an optional type for the data member.  See save_instance_state type for information
    # on when it might be useful
    DATA_TYPE = None  # type: type

    def save_instance_state(self, saver):
        # This is a convenient way of storing primitive data types directly as the state
        # rather than having to be a 'data' member of a dictionary.  This makes it much
        # easier to search these types
        if self.DATA_TYPE is not None and \
                issubclass(self.DATA_TYPE, saver.get_historian().primitives):
            self._on_save(saver)  # Call this here are we aren't going to call up the hierarchy
            return self.data

        return super().save_instance_state(saver)

    def load_instance_state(self, saved_state, loader):
        # See save_instance_state
        if self.DATA_TYPE is not None and \
                issubclass(self.DATA_TYPE, loader.get_historian().primitives):
            self._on_load(loader)  # Call this here are we aren't going to call up the hierarchy
            self.data = saved_state
        else:
            super(_UserType, self).load_instance_state(saved_state, loader)


class ObjProxy(_UserType):
    """A simple proxy for any object/data type which can also be a primitive"""
    TYPE_ID = uuid.UUID('d43c2db5-1e8c-428f-988f-8b198accde47')

    def __init__(self, data=None):
        super(ObjProxy, self).__init__()
        self.data = data

    def __call__(self):
        return self.data

    def assign(self, value):
        self.data = value


class Str(collections.UserString, _UserType):
    TYPE_ID = uuid.UUID('350f3634-4a6f-4d35-b229-71238ce9727d')
    DATA_TYPE = str

    def __init__(self, seq):
        collections.UserString.__init__(self, seq)
        _UserType.__init__(self)


class Reffer:
    """Mixin for types that want to be able to create references non-primitive types"""

    # pylint: disable=too-few-public-methods, no-self-use

    def _ref(self, obj):
        """Create a reference for a non-primitive, otherwise use the value"""
        if types.is_primitive(obj):
            return obj

        return self._create_ref(obj)

    def _deref(self, obj):
        """Dereference a non-primitive, otherwise return the value"""
        if types.is_primitive(obj):
            return obj

        return obj()

    def _create_ref(self, obj):
        """Create a reference for a given object"""
        return refs.ObjRef(obj)


# region lists


class List(collections.UserList, _UserType):
    TYPE_ID = uuid.UUID('2b033f70-168f-4412-99ea-d1f131e3a25a')
    DATA_TYPE = list

    def __init__(self, initlist=None):
        collections.UserList.__init__(self, initlist)
        _UserType.__init__(self)


class RefList(collections.abc.MutableSequence, Reffer, _UserType):
    """A list that stores all entries as references in the database except primitives"""
    TYPE_ID = uuid.UUID('091efff5-136d-4ac2-bd59-28f50f151263')
    DATA_TYPE = list

    def __init__(self, init_list=None):
        super().__init__()
        init_list = init_list or []
        self.data = self.DATA_TYPE(self._ref(item) for item in init_list)

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

    def __init__(self, init_list=None):
        super(LiveList, self).__init__()
        init_list = init_list or []
        self.data = RefList(self._create_proxy(item) for item in init_list)

    @sync()
    def __getitem__(self, item):
        proxy = self.data[item]  # type: ObjProxy
        if self.is_saved():
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
        if self._historian is not None:
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

    def _create_proxy(self, obj):  # pylint: disable=no-self-use
        return ObjProxy(obj)


class LiveRefList(Reffer, LiveList):
    """A live list that uses references to store objects"""
    TYPE_ID = uuid.UUID('98454806-c587-4fcc-a514-65fdefb0180d')

    @sync()
    def __getitem__(self, item):
        proxy = self.data[item]  # type: ObjProxy
        if self.is_saved():
            proxy.sync()
        ref = proxy()
        return self._deref(ref)

    @sync(save=True)
    def __setitem__(self, key, value):
        proxy = self.data[key]  # type: ObjProxy
        proxy.assign(self._ref(value))

    def _create_proxy(self, obj):
        return ObjProxy(self._ref(obj))


# endregion

# region Dicts


class Dict(collections.UserDict, _UserType):
    TYPE_ID = uuid.UUID('a7584078-95b6-4e00-bb8a-b077852ca510')
    DATA_TYPE = dict

    def __init__(self, *args, **kwarg):
        collections.UserDict.__init__(self, *args, **kwarg)
        _UserType.__init__(self)


class RefDict(collections.MutableMapping, Reffer, _UserType):
    """A dictionary that stores all values as references in the database."""
    TYPE_ID = uuid.UUID('c95f4c4e-766b-4dda-a43c-5fca4fd7bdd0')
    DATA_TYPE = dict

    def __init__(self, *args, **kwargs):
        super().__init__()
        initial = dict(*args, **kwargs)
        self.data = self.DATA_TYPE({key: self._ref(value) for key, value in initial.items()})

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
    DATA_TYPE = RefDict

    def __init__(self, *args, **kwargs):
        super().__init__()
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

    def _create_proxy(self, value):  # pylint: disable=no-self-use
        return ObjProxy(value)


class LiveRefDict(Reffer, LiveDict):
    """A live dictionary that uses references to refer to contained objects"""
    TYPE_ID = uuid.UUID('16e7e814-8268-46e0-8d8e-6f34132366b9')

    def __init__(self, *args, **kwargs):
        super().__init__()

        initial = dict(*args, **kwargs)
        self.data = RefDict({key: self._create_proxy(value) for key, value in initial.items()})

    @sync()
    def __getitem__(self, item):
        proxy = self.data[item]  # type: ObjProxy
        proxy.sync()
        ref = proxy()
        return self._deref(ref)

    @sync()
    def __setitem__(self, key, value):
        if key in self.data:
            # Can simply update the proxy
            proxy = self.data[key]  # type: ObjProxy
            proxy.assign(self._ref(value))
            if proxy.is_saved():
                proxy.save()
        else:
            proxy = self._create_proxy(value)
            self.data[key] = proxy
            if self.is_saved():
                self.save()  # This will cause the proxy to be saved as well

    def _create_proxy(self, value):
        return ObjProxy(self._ref(value))


# endregion


class File(base_savable.SimpleSavable, metaclass=ABCMeta):
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


class SnapshotIdHelper(helpers.TypeHelper):
    """Add ability to store references"""
    TYPE = records.SnapshotId
    TYPE_ID = uuid.UUID('05fe092b-07b3-4ffc-8cf2-cee27aa37e81')

    def eq(self, one, other):  # pylint: disable=invalid-name, no-self-use
        if not (isinstance(one, records.SnapshotId) and isinstance(other, records.SnapshotId)):
            return False

        return one.obj_id == other.obj_id and one.version == other.version

    def yield_hashables(self, obj, hasher):  # pylint: disable=no-self-use
        yield from hasher.yield_hashables(obj.obj_id)
        yield from hasher.yield_hashables(obj.version)

    def save_instance_state(self, obj, _saver):  # pylint: disable=no-self-use
        return obj.to_dict()

    def load_instance_state(self, obj, saved_state, _loader):  # pylint: disable=no-self-use
        if isinstance(saved_state, list):
            # Legacy version
            obj.__init__(*saved_state)
        else:
            # New version is a dictionary
            obj.__init__(**saved_state)


HISTORIAN_TYPES = (Str, List, RefList, LiveList, LiveRefList, Dict, RefDict, LiveDict, LiveRefDict,
                   ObjProxy)
