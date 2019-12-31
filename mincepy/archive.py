from abc import ABCMeta, abstractmethod
from collections import namedtuple
import typing

from .depositor import Referencer

__all__ = ('Archive', 'DataRecord', 'TypeCodec')

DataRecord = namedtuple('DataRecord', ('obj_id', 'type_id', 'ancestor_id', 'encoded_value', 'obj_hash'))


class TypeCodec(metaclass=ABCMeta):
    """Defines how to encode and decode an object"""
    TYPE = None
    TYPE_ID = None

    def __init__(self):
        assert self.TYPE is not None, "Must set TYPE that this codec corresponds to"
        assert self.TYPE_ID is not None, "Must set the TYPE_ID for this codec"

    @abstractmethod
    def encode(self, value, lookup: Referencer):
        pass

    @abstractmethod
    def decode(self, encoded_value, obj, lookup: Referencer):
        pass


class Archive(metaclass=ABCMeta):
    @abstractmethod
    def create_archive_id(self):
        """Create a new archive id"""

    @abstractmethod
    def save(self, record: DataRecord):
        """Save a data record to the archive"""

    @abstractmethod
    def save_many(self, records: typing.Sequence[DataRecord]):
        """Save many data records to the archive"""

    @abstractmethod
    def load(self, archive_id) -> DataRecord:
        """Load a data record from its archive id"""

    @abstractmethod
    def get_codec_from_id(self, type_id):
        """Get the codec for the given type id"""

    @abstractmethod
    def get_codec_from_type(self, obj_type):
        """Get the codec for the given type"""


class BaseArchive(Archive):
    def __init__(self, codecs: typing.Sequence[TypeCodec] = tuple()):
        self._codec_type_map = {}
        self._codec_typeid_map = {}
        for codec in codecs:
            self.add_codec(codec)

    def save_many(self, records: typing.Sequence[DataRecord]):
        """
        This will save records one by one but subclass may want to override this behaviour if
        they can save multiple records at once.
        """
        for record in records:
            self.save(record)

    def get_codec_from_id(self, type_id):
        try:
            return self._codec_typeid_map[type_id]
        except KeyError:
            raise ValueError("No codec for type id '{}'".format(type_id))

    def get_codec_from_type(self, obj_type):
        try:
            return self._codec_type_map[obj_type]
        except KeyError:
            raise TypeError("No codec for type '{}'".format(obj_type))

    def add_codec(self, codec: TypeCodec):
        self._codec_type_map[codec.TYPE] = codec
        self._codec_typeid_map[codec.TYPE_ID] = codec


class Archivist:
    """The archivists uses an archive and a referencer to save and load objects"""

    class RecordBuilder:
        """
        Build a record in two steps:
        1. Create the archive ID which be used to refer to it
        2. Encode the value of the object and create the record
        """

        def __init__(self, archivist, obj, obj_hash, ancestor_id=None):
            self._archivist = archivist
            self._archive_id = archivist.archive.create_archive_id()
            self._obj = obj
            self._obj_hash = obj_hash
            self._ancestor_id = ancestor_id
            self._codec = archivist.archive.get_codec_from_type(type(obj))

        @property
        def archive_id(self):
            return self._archive_id

        def build(self) -> DataRecord:
            encoded_value = self._codec.encode(self._obj, self._archivist.referencer)
            return DataRecord(
                self.archive_id,
                self._codec.TYPE_ID,
                self._ancestor_id,
                encoded_value,
                self._obj_hash
            )

    class ObjectLoader:
        """
        Load an object in two steps:
        1. Create a blank object in memory that can be used to make references
        2. Load the state into that object
        """

        def __init__(self, archivist, record):
            self._archivist = archivist
            self._record = record
            self._codec = archivist.archive.get_codec_from_id(record.type_id)
            self._obj = self._codec.TYPE.__new__(self._codec.TYPE)

        @property
        def obj(self):
            return self._obj

        def load(self):
            self._codec.decode(self._record.encoded_value, self._obj, self._archivist.referencer)
            return self.obj

    def __init__(self, archive: Archive, referencer: Referencer):
        self._archive = archive
        self._referencer = referencer

    @property
    def archive(self):
        return self._archive

    @property
    def referencer(self):
        return self._referencer

    def record_builder(self, obj, obj_hash, ancestor_id=None):
        return self.RecordBuilder(self, obj, obj_hash, ancestor_id)

    def object_loader(self, record: DataRecord):
        return self.ObjectLoader(self, record)
