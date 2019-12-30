import typing

import bson

from .archive import BaseArchive, DataRecord, TypeCodec

__all__ = ('MongoArchive', 'MongoTypeCodec')


class MongoArchive(BaseArchive):
    DATA_COLLECTION = 'data'

    def __init__(self, db, codecs=tuple()):
        super(MongoArchive, self).__init__(codecs)
        self._data_collection = db[self.DATA_COLLECTION]

    def create_archive_id(self):
        return bson.ObjectId()

    def save(self, record: DataRecord):
        entry = {
            '_id': record.obj_id,
            'type_id': record.type_id,
            'ancestor_id': record.ancestor_id,
            'encoded_value': record.encoded_value,
            'hash': record.obj_hash
        }
        self._data_collection.insert_one(entry)

    def save_many(self, records: typing.List[DataRecord]):
        for record in records:
            self.save(record)

    def load(self, archive_id) -> DataRecord:
        entry = self._data_collection.find_one(filter=archive_id)
        return DataRecord(entry['_id'], entry['type_id'], entry['ancestor_id'], entry['encoded_value'], entry['hash'])


class MongoTypeCodec(TypeCodec):
    def enc(self, value, lookup):
        if type(value) in (type(None), bool, int, float, str, bytes):
            return value
        if isinstance(value, list):
            return [self.enc(entry, lookup) for entry in value]
        if isinstance(value, dict):
            return {key: self.enc(val, lookup) for key, val in value.items()}

        return lookup.ref(value)

    def dec(self, encoded, lookup):
        if isinstance(encoded, bson.ObjectId):
            return lookup.deref(encoded)
        if isinstance(encoded, dict):
            return {key: self.dec(val, lookup) for key, val in encoded.items()}
        if isinstance(encoded, list):
            return [self.dec(entry, lookup) for entry in encoded]

        return encoded
